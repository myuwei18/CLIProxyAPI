package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store owns the plugin SQLite database.
type Store struct {
	path string
	db   *sql.DB
	mu   sync.Mutex
}

// Open opens or creates the FreeModel plugin database.
func Open(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = filepath.Join("plugins", "cpa-freemodel-data", "freemodel.db")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	s := &Store{path: path, db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Path returns the database path.
func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS fm_account (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	email           TEXT NOT NULL UNIQUE,
	user_id         INTEGER DEFAULT 0,
	cookie          TEXT NOT NULL,
	model_api_key   TEXT DEFAULT '',
	proxy           TEXT DEFAULT '',
	created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fm_quota_snapshot (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	account_id      INTEGER NOT NULL REFERENCES fm_account(id),
	email           TEXT NOT NULL,
	plan_id         TEXT DEFAULT '',
	plan_status     TEXT DEFAULT '',
	credit_cents    INTEGER DEFAULT 0,
	topup_cents     INTEGER DEFAULT 0,
	referral_credits REAL DEFAULT 0,
	referral_used    REAL DEFAULT 0,
	current_period_end TEXT DEFAULT '',
	cancel_at_period_end INTEGER DEFAULT 0,
	window_5h_used  INTEGER DEFAULT 0,
	window_5h_limit INTEGER DEFAULT 0,
	window_5h_resets_at INTEGER DEFAULT 0,
	window_week_used  INTEGER DEFAULT 0,
	window_week_limit INTEGER DEFAULT 0,
	window_week_resets_at INTEGER DEFAULT 0,
	total_requests  INTEGER DEFAULT 0,
	total_tokens    INTEGER DEFAULT 0,
	fetched_at      DATETIME NOT NULL,
	created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_snapshot_account ON fm_quota_snapshot(account_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshot_fetched ON fm_quota_snapshot(fetched_at DESC);
`)
	if err != nil {
		return fmt.Errorf("migrate freemodel tables: %w", err)
	}
	if err := createOpsTables(ctx, s.db); err != nil {
		return err
	}

	migrations := []string{
		"ALTER TABLE fm_account ADD COLUMN user_id INTEGER DEFAULT 0",
		"ALTER TABLE fm_quota_snapshot ADD COLUMN current_period_end TEXT DEFAULT ''",
		"ALTER TABLE fm_quota_snapshot ADD COLUMN cancel_at_period_end INTEGER DEFAULT 0",
		"ALTER TABLE fm_quota_snapshot ADD COLUMN referral_used REAL DEFAULT 0",
		"ALTER TABLE fm_quota_snapshot ADD COLUMN topup_cents INTEGER DEFAULT 0",
		"ALTER TABLE fm_account ADD COLUMN model_api_key TEXT DEFAULT ''",
	}
	for _, migration := range migrations {
		_, _ = s.db.ExecContext(ctx, migration)
	}
	return nil
}

// SaveAccount inserts or updates an account.
func (s *Store) SaveAccount(ctx context.Context, input AccountInput) (*Account, error) {
	email := strings.TrimSpace(input.Email)
	if email == "" {
		return nil, errors.New("email is required")
	}
	if strings.TrimSpace(input.Cookie) == "" && strings.TrimSpace(input.ModelAPIKey) == "" {
		return nil, errors.New("cookie or api_key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO fm_account (email, cookie, model_api_key, proxy, updated_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(email) DO UPDATE SET
			cookie = CASE WHEN excluded.cookie != '' THEN excluded.cookie ELSE fm_account.cookie END,
			model_api_key = CASE WHEN excluded.model_api_key != '' THEN excluded.model_api_key ELSE fm_account.model_api_key END,
			proxy = excluded.proxy,
			updated_at = CURRENT_TIMESTAMP
	`, email, strings.TrimSpace(input.Cookie), strings.TrimSpace(input.ModelAPIKey), strings.TrimSpace(input.Proxy))
	if err != nil {
		return nil, fmt.Errorf("save account: %w", err)
	}
	return s.GetAccount(ctx, email)
}

// GetAccount returns one account by email.
func (s *Store) GetAccount(ctx context.Context, email string) (*Account, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, email, user_id, cookie, model_api_key, proxy, created_at, updated_at
		FROM fm_account
		WHERE email = ?
	`, strings.TrimSpace(email))
	return scanAccount(row)
}

// ListAccounts returns configured accounts without cookies in JSON output.
func (s *Store) ListAccounts(ctx context.Context) ([]Account, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, email, user_id, cookie, model_api_key, proxy, created_at, updated_at
		FROM fm_account
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	accounts := make([]Account, 0)
	for rows.Next() {
		account, errScan := scanAccount(rows)
		if errScan != nil {
			return nil, errScan
		}
		accounts = append(accounts, *account)
	}
	return accounts, rows.Err()
}

// UpdateAccountUserID updates a FreeModel account user ID discovered during sync.
func (s *Store) UpdateAccountUserID(ctx context.Context, email string, userID int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE fm_account
		SET user_id = ?, updated_at = CURRENT_TIMESTAMP
		WHERE email = ?
	`, userID, strings.TrimSpace(email))
	if err != nil {
		return fmt.Errorf("update account user id: %w", err)
	}
	return nil
}

// PickExecutableAccount returns the first configured account that currently has real available balance.
func (s *Store) PickExecutableAccount(ctx context.Context) (*Account, *QuotaSnapshot, error) {
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, account := range accounts {
		snap, errSnap := s.GetLatestSnapshot(ctx, account.Email)
		if errSnap != nil {
			if errors.Is(errSnap, sql.ErrNoRows) {
				continue
			}
			return nil, nil, errSnap
		}
		if snap.AvailabilityStatus == "available" && snap.RealAvailableCents > 0 && strings.TrimSpace(account.ModelAPIKey) != "" {
			return &account, snap, nil
		}
	}
	return nil, nil, sql.ErrNoRows
}

// DeleteAccount deletes an account and its snapshots.
func (s *Store) DeleteAccount(ctx context.Context, email string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM fm_quota_snapshot WHERE email = ?", strings.TrimSpace(email)); err != nil {
		return fmt.Errorf("delete snapshots: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM fm_account WHERE email = ?", strings.TrimSpace(email)); err != nil {
		return fmt.Errorf("delete account: %w", err)
	}
	return tx.Commit()
}

// UpdateAccountProxy updates an account proxy without changing its cookie.
func (s *Store) UpdateAccountProxy(ctx context.Context, email, proxy string) (*Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `
		UPDATE fm_account
		SET proxy = ?, updated_at = CURRENT_TIMESTAMP
		WHERE email = ?
	`, strings.TrimSpace(proxy), strings.TrimSpace(email))
	if err != nil {
		return nil, fmt.Errorf("update proxy: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return nil, sql.ErrNoRows
	}
	return s.GetAccount(ctx, email)
}

// SaveSnapshot stores a quota snapshot for an existing account.
func (s *Store) SaveSnapshot(ctx context.Context, snap QuotaSnapshot) (*QuotaSnapshot, error) {
	account, err := s.GetAccount(ctx, snap.Email)
	if err != nil {
		return nil, fmt.Errorf("lookup account: %w", err)
	}
	if snap.FetchedAt.IsZero() {
		snap.FetchedAt = time.Now().UTC()
	}
	cancelAtPeriodEnd := 0
	if snap.CancelAtPeriodEnd {
		cancelAtPeriodEnd = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO fm_quota_snapshot (
			account_id, email, plan_id, plan_status,
			credit_cents, topup_cents, referral_credits, referral_used,
			current_period_end, cancel_at_period_end,
			window_5h_used, window_5h_limit, window_5h_resets_at,
			window_week_used, window_week_limit, window_week_resets_at,
			total_requests, total_tokens, fetched_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, account.ID, snap.Email, snap.PlanID, snap.PlanStatus,
		snap.CreditCents, snap.TopupCents, snap.ReferralCredits, snap.ReferralUsed,
		snap.CurrentPeriodEnd, cancelAtPeriodEnd,
		snap.Window5h.UsedCents, snap.Window5h.LimitCents, snap.Window5h.ResetsAt,
		snap.WindowWeek.UsedCents, snap.WindowWeek.LimitCents, snap.WindowWeek.ResetsAt,
		snap.TotalRequests, snap.TotalTokens, snap.FetchedAt)
	if err != nil {
		return nil, fmt.Errorf("save snapshot: %w", err)
	}
	snap.ID, _ = result.LastInsertId()
	snap.AccountID = account.ID
	snap.Proxy = account.Proxy
	snap.SyncStatus = "ready"
	setRemaining(&snap)
	return &snap, nil
}

// ListLatestSnapshots returns the latest snapshot for each account and pending rows for unsynced accounts.
func (s *Store) ListLatestSnapshots(ctx context.Context) ([]QuotaSnapshot, error) {
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]QuotaSnapshot, 0, len(accounts))
	for _, account := range accounts {
		snap, errSnap := s.GetLatestSnapshot(ctx, account.Email)
		if errSnap != nil {
			if errors.Is(errSnap, sql.ErrNoRows) {
				pending := QuotaSnapshot{AccountID: account.ID, Email: account.Email, PlanID: "pending", PlanStatus: "syncing", Proxy: account.Proxy, SyncStatus: "pending"}
				setAvailability(&pending)
				snapshots = append(snapshots, pending)
				continue
			}
			return nil, errSnap
		}
		snap.Proxy = account.Proxy
		snap.AccountID = account.ID
		snap.SyncStatus = "ready"
		setRemaining(snap)
		snapshots = append(snapshots, *snap)
	}
	return snapshots, nil
}

// GetLatestSnapshot returns the latest snapshot for an account.
func (s *Store) GetLatestSnapshot(ctx context.Context, email string) (*QuotaSnapshot, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, account_id, email, plan_id, plan_status, credit_cents, topup_cents, referral_credits, referral_used,
			current_period_end, cancel_at_period_end,
			window_5h_used, window_5h_limit, window_5h_resets_at,
			window_week_used, window_week_limit, window_week_resets_at,
			total_requests, total_tokens, fetched_at
		FROM fm_quota_snapshot
		WHERE email = ?
		ORDER BY fetched_at DESC, id DESC
		LIMIT 1
	`, strings.TrimSpace(email))
	snap, err := scanSnapshot(row)
	if err != nil {
		return nil, err
	}
	setRemaining(snap)
	return snap, nil
}

type accountScanner interface {
	Scan(dest ...any) error
}

func scanAccount(scanner accountScanner) (*Account, error) {
	var account Account
	var userID sql.NullInt64
	if err := scanner.Scan(&account.ID, &account.Email, &userID, &account.Cookie, &account.ModelAPIKey, &account.Proxy, &account.CreatedAt, &account.UpdatedAt); err != nil {
		return nil, err
	}
	if userID.Valid {
		account.UserID = userID.Int64
	}
	account.ModelAPIConfigured = strings.TrimSpace(account.ModelAPIKey) != ""
	return &account, nil
}

func setRemaining(snap *QuotaSnapshot) {
	snap.Window5hRemaining = snap.Window5h.LimitCents - snap.Window5h.UsedCents
	if snap.Window5hRemaining < 0 {
		snap.Window5hRemaining = 0
	}
	snap.WindowWeekRemaining = snap.WindowWeek.LimitCents - snap.WindowWeek.UsedCents
	if snap.WindowWeekRemaining < 0 {
		snap.WindowWeekRemaining = 0
	}
	setAvailability(snap)
}

func setAvailability(snap *QuotaSnapshot) {
	snap.WindowAvailableCents = trueWindowAvailableCents(snap.Window5hRemaining, snap.WindowWeekRemaining)
	snap.ExtraAvailableCents = snap.CreditCents + int64(math.Round(snap.ReferralCredits*100))
	if snap.ExtraAvailableCents < 0 {
		snap.ExtraAvailableCents = 0
	}
	snap.SubscriptionExpired = subscriptionExpired(*snap, time.Now())
	if snap.SubscriptionExpired {
		snap.RealAvailableCents = 0
		snap.AvailabilityStatus = "other"
		snap.AvailabilityReason = "subscription_expired"
		return
	}
	snap.RealAvailableCents = snap.WindowAvailableCents + snap.ExtraAvailableCents
	if strings.EqualFold(snap.PlanStatus, "syncing") || strings.EqualFold(snap.PlanID, "pending") {
		snap.AvailabilityStatus = "other"
		snap.AvailabilityReason = "pending_sync"
		return
	}
	if snap.RealAvailableCents > 0 {
		snap.AvailabilityStatus = "available"
		snap.AvailabilityReason = "real_balance_available"
		return
	}
	snap.AvailabilityStatus = "other"
	snap.AvailabilityReason = "no_real_balance"
}

func trueWindowAvailableCents(window5hRemaining, windowWeekRemaining int64) int64 {
	if window5hRemaining <= 0 || windowWeekRemaining <= 0 {
		return 0
	}
	if window5hRemaining < windowWeekRemaining {
		return window5hRemaining
	}
	return windowWeekRemaining
}

func subscriptionExpired(snap QuotaSnapshot, now time.Time) bool {
	status := strings.ToLower(strings.TrimSpace(snap.PlanStatus))
	if status == "expired" || status == "canceled" || status == "cancelled" || status == "inactive" {
		return true
	}
	periodEnd := strings.TrimSpace(snap.CurrentPeriodEnd)
	if periodEnd == "" {
		return false
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z07:00", "2006-01-02"} {
		parsed, err := time.Parse(layout, periodEnd)
		if err == nil {
			return parsed.Before(now)
		}
	}
	return false
}
