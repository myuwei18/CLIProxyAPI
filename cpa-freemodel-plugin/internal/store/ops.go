package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Incident records a provider-specific operational event.
type Incident struct {
	ID           int64     `json:"id"`
	Kind         string    `json:"kind"`
	Severity     string    `json:"severity"`
	StatusCode   int       `json:"status_code"`
	Message      string    `json:"message"`
	ResetHint    string    `json:"reset_hint,omitempty"`
	AccountEmail string    `json:"account_email,omitempty"`
	Model        string    `json:"model,omitempty"`
	Path         string    `json:"path,omitempty"`
	RequestID    string    `json:"request_id,omitempty"`
	DetectedAt   time.Time `json:"detected_at"`
	ResolvedAt   time.Time `json:"resolved_at,omitempty"`
	Resolved     bool      `json:"resolved"`
}

// ReconciliationRecord stores normalized supplier-side request cost data.
type ReconciliationRecord struct {
	ID                int64     `json:"id"`
	UpstreamRequestID string    `json:"upstream_request_id"`
	AccountEmail      string    `json:"account_email,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	Method            string    `json:"method"`
	Path              string    `json:"path"`
	Model             string    `json:"model"`
	TokensIn          int64     `json:"tokens_in"`
	TokensOut         int64     `json:"tokens_out"`
	CacheReadTokens   int64     `json:"cache_read_tokens"`
	CacheWriteTokens  int64     `json:"cache_write_tokens"`
	RawSubtotalUSD    float64   `json:"raw_subtotal_usd"`
	ChargedUSD        float64   `json:"charged_usd"`
	Status            int       `json:"status"`
}

func createOpsTables(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS fm_incident (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	kind TEXT NOT NULL,
	severity TEXT NOT NULL,
	status_code INTEGER DEFAULT 0,
	message TEXT DEFAULT '',
	reset_hint TEXT DEFAULT '',
	account_email TEXT DEFAULT '',
	model TEXT DEFAULT '',
	path TEXT DEFAULT '',
	request_id TEXT DEFAULT '',
	detected_at DATETIME NOT NULL,
	resolved_at DATETIME,
	resolved INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_fm_incident_detected ON fm_incident(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_fm_incident_kind ON fm_incident(kind, resolved);

CREATE TABLE IF NOT EXISTS fm_reconciliation_record (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	upstream_request_id TEXT DEFAULT '',
	account_email TEXT DEFAULT '',
	created_at DATETIME NOT NULL,
	method TEXT DEFAULT '',
	path TEXT DEFAULT '',
	model TEXT DEFAULT '',
	tokens_in INTEGER DEFAULT 0,
	tokens_out INTEGER DEFAULT 0,
	cache_read_tokens INTEGER DEFAULT 0,
	cache_write_tokens INTEGER DEFAULT 0,
	raw_subtotal_usd REAL DEFAULT 0,
	charged_usd REAL DEFAULT 0,
	status INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_fm_recon_created ON fm_reconciliation_record(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fm_recon_model ON fm_reconciliation_record(model);
`)
	if err != nil {
		return fmt.Errorf("migrate ops tables: %w", err)
	}
	return nil
}

// ListSnapshots returns quota snapshots ordered by fetch time descending.
func (s *Store) ListSnapshots(ctx context.Context, limit int) ([]QuotaSnapshot, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, account_id, email, plan_id, plan_status, credit_cents, topup_cents, referral_credits, referral_used,
			current_period_end, cancel_at_period_end,
			window_5h_used, window_5h_limit, window_5h_resets_at,
			window_week_used, window_week_limit, window_week_resets_at,
			total_requests, total_tokens, fetched_at
		FROM fm_quota_snapshot
		ORDER BY fetched_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]QuotaSnapshot, 0)
	for rows.Next() {
		snap, errScan := scanSnapshot(rows)
		if errScan != nil {
			return nil, errScan
		}
		if account, errAccount := s.GetAccount(ctx, snap.Email); errAccount == nil {
			snap.Proxy = account.Proxy
			markSnapshotAccountState(snap, *account)
		}
		snap.SyncStatus = "ready"
		setRemaining(snap)
		out = append(out, *snap)
	}
	return out, rows.Err()
}

// ListIncidents returns recent provider incidents.
func (s *Store) ListIncidents(ctx context.Context, limit int, includeResolved bool) ([]Incident, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT id, kind, severity, status_code, message, reset_hint, account_email, model, path, request_id, detected_at, resolved_at, resolved FROM fm_incident`
	args := []any{}
	if !includeResolved {
		query += ` WHERE resolved = 0`
	}
	query += ` ORDER BY detected_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Incident, 0)
	for rows.Next() {
		var item Incident
		var resolvedAt sql.NullTime
		var resolved int
		if err := rows.Scan(&item.ID, &item.Kind, &item.Severity, &item.StatusCode, &item.Message, &item.ResetHint, &item.AccountEmail, &item.Model, &item.Path, &item.RequestID, &item.DetectedAt, &resolvedAt, &resolved); err != nil {
			return nil, err
		}
		item.Resolved = resolved == 1
		if resolvedAt.Valid {
			item.ResolvedAt = resolvedAt.Time
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// SaveIncident stores an operational incident.
func (s *Store) SaveIncident(ctx context.Context, item Incident) (*Incident, error) {
	if item.DetectedAt.IsZero() {
		item.DetectedAt = time.Now().UTC()
	}
	item.Kind = strings.TrimSpace(item.Kind)
	if item.Kind == "" {
		return nil, fmt.Errorf("incident kind is required")
	}
	if item.Severity == "" {
		item.Severity = "info"
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO fm_incident (kind, severity, status_code, message, reset_hint, account_email, model, path, request_id, detected_at, resolved)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, item.Kind, item.Severity, item.StatusCode, item.Message, item.ResetHint, item.AccountEmail, item.Model, item.Path, item.RequestID, item.DetectedAt, boolInt(item.Resolved))
	if err != nil {
		return nil, fmt.Errorf("save incident: %w", err)
	}
	item.ID, _ = result.LastInsertId()
	return &item, nil
}

// ListReconciliationRecords returns normalized supplier cost records.
func (s *Store) ListReconciliationRecords(ctx context.Context, limit int) ([]ReconciliationRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, upstream_request_id, account_email, created_at, method, path, model,
			tokens_in, tokens_out, cache_read_tokens, cache_write_tokens,
			raw_subtotal_usd, charged_usd, status
		FROM fm_reconciliation_record
		ORDER BY created_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list reconciliation records: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ReconciliationRecord, 0)
	for rows.Next() {
		var item ReconciliationRecord
		if err := rows.Scan(&item.ID, &item.UpstreamRequestID, &item.AccountEmail, &item.CreatedAt, &item.Method, &item.Path, &item.Model, &item.TokensIn, &item.TokensOut, &item.CacheReadTokens, &item.CacheWriteTokens, &item.RawSubtotalUSD, &item.ChargedUSD, &item.Status); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// SaveReconciliationRecord stores a normalized supplier cost record.
func (s *Store) SaveReconciliationRecord(ctx context.Context, item ReconciliationRecord) (*ReconciliationRecord, error) {
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO fm_reconciliation_record (upstream_request_id, account_email, created_at, method, path, model, tokens_in, tokens_out, cache_read_tokens, cache_write_tokens, raw_subtotal_usd, charged_usd, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, item.UpstreamRequestID, item.AccountEmail, item.CreatedAt, item.Method, item.Path, item.Model, item.TokensIn, item.TokensOut, item.CacheReadTokens, item.CacheWriteTokens, item.RawSubtotalUSD, item.ChargedUSD, item.Status)
	if err != nil {
		return nil, fmt.Errorf("save reconciliation record: %w", err)
	}
	item.ID, _ = result.LastInsertId()
	return &item, nil
}

func scanSnapshot(scanner interface{ Scan(dest ...any) error }) (*QuotaSnapshot, error) {
	var snap QuotaSnapshot
	var cancelAtPeriodEnd int
	if err := scanner.Scan(&snap.ID, &snap.AccountID, &snap.Email, &snap.PlanID, &snap.PlanStatus,
		&snap.CreditCents, &snap.TopupCents, &snap.ReferralCredits, &snap.ReferralUsed,
		&snap.CurrentPeriodEnd, &cancelAtPeriodEnd,
		&snap.Window5h.UsedCents, &snap.Window5h.LimitCents, &snap.Window5h.ResetsAt,
		&snap.WindowWeek.UsedCents, &snap.WindowWeek.LimitCents, &snap.WindowWeek.ResetsAt,
		&snap.TotalRequests, &snap.TotalTokens, &snap.FetchedAt); err != nil {
		return nil, err
	}
	snap.CancelAtPeriodEnd = cancelAtPeriodEnd == 1
	return &snap, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
