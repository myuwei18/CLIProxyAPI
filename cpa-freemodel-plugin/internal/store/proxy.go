package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ProxyNode stores an operator-managed proxy endpoint.
type ProxyNode struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	URL           string    `json:"url"`
	Kind          string    `json:"kind"`
	Status        string    `json:"status"`
	LatencyMS     int64     `json:"latency_ms"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	LastError     string    `json:"last_error"`
	Enabled       bool      `json:"enabled"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// ProxyInput is the create/update payload for a proxy node.
type ProxyInput struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Enabled *bool  `json:"enabled,omitempty"`
}

// ProxyCheckResult is persisted after a proxy health check.
type ProxyCheckResult struct {
	Status        string
	LatencyMS     int64
	LastCheckedAt time.Time
	LastError     string
}

func createProxyTables(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS fm_proxy_node (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT DEFAULT '',
	url TEXT NOT NULL UNIQUE,
	kind TEXT DEFAULT 'unknown',
	status TEXT DEFAULT 'unchecked',
	latency_ms INTEGER DEFAULT 0,
	last_checked_at DATETIME,
	last_error TEXT DEFAULT '',
	enabled INTEGER DEFAULT 1,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fm_proxy_node_status ON fm_proxy_node(status, enabled);
`)
	if err != nil {
		return fmt.Errorf("migrate proxy tables: %w", err)
	}
	return nil
}

// SaveProxy inserts or updates a proxy node.
func (s *Store) SaveProxy(ctx context.Context, input ProxyInput) (*ProxyNode, error) {
	proxyURL := strings.TrimSpace(input.URL)
	if proxyURL == "" {
		return nil, errors.New("proxy url is required")
	}
	kind := ProxyKind(proxyURL)
	if kind == "unknown" {
		return nil, errors.New("unsupported proxy url scheme")
	}
	name := strings.TrimSpace(input.Name)
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO fm_proxy_node (name, url, kind, status, enabled, updated_at)
		VALUES (?, ?, ?, 'unchecked', ?, CURRENT_TIMESTAMP)
		ON CONFLICT(url) DO UPDATE SET
			name = excluded.name,
			kind = excluded.kind,
			enabled = excluded.enabled,
			updated_at = CURRENT_TIMESTAMP
	`, name, proxyURL, kind, boolInt(enabled))
	if err != nil {
		return nil, fmt.Errorf("save proxy: %w", err)
	}
	return s.GetProxyByURL(ctx, proxyURL)
}

// UpdateProxy updates mutable proxy node fields.
func (s *Store) UpdateProxy(ctx context.Context, id int64, input ProxyInput) (*ProxyNode, error) {
	if id <= 0 {
		return nil, errors.New("proxy id is required")
	}
	current, err := s.GetProxy(ctx, id)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = current.Name
	}
	proxyURL := strings.TrimSpace(input.URL)
	if proxyURL == "" {
		proxyURL = current.URL
	}
	kind := ProxyKind(proxyURL)
	if kind == "unknown" {
		return nil, errors.New("unsupported proxy url scheme")
	}
	enabled := current.Enabled
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `
		UPDATE fm_proxy_node
		SET name = ?, url = ?, kind = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, name, proxyURL, kind, boolInt(enabled), id)
	if err != nil {
		return nil, fmt.Errorf("update proxy: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return nil, sql.ErrNoRows
	}
	return s.GetProxy(ctx, id)
}

// DeleteProxy deletes a proxy node.
func (s *Store) DeleteProxy(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("proxy id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `DELETE FROM fm_proxy_node WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete proxy: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// GetProxy returns a proxy node by id.
func (s *Store) GetProxy(ctx context.Context, id int64) (*ProxyNode, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, url, kind, status, latency_ms, last_checked_at, last_error, enabled, created_at, updated_at
		FROM fm_proxy_node WHERE id = ?
	`, id)
	return scanProxy(row)
}

// GetProxyByURL returns a proxy node by URL.
func (s *Store) GetProxyByURL(ctx context.Context, proxyURL string) (*ProxyNode, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, url, kind, status, latency_ms, last_checked_at, last_error, enabled, created_at, updated_at
		FROM fm_proxy_node WHERE url = ?
	`, strings.TrimSpace(proxyURL))
	return scanProxy(row)
}

// ListProxies returns proxy nodes ordered by id.
func (s *Store) ListProxies(ctx context.Context) ([]ProxyNode, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, url, kind, status, latency_ms, last_checked_at, last_error, enabled, created_at, updated_at
		FROM fm_proxy_node ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]ProxyNode, 0)
	for rows.Next() {
		item, errScan := scanProxy(rows)
		if errScan != nil {
			return nil, errScan
		}
		out = append(out, *item)
	}
	return out, rows.Err()
}

// UpdateProxyCheckResult persists a proxy health check result.
func (s *Store) UpdateProxyCheckResult(ctx context.Context, id int64, result ProxyCheckResult) (*ProxyNode, error) {
	if result.LastCheckedAt.IsZero() {
		result.LastCheckedAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `
		UPDATE fm_proxy_node
		SET status = ?, latency_ms = ?, last_checked_at = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, strings.TrimSpace(result.Status), result.LatencyMS, result.LastCheckedAt, strings.TrimSpace(result.LastError), id)
	if err != nil {
		return nil, fmt.Errorf("update proxy check result: %w", err)
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return nil, sql.ErrNoRows
	}
	return s.GetProxy(ctx, id)
}

func scanProxy(scanner interface{ Scan(dest ...any) error }) (*ProxyNode, error) {
	var item ProxyNode
	var checked sql.NullTime
	var enabled int
	if err := scanner.Scan(&item.ID, &item.Name, &item.URL, &item.Kind, &item.Status, &item.LatencyMS, &checked, &item.LastError, &enabled, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	if checked.Valid {
		item.LastCheckedAt = checked.Time
	}
	item.Enabled = enabled == 1
	return &item, nil
}

// ProxyKind returns a normalized proxy kind.
func ProxyKind(proxyURL string) string {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" || strings.EqualFold(proxyURL, "direct") || strings.EqualFold(proxyURL, "none") {
		return "direct"
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return "unknown"
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "unknown"
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "socks5", "socks5h":
		return strings.ToLower(parsed.Scheme)
	default:
		return "unknown"
	}
}
