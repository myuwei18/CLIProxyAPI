package management

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

type publicIncident struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	Severity     string `json:"severity"`
	Status       string `json:"status"`
	StatusCode   int    `json:"status_code"`
	Message      string `json:"message"`
	ResetHint    string `json:"reset_hint,omitempty"`
	AccountEmail string `json:"account_email,omitempty"`
	EmailHash    string `json:"email_hash,omitempty"`
	Model        string `json:"model,omitempty"`
	Path         string `json:"path,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
	DetectedAt   string `json:"detected_at"`
	ResolvedAt   string `json:"resolved_at,omitempty"`
	Resolved     bool   `json:"resolved"`
	Source       string `json:"source"`
}

type publicReconciliationRecord struct {
	ID                int64   `json:"id"`
	UpstreamRequestID string  `json:"upstream_request_id"`
	AccountEmail      string  `json:"account_email,omitempty"`
	EmailHash         string  `json:"email_hash,omitempty"`
	CreatedAt         string  `json:"created_at"`
	Method            string  `json:"method"`
	Path              string  `json:"path"`
	Model             string  `json:"model"`
	TokensIn          int64   `json:"tokens_in"`
	TokensOut         int64   `json:"tokens_out"`
	CacheReadTokens   int64   `json:"cache_read_tokens"`
	CacheWriteTokens  int64   `json:"cache_write_tokens"`
	RawSubtotalUSD    float64 `json:"raw_subtotal_usd"`
	ChargedUSD        float64 `json:"charged_usd"`
	Status            int     `json:"status"`
	ErrorType         string  `json:"error_type,omitempty"`
}

func handleIncidents(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	items, errList := db.ListIncidents(context.Background(), queryInt(req.Query, "limit", 100), queryBool(req.Query, "include_resolved"))
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	return jsonResponse(http.StatusOK, map[string]any{"success": true, "data": incidentsPublic(items)})
}

func handleUsageSnapshots(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	items, errList := db.ListSnapshots(context.Background(), queryInt(req.Query, "limit", 100))
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"success":  true,
		"provider": "freemodel",
		"data":     usageSnapshotsPublic(items),
	})
}

func handleReconciliationSource(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	items, errList := db.ListReconciliationRecords(context.Background(), queryInt(req.Query, "limit", 100))
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"success":  true,
		"provider": "freemodel",
		"currency": "USD",
		"rounding": "ceil_to_cent_per_request",
		"data":     reconciliationPublic(items),
	})
}

func incidentsPublic(items []store.Incident) []publicIncident {
	out := make([]publicIncident, 0, len(items))
	for _, item := range items {
		pub := publicIncident{
			ID:           item.ID,
			Kind:         item.Kind,
			Severity:     item.Severity,
			Status:       incidentStatus(item),
			StatusCode:   item.StatusCode,
			Message:      item.Message,
			ResetHint:    item.ResetHint,
			AccountEmail: maskEmail(item.AccountEmail),
			EmailHash:    hashStable(item.AccountEmail),
			Model:        item.Model,
			Path:         item.Path,
			RequestID:    shortHash(item.RequestID),
			DetectedAt:   formatTime(item.DetectedAt),
			Resolved:     item.Resolved,
			Source:       "sync_worker",
		}
		if !item.ResolvedAt.IsZero() {
			pub.ResolvedAt = formatTime(item.ResolvedAt)
		}
		out = append(out, pub)
	}
	return out
}

func reconciliationPublic(items []store.ReconciliationRecord) []publicReconciliationRecord {
	out := make([]publicReconciliationRecord, 0, len(items))
	for _, item := range items {
		out = append(out, publicReconciliationRecord{
			ID:                item.ID,
			UpstreamRequestID: shortHash(item.UpstreamRequestID),
			AccountEmail:      maskEmail(item.AccountEmail),
			EmailHash:         hashStable(item.AccountEmail),
			CreatedAt:         formatTime(item.CreatedAt),
			Method:            item.Method,
			Path:              item.Path,
			Model:             item.Model,
			TokensIn:          item.TokensIn,
			TokensOut:         item.TokensOut,
			CacheReadTokens:   item.CacheReadTokens,
			CacheWriteTokens:  item.CacheWriteTokens,
			RawSubtotalUSD:    item.RawSubtotalUSD,
			ChargedUSD:        item.ChargedUSD,
			Status:            item.Status,
			ErrorType:         reconciliationErrorType(item.Status),
		})
	}
	return out
}

func incidentStatus(item store.Incident) string {
	if item.Resolved {
		return "resolved"
	}
	return "open"
}

func reconciliationErrorType(status int) string {
	if status >= 200 && status < 400 {
		return ""
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return "auth_error"
	}
	if status == http.StatusRequestTimeout {
		return "timeout"
	}
	if status == http.StatusRequestEntityTooLarge {
		return "payload_too_large"
	}
	if status == http.StatusTooManyRequests {
		return "rate_limited"
	}
	if status >= 500 {
		return "upstream_5xx"
	}
	if status > 0 {
		return "upstream_error"
	}
	return ""
}

func queryInt(values map[string][]string, key string, fallback int) int {
	raw := firstQuery(values, key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func queryBool(values map[string][]string, key string) bool {
	switch strings.ToLower(firstQuery(values, key)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}
