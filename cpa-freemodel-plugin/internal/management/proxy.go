package management

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

const proxyCheckURL = "https://freemodel.dev/api/auth/me"

type publicProxyNode struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	URL           string `json:"url"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	LatencyMS     int64  `json:"latency_ms"`
	LastCheckedAt string `json:"last_checked_at"`
	LastError     string `json:"last_error"`
	Enabled       bool   `json:"enabled"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

func handleProxies(req managementRequest) ([]byte, error) {
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	ctx := context.Background()
	switch strings.ToUpper(req.Method) {
	case http.MethodGet:
		items, errList := db.ListProxies(ctx)
		if errList != nil {
			return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
		}
		return jsonResponse(http.StatusOK, success(proxiesPublic(items)))
	case http.MethodPost:
		var input store.ProxyInput
		if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
			return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
		}
		item, errSave := db.SaveProxy(ctx, input)
		if errSave != nil {
			return jsonResponse(http.StatusBadRequest, failure(safeProxyError(errSave)))
		}
		return jsonResponse(http.StatusOK, success(proxyPublic(*item)))
	case http.MethodPatch:
		id, errID := proxyIDFromRequest(req)
		if errID != nil {
			return jsonResponse(http.StatusBadRequest, failure(errID.Error()))
		}
		var input store.ProxyInput
		if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
			return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
		}
		item, errUpdate := db.UpdateProxy(ctx, id, input)
		if errUpdate != nil {
			status := http.StatusInternalServerError
			if errors.Is(errUpdate, sql.ErrNoRows) {
				status = http.StatusNotFound
			}
			return jsonResponse(status, failure(safeProxyError(errUpdate)))
		}
		return jsonResponse(http.StatusOK, success(proxyPublic(*item)))
	case http.MethodDelete:
		id, errID := proxyIDFromRequest(req)
		if errID != nil {
			return jsonResponse(http.StatusBadRequest, failure(errID.Error()))
		}
		if errDelete := db.DeleteProxy(ctx, id); errDelete != nil {
			status := http.StatusInternalServerError
			if errors.Is(errDelete, sql.ErrNoRows) {
				status = http.StatusNotFound
			}
			return jsonResponse(status, failure(safeProxyError(errDelete)))
		}
		return jsonResponse(http.StatusOK, map[string]any{"success": true, "message": "proxy deleted"})
	default:
		return methodNotAllowed()
	}
}

func handleProxyCheck(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	ctx := context.Background()
	id, errID := proxyIDFromRequest(req)
	if errID != nil {
		return jsonResponse(http.StatusBadRequest, failure(errID.Error()))
	}
	item, errGet := db.GetProxy(ctx, id)
	if errGet != nil {
		status := http.StatusInternalServerError
		if errors.Is(errGet, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		return jsonResponse(status, failure(safeProxyError(errGet)))
	}
	checked := checkProxyNode(*item)
	updated, errUpdate := db.UpdateProxyCheckResult(ctx, item.ID, checked)
	if errUpdate != nil {
		return jsonResponse(http.StatusInternalServerError, failure(safeProxyError(errUpdate)))
	}
	if checked.Status != "online" {
		_, _ = db.SaveIncident(ctx, store.Incident{Kind: "proxy_failed", Severity: "warning", Message: checked.LastError, DetectedAt: time.Now().UTC()})
	}
	return jsonResponse(http.StatusOK, success(proxyPublic(*updated)))
}

func handleProxyCheckAll(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	ctx := context.Background()
	items, errList := db.ListProxies(ctx)
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	out := make([]publicProxyNode, 0, len(items))
	for _, item := range items {
		checked := checkProxyNode(item)
		updated, errUpdate := db.UpdateProxyCheckResult(ctx, item.ID, checked)
		if errUpdate != nil {
			continue
		}
		if checked.Status != "online" {
			_, _ = db.SaveIncident(ctx, store.Incident{Kind: "proxy_failed", Severity: "warning", Message: checked.LastError, DetectedAt: time.Now().UTC()})
		}
		out = append(out, proxyPublic(*updated))
	}
	return jsonResponse(http.StatusOK, success(out))
}

func proxyIDFromRequest(req managementRequest) (int64, error) {
	idText := firstQuery(req.Query, "id")
	if idText == "" {
		var body struct {
			ID int64 `json:"id"`
		}
		_ = json.Unmarshal(req.Body, &body)
		if body.ID > 0 {
			return body.ID, nil
		}
	}
	id, err := strconv.ParseInt(strings.TrimSpace(idText), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("proxy id is required")
	}
	return id, nil
}

func checkProxyNode(item store.ProxyNode) store.ProxyCheckResult {
	started := time.Now()
	result := store.ProxyCheckResult{Status: "offline", LastCheckedAt: started.UTC()}
	kind := store.ProxyKind(item.URL)
	if kind == "unknown" {
		result.Status = "invalid"
		result.LastError = "invalid proxy url"
		return result
	}
	client, errClient := proxyHTTPClient(item.URL)
	if errClient != nil {
		result.Status = "invalid"
		result.LastError = safeProxyError(errClient)
		return result
	}
	req, errReq := http.NewRequest(http.MethodGet, proxyCheckURL, nil)
	if errReq != nil {
		result.Status = "invalid"
		result.LastError = safeProxyError(errReq)
		return result
	}
	req.Header.Set("User-Agent", "cpa-freemodel-plugin/0.1")
	resp, errDo := client.Do(req)
	result.LatencyMS = time.Since(started).Milliseconds()
	if errDo != nil {
		result.LastError = safeProxyError(errDo)
		return result
	}
	defer func() { _ = resp.Body.Close() }()
	// 401 means the FreeModel dashboard endpoint is reachable without valid auth.
	if resp.StatusCode >= 200 && resp.StatusCode < 500 {
		result.Status = "online"
		result.LastError = ""
		return result
	}
	result.LastError = fmt.Sprintf("HTTP %d", resp.StatusCode)
	return result
}

func proxyHTTPClient(proxyValue string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	proxyValue = strings.TrimSpace(proxyValue)
	if proxyValue != "" && !strings.EqualFold(proxyValue, "direct") && !strings.EqualFold(proxyValue, "none") {
		parsed, err := url.Parse(proxyValue)
		if err != nil {
			return nil, err
		}
		transport.Proxy = http.ProxyURL(parsed)
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, nil
}

func proxiesPublic(items []store.ProxyNode) []publicProxyNode {
	out := make([]publicProxyNode, 0, len(items))
	for _, item := range items {
		out = append(out, proxyPublic(item))
	}
	return out
}

func proxyPublic(item store.ProxyNode) publicProxyNode {
	return publicProxyNode{ID: item.ID, Name: item.Name, URL: maskProxy(item.URL), Kind: item.Kind, Status: item.Status, LatencyMS: item.LatencyMS, LastCheckedAt: formatTime(item.LastCheckedAt), LastError: safeProxyErrorString(item.LastError), Enabled: item.Enabled, CreatedAt: formatTime(item.CreatedAt), UpdatedAt: formatTime(item.UpdatedAt)}
}

func safeProxyError(err error) string {
	if err == nil {
		return ""
	}
	return safeProxyErrorString(err.Error())
}

func safeProxyErrorString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = maskProxy(value)
	for _, marker := range []string{"password", "token", "authorization", "cookie", "session", "fe_oa_"} {
		if strings.Contains(strings.ToLower(value), marker) {
			return "proxy request failed"
		}
	}
	if len(value) > 160 {
		value = value[:160]
	}
	return value
}
