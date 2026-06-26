package management

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

const proxyCheckURL = "https://freemodel.dev/api/auth/me"

type proxySubscriptionImportRequest struct {
	URL        string `json:"url"`
	NamePrefix string `json:"name_prefix"`
	DryRun     bool   `json:"dry_run"`
}

type proxySubscriptionImportResult struct {
	Total           int               `json:"total"`
	Imported        int               `json:"imported"`
	Skipped         int               `json:"skipped"`
	DryRun          bool              `json:"dry_run"`
	Schemes         map[string]int    `json:"schemes"`
	Errors          []string          `json:"errors,omitempty"`
	ImportedPreview []publicProxyNode `json:"imported_preview,omitempty"`
}

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

func handleProxySubscriptionImport(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var input proxySubscriptionImportRequest
	if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	items, errImport := importProxySubscription(context.Background(), db, input)
	if errImport != nil {
		return jsonResponse(http.StatusBadRequest, failure(safeProxyError(errImport)))
	}
	return jsonResponse(http.StatusOK, success(items))
}

func importProxySubscription(ctx context.Context, db *store.Store, input proxySubscriptionImportRequest) (proxySubscriptionImportResult, error) {
	result := proxySubscriptionImportResult{DryRun: input.DryRun, Schemes: map[string]int{}}
	subURL := strings.TrimSpace(input.URL)
	if subURL == "" {
		return result, errors.New("subscription url is required")
	}
	parsedSub, errParse := url.Parse(subURL)
	if errParse != nil || parsedSub.Scheme == "" || parsedSub.Host == "" {
		return result, errors.New("invalid subscription url")
	}
	body, errFetch := fetchProxySubscription(subURL)
	if errFetch != nil {
		return result, fmt.Errorf("subscription fetch failed: %s", safeProxyError(errFetch))
	}
	lines := parseProxySubscriptionLines(string(body))
	result.Total = len(lines)
	prefix := strings.TrimSpace(input.NamePrefix)
	if prefix == "" {
		prefix = "sub"
	}
	for idx, line := range lines {
		kind := store.ProxyKind(line)
		if kind == "unknown" || kind == "direct" {
			result.Skipped++
			if parsed, err := url.Parse(line); err == nil && parsed.Scheme != "" {
				result.Schemes[strings.ToLower(parsed.Scheme)]++
			}
			continue
		}
		result.Schemes[kind]++
		if input.DryRun {
			result.Imported++
			if len(result.ImportedPreview) < 5 {
				result.ImportedPreview = append(result.ImportedPreview, publicProxyNode{ID: 0, Name: fmt.Sprintf("%s-%03d", prefix, idx+1), URL: maskProxy(line), Kind: kind, Status: "unchecked", Enabled: true})
			}
			continue
		}
		item, errSave := db.SaveProxy(ctx, store.ProxyInput{Name: fmt.Sprintf("%s-%03d", prefix, idx+1), URL: line})
		if errSave != nil {
			result.Skipped++
			if len(result.Errors) < 5 {
				result.Errors = append(result.Errors, safeProxyError(errSave))
			}
			continue
		}
		result.Imported++
		if len(result.ImportedPreview) < 5 {
			result.ImportedPreview = append(result.ImportedPreview, proxyPublic(*item))
		}
	}
	return result, nil
}

func fetchProxySubscription(subURL string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	req, errReq := http.NewRequest(http.MethodGet, subURL, nil)
	if errReq != nil {
		return nil, errReq
	}
	req.Header.Set("User-Agent", "Clash.Meta/1.18 cpa-freemodel-plugin")
	req.Header.Set("Accept", "text/plain, */*")
	resp, errDo := client.Do(req)
	if errDo != nil {
		return nil, errDo
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, errRead := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if errRead != nil {
		return nil, errRead
	}
	return body, nil
}

func parseProxySubscriptionLines(raw string) []string {
	content := strings.TrimSpace(raw)
	if content == "" {
		return nil
	}
	if decoded, err := base64.StdEncoding.DecodeString(content); err == nil && looksLikeProxyList(string(decoded)) {
		content = string(decoded)
	} else if decoded, err := base64.RawStdEncoding.DecodeString(content); err == nil && looksLikeProxyList(string(decoded)) {
		content = string(decoded)
	}
	seen := map[string]bool{}
	lines := make([]string, 0)
	for _, line := range strings.FieldsFunc(content, func(r rune) bool { return r == '\n' || r == '\r' || r == '\t' }) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	return lines
}

func looksLikeProxyList(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"http://", "https://", "socks5://", "socks5h://", "vmess://", "vless://", "trojan://", "ss://"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
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
	if kind == "socks5" || kind == "socks5h" {
		result.Status = "invalid"
		result.LastError = "socks proxy check not supported in this build"
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
	lower := strings.ToLower(value)
	for _, marker := range []string{"password", "token", "authorization", "cookie", "session", "fe_oa_", "api key", "apikey"} {
		if strings.Contains(lower, marker) {
			return "代理不可用"
		}
	}
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline") || strings.Contains(lower, "timed out") {
		return "请求超时"
	}
	if strings.Contains(lower, "no such host") || strings.Contains(lower, "lookup") || strings.Contains(lower, "dns") {
		return "目标不可达"
	}
	if strings.Contains(lower, "connection refused") || strings.Contains(lower, "connection reset") || strings.Contains(lower, "proxyconnect") || strings.Contains(lower, "connect:") || strings.Contains(lower, "eof") || strings.Contains(lower, "tls") {
		return "连接失败"
	}
	if strings.Contains(lower, "invalid") || strings.Contains(lower, "unsupported") || strings.Contains(lower, "socks") {
		return "代理不可用"
	}
	if strings.Contains(lower, "http ") {
		return "目标返回异常"
	}
	return "代理不可用"
}
