package management

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	freemodelsvc "github.com/myuwei18/cpa-freemodel-plugin/internal/freemodel"
	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

const (
	pluginID      = "cpa-freemodel"
	pluginName    = "cpa-freemodel-plugin"
	pluginVersion = "0.2.0-poc"
	schemaVersion = "2026-06-24.p1"
)

var runtimeState = &state{}

type state struct {
	mu              sync.Mutex
	store           *store.Store
	syncer          *freemodelsvc.Service
	dbPath          string
	modelAPIBaseURL string
	modelExecutor   bool
	lastErr         string
}

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

type hostCallerFunc func(method string, payload []byte) ([]byte, error)

var hostCaller hostCallerFunc

// SetHostCaller installs the native ABI host callback bridge.
func SetHostCaller(fn hostCallerFunc) {
	hostCaller = fn
}

type lifecycleRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

type pluginConfig struct {
	DataDir         string `yaml:"data_dir"`
	DBPath          string `yaml:"db_path"`
	ModelAPIBaseURL string `yaml:"model_api_base_url"`
	ModelExecutor   bool   `yaml:"model_executor_enabled"`
}

type managementRequest struct {
	Method string              `json:"Method"`
	Path   string              `json:"Path"`
	Query  map[string][]string `json:"Query"`
	Body   []byte              `json:"Body"`
}

type managementResponse struct {
	StatusCode int                 `json:"StatusCode"`
	Headers    map[string][]string `json:"Headers,omitempty"`
	Body       []byte              `json:"Body,omitempty"`
}

type healthResponse struct {
	Success              bool   `json:"success"`
	Status               string `json:"status"`
	PluginID             string `json:"plugin_id"`
	Name                 string `json:"name"`
	Version              string `json:"version"`
	SchemaVersion        string `json:"schema_version"`
	Timestamp            string `json:"timestamp"`
	StoreStatus          string `json:"store_status"`
	StoreOK              bool   `json:"store_ok"`
	SyncWorkerStatus     string `json:"sync_worker_status"`
	LastSyncAt           string `json:"last_sync_at,omitempty"`
	LastError            string `json:"last_error,omitempty"`
	ModelExecutorEnabled bool   `json:"model_executor_enabled"`
	ExecutorStatus       string `json:"executor_status"`
	DBPath               string `json:"db_path,omitempty"`
}

// HandleMethod dispatches CLIProxyAPI plugin ABI methods.
func HandleMethod(method string, request []byte) (raw []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			raw = ErrorEnvelope("plugin_panic", fmt.Sprintf("plugin panic: %v", recovered))
			err = nil
		}
	}()
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := runtimeState.configure(request); err != nil {
			runtimeState.setError(err)
		} else {
			runtimeState.setError(nil)
		}
		return okEnvelopeJSON(registrationJSON())
	case "model.static", "model.for_auth":
		return okEnvelopeJSON(staticModelsJSON())
	case "model.route":
		return routeModel(request)
	case "executor.identifier":
		return okEnvelopeJSON(`{"identifier":"` + providerID + `"}`)
	case "executor.execute":
		return executeModel(request, false)
	case "executor.execute_stream":
		return executeModel(request, true)
	case "executor.count_tokens":
		return okEnvelope(executorResponse{Payload: []byte(`{"total_tokens":0}`), Headers: map[string][]string{"content-type": []string{"application/json"}}})
	case "management.register":
		return okEnvelopeJSON(managementRegistrationJSON())
	case "management.handle":
		return handleManagement(request)
	default:
		return ErrorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func (s *state) configure(raw []byte) error {
	cfg := pluginConfig{DataDir: filepath.Join("plugins", "cpa-freemodel-data"), ModelAPIBaseURL: defaultModelAPIBase}
	if len(raw) > 0 {
		var req lifecycleRequest
		if err := json.Unmarshal(raw, &req); err == nil && len(req.ConfigYAML) > 0 {
			parseSimpleYAML(req.ConfigYAML, &cfg)
		}
	}
	dbPath := strings.TrimSpace(cfg.DBPath)
	if dbPath == "" {
		dataDir := strings.TrimSpace(cfg.DataDir)
		if dataDir == "" {
			dataDir = filepath.Join("plugins", "cpa-freemodel-data")
		}
		dbPath = filepath.Join(dataDir, "freemodel.db")
	}
	modelAPIBaseURL := normalizeModelAPIBaseURL(cfg.ModelAPIBaseURL)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != nil && s.dbPath == dbPath {
		s.modelAPIBaseURL = modelAPIBaseURL
		s.modelExecutor = cfg.ModelExecutor
		return nil
	}
	next, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	previous := s.store
	s.store = next
	s.syncer = freemodelsvc.NewService(next)
	s.dbPath = dbPath
	s.modelAPIBaseURL = modelAPIBaseURL
	s.modelExecutor = cfg.ModelExecutor
	if previous != nil {
		_ = previous.Close()
	}
	return nil
}

func (s *state) modelAPIBase() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return normalizeModelAPIBaseURL(s.modelAPIBaseURL)
}

func (s *state) modelExecutorEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.modelExecutor
}

func (s *state) getStore() (*store.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store == nil {
		return nil, errors.New("store is not initialized")
	}
	return s.store, nil
}

func (s *state) health() healthResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastSyncAt := ""
	syncWorkerStatus := "disabled"
	if s.syncer != nil {
		syncWorkerStatus = "idle"
		if result := s.syncer.LastResult(); result != nil {
			if !result.FinishedAt.IsZero() {
				lastSyncAt = formatTime(result.FinishedAt)
			}
			if result.Failed > 0 {
				syncWorkerStatus = "last_sync_failed"
			}
		}
	}
	storeStatus := "unavailable"
	if s.store != nil {
		storeStatus = "ok"
	}
	executorStatus := "disabled"
	if s.modelExecutor {
		executorStatus = "experimental_enabled"
	}
	return healthResponse{
		Success:              true,
		Status:               "ok",
		PluginID:             pluginID,
		Name:                 pluginName,
		Version:              pluginVersion,
		SchemaVersion:        schemaVersion,
		DBPath:               s.dbPath,
		Timestamp:            time.Now().UTC().Format(time.RFC3339),
		StoreStatus:          storeStatus,
		StoreOK:              s.store != nil,
		SyncWorkerStatus:     syncWorkerStatus,
		LastSyncAt:           lastSyncAt,
		LastError:            s.lastErr,
		ModelExecutorEnabled: s.modelExecutor,
		ExecutorStatus:       executorStatus,
	}
}

func (s *state) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.lastErr = ""
		return
	}
	s.lastErr = err.Error()
}

func registrationJSON() string {
	capabilities := `"management_api":true`
	if runtimeState.modelExecutorEnabled() {
		capabilities = `"model_provider":true,"model_router":true,"executor":true,"executor_model_scope":"static","executor_input_formats":["chat-completions","responses","openai"],"executor_output_formats":["chat-completions","responses","openai"],` + capabilities
	}
	return `{"schema_version":1,"metadata":{"Name":"` + pluginName + `","Version":"` + pluginVersion + `","Author":"myuwei18","GitHubRepository":"https://github.com/myuwei18/cpa-freemodel-plugin","Logo":"","ConfigFields":[{"Name":"data_dir","Type":"string","Description":"Plugin-owned data directory for freemodel.db and gost state."},{"Name":"db_path","Type":"string","Description":"Optional explicit SQLite database path. Defaults to data_dir/freemodel.db."},{"Name":"model_api_base_url","Type":"string","Description":"FreeModel OpenAI-compatible model API base URL. Defaults to https://api.freemodel.dev/v1."},{"Name":"model_executor_enabled","Type":"boolean","Description":"Experimental: expose model_provider/model_router/executor capabilities for CPA-as-channel validation. Defaults to false."}]},"capabilities":{` + capabilities + `}}`
}

func managementRegistrationJSON() string {
	return `{"routes":[` + strings.Join([]string{
		`{"Method":"GET","Path":"/freemodel-plugin/health","Description":"FreeModel plugin health check."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/accounts","Description":"List stored FreeModel accounts."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/accounts","Description":"Create or update a FreeModel account."}`,
		`{"Method":"DELETE","Path":"/freemodel-plugin/accounts","Description":"Delete a FreeModel account by email query."}`,
		`{"Method":"PATCH","Path":"/freemodel-plugin/accounts/proxy","Description":"Update a FreeModel account proxy URL."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/auth/send-otp","Description":"Send a FreeModel login OTP."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/auth/verify-otp","Description":"Verify a FreeModel OTP and save the account."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/sync","Description":"Synchronize all FreeModel account quota snapshots."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/sync-status","Description":"Return the latest FreeModel sync result."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/quota","Description":"List latest quota snapshots with pending accounts."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/incidents","Description":"List provider-specific incidents."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/usage-snapshots","Description":"List quota and usage snapshots for operations."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/reconciliation-source","Description":"List normalized supplier reconciliation records."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/export/accounts","Description":"Export FreeModel accounts. Default output is redacted."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/export/quota-snapshots","Description":"Export quota snapshots."}`,
		`{"Method":"GET","Path":"/freemodel-plugin/export/incidents","Description":"Export incidents."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/import/accounts","Description":"Import FreeModel accounts with dry-run support."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/import/snapshots","Description":"Import quota snapshots for local testing with dry-run support."}`,
		`{"Method":"POST","Path":"/freemodel-plugin/snapshots","Description":"Insert a quota snapshot for PoC/testing."}`,
	}, ",") + `],"resources":[{"Path":"/status","Menu":"FreeModel PoC","Description":"CPA FreeModel plugin PoC status page."}]}`
}

func handleManagement(raw []byte) ([]byte, error) {
	var req managementRequest
	if len(raw) > 0 {
		if errDecode := json.Unmarshal(raw, &req); errDecode != nil {
			return nil, fmt.Errorf("decode management request: %w", errDecode)
		}
	}

	switch req.Path {
	case "/v0/management/freemodel-plugin/health":
		return jsonResponse(http.StatusOK, runtimeState.health())
	case "/v0/management/freemodel-plugin/accounts":
		return handleAccounts(req)
	case "/v0/management/freemodel-plugin/accounts/proxy":
		return handleAccountProxy(req)
	case "/v0/management/freemodel-plugin/auth/send-otp":
		return handleSendOTP(req)
	case "/v0/management/freemodel-plugin/auth/verify-otp":
		return handleVerifyOTP(req)
	case "/v0/management/freemodel-plugin/sync":
		return handleSync(req)
	case "/v0/management/freemodel-plugin/sync-status":
		return handleSyncStatus(req)
	case "/v0/management/freemodel-plugin/quota":
		return handleQuota(req)
	case "/v0/management/freemodel-plugin/incidents":
		return handleIncidents(req)
	case "/v0/management/freemodel-plugin/usage-snapshots":
		return handleUsageSnapshots(req)
	case "/v0/management/freemodel-plugin/reconciliation-source":
		return handleReconciliationSource(req)
	case "/v0/management/freemodel-plugin/export/accounts":
		return handleExportAccounts(req)
	case "/v0/management/freemodel-plugin/export/quota-snapshots":
		return handleExportQuotaSnapshots(req)
	case "/v0/management/freemodel-plugin/export/incidents":
		return handleExportIncidents(req)
	case "/v0/management/freemodel-plugin/import/accounts":
		return handleImportAccounts(req)
	case "/v0/management/freemodel-plugin/import/snapshots":
		return handleImportSnapshots(req)
	case "/v0/management/freemodel-plugin/snapshots":
		return handleSnapshots(req)
	case "/v0/resource/plugins/cpa-freemodel/status":
		return htmlResponse(http.StatusOK, statusHTML())
	default:
		return jsonResponse(http.StatusNotFound, map[string]string{
			"error":   "not_found",
			"message": "unknown plugin route",
		})
	}
}

func handleAccounts(req managementRequest) ([]byte, error) {
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	ctx := context.Background()
	switch strings.ToUpper(req.Method) {
	case http.MethodGet:
		accounts, errList := db.ListAccounts(ctx)
		if errList != nil {
			return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
		}
		return jsonResponse(http.StatusOK, success(accountsPublic(accounts)))
	case http.MethodPost:
		var input store.AccountInput
		if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
			return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
		}
		account, errSave := db.SaveAccount(ctx, input)
		if errSave != nil {
			return jsonResponse(http.StatusBadRequest, failure(errSave.Error()))
		}
		return jsonResponse(http.StatusOK, success(account))
	case http.MethodDelete:
		email := firstQuery(req.Query, "email")
		if email == "" {
			return jsonResponse(http.StatusBadRequest, failure("email is required"))
		}
		if errDelete := db.DeleteAccount(ctx, email); errDelete != nil {
			return jsonResponse(http.StatusInternalServerError, failure(errDelete.Error()))
		}
		return jsonResponse(http.StatusOK, map[string]any{"success": true, "message": "account deleted"})
	default:
		return methodNotAllowed()
	}
}

func handleAccountProxy(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPatch {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var input struct {
		Email string `json:"email"`
		Proxy string `json:"proxy"`
	}
	if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	account, errUpdate := db.UpdateAccountProxy(context.Background(), input.Email, input.Proxy)
	if errUpdate != nil {
		status := http.StatusInternalServerError
		if errors.Is(errUpdate, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		return jsonResponse(status, failure(errUpdate.Error()))
	}
	return jsonResponse(http.StatusOK, success(account))
}

func handleQuota(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	snapshots, errList := db.ListLatestSnapshots(context.Background())
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	view := firstQuery(req.Query, "view")
	if view == "" {
		view = firstQuery(req.Query, "filter")
	}
	filtered := filterQuotaSnapshots(snapshots, view)
	return jsonResponse(http.StatusOK, map[string]any{
		"success": true,
		"view":    normalizedQuotaView(view),
		"summary": quotaSummary(snapshots),
		"data":    quotasPublic(filtered),
	})
}

func handleSnapshots(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var snap store.QuotaSnapshot
	if errDecode := json.Unmarshal(req.Body, &snap); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	stored, errSave := db.SaveSnapshot(context.Background(), snap)
	if errSave != nil {
		return jsonResponse(http.StatusBadRequest, failure(errSave.Error()))
	}
	return jsonResponse(http.StatusOK, success(stored))
}

func filterQuotaSnapshots(items []store.QuotaSnapshot, view string) []store.QuotaSnapshot {
	switch normalizedQuotaView(view) {
	case "available":
		filtered := make([]store.QuotaSnapshot, 0, len(items))
		for _, item := range items {
			if item.AvailabilityStatus == "available" && item.RealAvailableCents > 0 {
				filtered = append(filtered, item)
			}
		}
		return filtered
	case "other":
		filtered := make([]store.QuotaSnapshot, 0, len(items))
		for _, item := range items {
			if item.AvailabilityStatus != "available" || item.RealAvailableCents <= 0 {
				filtered = append(filtered, item)
			}
		}
		return filtered
	default:
		return items
	}
}

func normalizedQuotaView(view string) string {
	switch strings.ToLower(strings.TrimSpace(view)) {
	case "available", "可用", "balance":
		return "available"
	case "other", "others", "其他":
		return "other"
	default:
		return "all"
	}
}

func quotaSummary(items []store.QuotaSnapshot) map[string]any {
	var available int
	var other int
	var subscriptionExpired int
	var realAvailableCents int64
	var windowAvailableCents int64
	var extraAvailableCents int64
	for _, item := range items {
		if item.AvailabilityStatus == "available" && item.RealAvailableCents > 0 {
			available++
		} else {
			other++
		}
		if item.SubscriptionExpired {
			subscriptionExpired++
		}
		realAvailableCents += item.RealAvailableCents
		windowAvailableCents += item.WindowAvailableCents
		extraAvailableCents += item.ExtraAvailableCents
	}
	return map[string]any{
		"all":                    len(items),
		"available":              available,
		"other":                  other,
		"subscription_expired":   subscriptionExpired,
		"real_available_cents":   realAvailableCents,
		"window_available_cents": windowAvailableCents,
		"extra_available_cents":  extraAvailableCents,
	}
}

func methodNotAllowed() ([]byte, error) {
	return jsonResponse(http.StatusMethodNotAllowed, failure("method not allowed"))
}

func success(data any) map[string]any {
	return map[string]any{"success": true, "data": data}
}

func failure(message string) map[string]any {
	return map[string]any{"success": false, "message": message}
}

func firstQuery(values map[string][]string, key string) string {
	if len(values) == 0 {
		return ""
	}
	items := values[key]
	if len(items) == 0 {
		return ""
	}
	return strings.TrimSpace(items[0])
}

func parseSimpleYAML(raw []byte, cfg *pluginConfig) {
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		switch key {
		case "data_dir":
			cfg.DataDir = value
		case "db_path":
			cfg.DBPath = value
		case "model_api_base_url":
			cfg.ModelAPIBaseURL = value
		case "model_executor_enabled":
			cfg.ModelExecutor = strings.EqualFold(value, "true") || value == "1" || strings.EqualFold(value, "yes")
		}
	}
}

func jsonResponse(status int, value any) ([]byte, error) {
	body, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return okEnvelope(managementResponse{
		StatusCode: status,
		Headers: map[string][]string{
			"content-type": {"application/json; charset=utf-8"},
		},
		Body: body,
	})
}

func htmlResponse(status int, body string) ([]byte, error) {
	return okEnvelope(managementResponse{
		StatusCode: status,
		Headers: map[string][]string{
			"content-type": {"text/html; charset=utf-8"},
		},
		Body: []byte(body),
	})
}

func statusHTML() string {
	return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CPA FreeModel Operations</title>
  <style>
    :root { color-scheme: light dark; --bg:#f5f7fb; --panel:#fff; --text:#172033; --muted:#687083; --line:#e5e9f2; --blue:#2563eb; --green:#059669; --amber:#d97706; --red:#dc2626; --shadow:0 14px 40px rgba(23,32,51,.08); --input:#fff; --thead:#f8fafc; --code-bg:#eef2ff; --code-text:#1e40af; }
    @media (prefers-color-scheme: dark) { :root { --bg:#0b1020; --panel:#111827; --text:#e5e7eb; --muted:#9ca3af; --line:#253044; --blue:#60a5fa; --green:#34d399; --amber:#fbbf24; --red:#f87171; --shadow:0 14px 40px rgba(0,0,0,.35); --input:#0f172a; --thead:#0f172a; --code-bg:#172554; --code-text:#bfdbfe; } }
    :root[data-theme="dark"] { --bg:#0b1020; --panel:#111827; --text:#e5e7eb; --muted:#9ca3af; --line:#253044; --blue:#60a5fa; --green:#34d399; --amber:#fbbf24; --red:#f87171; --shadow:0 14px 40px rgba(0,0,0,.35); --input:#0f172a; --thead:#0f172a; --code-bg:#172554; --code-text:#bfdbfe; }
    :root[data-theme="light"] { --bg:#f5f7fb; --panel:#fff; --text:#172033; --muted:#687083; --line:#e5e9f2; --blue:#2563eb; --green:#059669; --amber:#d97706; --red:#dc2626; --shadow:0 14px 40px rgba(23,32,51,.08); --input:#fff; --thead:#f8fafc; --code-bg:#eef2ff; --code-text:#1e40af; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif; background:var(--bg); color:var(--text); }
    main { max-width:1280px; margin:0 auto; padding:32px 24px 56px; }
    header { display:flex; align-items:flex-start; justify-content:space-between; gap:24px; margin-bottom:24px; }
    h1 { margin:0 0 8px; font-size:30px; letter-spacing:-.03em; }
    h2 { margin:0 0 16px; font-size:18px; }
    p { margin:0; color:var(--muted); line-height:1.65; }
    code { background:var(--code-bg); border:1px solid var(--line); color:var(--code-text); border-radius:7px; padding:2px 6px; }
    button, input, textarea, select { font:inherit; }
    button { border:0; border-radius:10px; padding:10px 14px; background:var(--blue); color:#fff; cursor:pointer; font-weight:700; box-shadow:0 8px 18px rgba(37,99,235,.22); }
    button.secondary { background:var(--panel); color:var(--text); border:1px solid var(--line); box-shadow:none; }
    button.danger { background:#dc2626; }
    button:disabled { opacity:.55; cursor:not-allowed; }
    input, textarea, select { width:100%; border:1px solid var(--line); border-radius:10px; padding:10px 12px; background:var(--input); color:var(--text); }
    textarea { min-height:132px; font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace; font-size:12px; }
    label { display:block; color:var(--muted); font-size:12px; font-weight:800; margin:0 0 6px; }
    .top-actions { display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end; }
    .grid { display:grid; grid-template-columns:repeat(12,1fr); gap:16px; }
    .card { background:var(--panel); border:1px solid var(--line); border-radius:18px; padding:18px; box-shadow:var(--shadow); }
    .span-3 { grid-column:span 3; } .span-4 { grid-column:span 4; } .span-5 { grid-column:span 5; } .span-7 { grid-column:span 7; } .span-12 { grid-column:span 12; }
    .metric { display:flex; flex-direction:column; gap:8px; min-height:122px; }
    .metric .label { color:var(--muted); font-size:13px; font-weight:700; }
    .metric .value { font-size:30px; font-weight:900; letter-spacing:-.04em; }
    .metric .hint { color:var(--muted); font-size:12px; }
    .pill { display:inline-flex; align-items:center; gap:6px; border-radius:999px; padding:5px 9px; font-size:12px; font-weight:800; background:#eef2ff; color:#1e40af; }
    .pill.ok { background:#dcfce7; color:#166534; } .pill.warn { background:#fef3c7; color:#92400e; } .pill.err { background:#fee2e2; color:#991b1b; } .pill.gray { background:#f1f5f9; color:#475569; }
    .toolbar { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:12px; flex-wrap:wrap; }
    .tabs { display:flex; gap:8px; flex-wrap:wrap; }
    .tab { background:var(--panel); color:var(--text); border:1px solid var(--line); box-shadow:none; padding:8px 12px; }
    .tab.active { background:var(--text); color:var(--panel); border-color:var(--text); }
    table { width:100%; border-collapse:separate; border-spacing:0; overflow:hidden; }
    th, td { text-align:left; padding:12px 10px; border-bottom:1px solid var(--line); vertical-align:top; font-size:13px; }
    th { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; background:var(--thead); }
    tr:last-child td { border-bottom:0; }
    .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:14px; }
    .muted { color:var(--muted); }
    .money { font-variant-numeric:tabular-nums; font-weight:800; }
    .forms { display:grid; grid-template-columns:repeat(2,1fr); gap:14px; }
    .form-row { margin-bottom:12px; }
    .notice { border:1px solid var(--line); background:var(--thead); color:var(--text); border-radius:14px; padding:12px 14px; line-height:1.55; }
    .errorbox { display:none; border:1px solid #fecaca; background:#fef2f2; color:#991b1b; border-radius:14px; padding:12px 14px; margin-bottom:16px; white-space:pre-wrap; }
    :root[data-theme="dark"] .errorbox { background:#450a0a; color:#fecaca; border-color:#7f1d1d; }
    .authbar { display:flex; align-items:end; gap:10px; flex-wrap:wrap; margin-bottom:16px; }
    .authbar .field { width:260px; }
    .small { font-size:12px; }
    .right { text-align:right; }
    @media (max-width:960px) { .span-3,.span-4,.span-5,.span-7 { grid-column:span 12; } header { flex-direction:column; } .forms { grid-template-columns:1fr; } }
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <div class="pill">CPA Plugin</div>
      <h1>FreeModel 供应商运营面板</h1>
      <p>只展示脱敏运营数据。默认 <code>model_executor_enabled=false</code>，RELAYX 第一阶段不依赖模型 executor。</p>
    </div>
    <div class="top-actions">
      <button id="refreshBtn">刷新数据</button>
      <button id="syncBtn" class="secondary">触发同步</button>
      <a href="/management.html#/plugins"><button class="secondary">返回插件管理</button></a>
    </div>
  </header>

  <div class="card authbar">
    <div class="field"><label>管理密码</label><input id="managementKey" type="password" placeholder="本地 PoC 默认 cpa" autocomplete="current-password"></div>
    <button id="saveKeyBtn" class="secondary">保存并刷新</button>
    <button id="clearKeyBtn" class="secondary">清除</button>
    <span class="muted small">登录后台不等于插件页自动带管理 key；这里保存到当前浏览器 localStorage。</span>
  </div>

  <div id="errorBox" class="errorbox"></div>

  <section class="grid" style="margin-bottom:16px">
    <div class="card metric span-3"><div class="label">插件状态</div><div id="pluginStatus" class="value">--</div><div id="pluginHint" class="hint">等待加载</div></div>
    <div class="card metric span-3"><div class="label">账号总数</div><div id="accountCount" class="value">--</div><div id="accountHint" class="hint">FreeModel account pool</div></div>
    <div class="card metric span-3"><div class="label">真实可用</div><div id="realAvailable" class="value">--</div><div id="quotaHint" class="hint">窗口可用 + 额外用量可用</div></div>
    <div class="card metric span-3"><div class="label">其他 / 过期</div><div id="otherCount" class="value">--</div><div id="otherHint" class="hint">过期、待同步、无真实余额</div></div>
  </section>

  <section class="grid">
    <div class="card span-7">
      <div class="toolbar">
        <h2>额度监控</h2>
        <div class="tabs">
          <button class="tab active" data-view="all">全部</button>
          <button class="tab" data-view="available">真实可用</button>
          <button class="tab" data-view="other">其他</button>
        </div>
      </div>
      <div class="table-wrap"><table>
        <thead><tr><th>账号</th><th>状态</th><th class="right">真实可用</th><th class="right">窗口可用</th><th class="right">额外可用</th><th>5h / 7d</th><th>更新时间</th></tr></thead>
        <tbody id="quotaRows"><tr><td colspan="7" class="muted">加载中...</td></tr></tbody>
      </table></div>
    </div>

    <div class="card span-5">
      <h2>账号池</h2>
      <div class="table-wrap"><table>
        <thead><tr><th>ID</th><th>账号</th><th>API Key</th><th>代理</th></tr></thead>
        <tbody id="accountRows"><tr><td colspan="4" class="muted">加载中...</td></tr></tbody>
      </table></div>
    </div>

    <div class="card span-12">
      <div class="toolbar"><h2>运营事件</h2><span class="muted small">auth expired / proxy failed / quota / upstream errors</span></div>
      <div class="table-wrap"><table>
        <thead><tr><th>类型</th><th>级别</th><th>状态</th><th>账号</th><th>模型/路径</th><th>消息</th><th>时间</th></tr></thead>
        <tbody id="incidentRows"><tr><td colspan="7" class="muted">加载中...</td></tr></tbody>
      </table></div>
    </div>

    <div class="card span-5">
      <h2>添加 / 更新账号</h2>
      <div class="notice small">Cookie / API key 只会提交给 CPA 插件本地存储，列表和导出默认不会返回敏感值。</div>
      <form id="accountForm" style="margin-top:14px">
        <div class="form-row"><label>Email</label><input name="email" placeholder="name@example.com" autocomplete="off" required></div>
        <div class="form-row"><label>Dashboard Cookie（可选）</label><input name="cookie" placeholder="bm_session=..." autocomplete="off"></div>
        <div class="form-row"><label>Model API Key（可选，executor 实验用）</label><input name="api_key" placeholder="fe_oa_..." autocomplete="off"></div>
        <div class="form-row"><label>Proxy（可选）</label><input name="proxy" placeholder="direct 或 http://user:pass@host:port" autocomplete="off"></div>
        <button type="submit">保存账号</button>
      </form>
    </div>

    <div class="card span-7">
      <h2>导入 / 导出</h2>
      <div class="forms">
        <div>
          <p class="small">默认导出均为脱敏数据，不包含 cookie、session、token、API key、Authorization header。</p>
          <p style="margin-top:12px"><a href="/v0/management/freemodel-plugin/export/accounts" target="_blank">导出账号</a></p>
          <p><a href="/v0/management/freemodel-plugin/export/quota-snapshots" target="_blank">导出额度快照</a></p>
          <p><a href="/v0/management/freemodel-plugin/export/incidents" target="_blank">导出运营事件</a></p>
        </div>
        <div>
          <form id="importForm">
            <label>Import accounts JSON（建议 dry_run:true）</label>
            <textarea name="payload">{"dry_run":true,"validate_only":false,"accounts":[]}</textarea>
            <button type="submit" class="secondary" style="margin-top:10px">验证导入</button>
          </form>
        </div>
      </div>
    </div>

    <div class="card span-12">
      <h2>RELAYX 对接边界</h2>
      <p>RELAYX 第一阶段可读取 <code>health</code>、<code>accounts</code>、<code>quota</code>；外部运营 agent 可额外读取 <code>incidents</code>、<code>usage-snapshots</code>、<code>reconciliation-source</code>。RELAYX 不应持有 FreeModel cookie/session/API key，也不应直接读 CPA 插件数据库。</p>
    </div>
  </section>
</main>
<script>
const api = '/v0/management/freemodel-plugin';
const keyStorageName = 'cpa_freemodel_management_key';
let currentView = 'all';
(function initTheme(){
  const saved = localStorage.getItem('theme') || localStorage.getItem('cpa_theme') || localStorage.getItem('vite-ui-theme');
  if(saved === 'dark' || saved === 'light') document.documentElement.dataset.theme = saved;
})();
function managementHeaders(extra){
  const key = (localStorage.getItem(keyStorageName) || '').trim();
  const headers = Object.assign({}, extra || {});
  if(key) headers['X-Management-Key'] = key;
  return headers;
}
function money(cents){ return '$' + ((Number(cents || 0))/100).toFixed(2); }
function text(v){ return (v === undefined || v === null || v === '') ? '--' : String(v); }
function esc(v){ return text(v).replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c])); }
function pill(label, kind){ return '<span class="pill '+kind+'">'+esc(label)+'</span>'; }
function showError(err){ const el=document.getElementById('errorBox'); el.style.display='block'; const msg = err && err.message ? err.message : String(err); el.textContent = (msg.includes('missing management key') || msg.includes('invalid management key') || msg.includes('HTTP 401')) ? msg + '\n请在上方填写管理密码，本地 PoC 默认：cpa。' : msg; }
function clearError(){ const el=document.getElementById('errorBox'); el.style.display='none'; el.textContent=''; }
async function getJSON(path){
  const res = await fetch(api + path, { credentials:'same-origin', headers: managementHeaders() });
  const data = await res.json().catch(() => ({}));
  if(!res.ok || data.success === false || data.error){ throw new Error(data.message || data.error || ('HTTP '+res.status)); }
  return data;
}
async function postJSON(path, body){
  const res = await fetch(api + path, { method:'POST', credentials:'same-origin', headers: managementHeaders({'Content-Type':'application/json'}), body:JSON.stringify(body) });
  const data = await res.json().catch(() => ({}));
  if(!res.ok || data.success === false || data.error){ throw new Error(data.message || data.error || ('HTTP '+res.status)); }
  return data;
}
async function patchJSON(path, body){
  const res = await fetch(api + path, { method:'PATCH', credentials:'same-origin', headers: managementHeaders({'Content-Type':'application/json'}), body:JSON.stringify(body) });
  const data = await res.json().catch(() => ({}));
  if(!res.ok || data.success === false || data.error){ throw new Error(data.message || data.error || ('HTTP '+res.status)); }
  return data;
}
function renderHealth(h){
  document.getElementById('pluginStatus').innerHTML = h.store_ok ? '<span style="color:var(--green)">OK</span>' : '<span style="color:var(--red)">FAIL</span>';
  document.getElementById('pluginHint').textContent = h.version + ' · executor ' + (h.model_executor_enabled ? 'experimental enabled' : 'disabled') + ' · schema ' + h.schema_version;
}
function renderAccounts(items){
  document.getElementById('accountCount').textContent = items.length;
  document.getElementById('accountRows').innerHTML = items.length ? items.map(a => '<tr><td>'+a.id+'</td><td>'+esc(a.email)+'<br><span class="muted small">'+esc((a.email_hash||'').slice(0,12))+'</span></td><td>'+(a.model_api_configured?pill('已配置','ok'):pill('未配置','gray'))+'</td><td>'+esc(a.proxy||'direct')+'</td></tr>').join('') : '<tr><td colspan="4" class="muted">暂无账号</td></tr>';
}
function renderQuota(resp){
  const s = resp.summary || {};
  document.getElementById('realAvailable').textContent = money(s.real_available_cents);
  document.getElementById('quotaHint').textContent = '窗口 ' + money(s.window_available_cents) + ' · 额外 ' + money(s.extra_available_cents);
  document.getElementById('otherCount').textContent = text(s.other) + ' / ' + text(s.subscription_expired);
  const rows = resp.data || [];
  document.getElementById('quotaRows').innerHTML = rows.length ? rows.map(q => {
    const kind = q.availability_status === 'available' && q.real_available_cents > 0 ? 'ok' : (q.subscription_expired ? 'err' : 'warn');
    const used5 = (q.window_5h && q.window_5h.limit_cents) ? money(q.window_5h.used_cents)+' / '+money(q.window_5h.limit_cents) : '--';
    const usedW = (q.window_week && q.window_week.limit_cents) ? money(q.window_week.used_cents)+' / '+money(q.window_week.limit_cents) : '--';
    return '<tr><td>'+esc(q.email)+'<br><span class="muted small">#'+esc(q.account_id)+'</span></td><td>'+pill(q.availability_reason || q.availability_status, kind)+'<br><span class="muted small">'+esc(q.plan_status)+'</span></td><td class="right money">'+money(q.real_available_cents)+'</td><td class="right money">'+money(q.window_available_cents)+'</td><td class="right money">'+money(q.extra_available_cents)+'</td><td><span class="small">5h '+used5+'<br>7d '+usedW+'</span></td><td class="small">'+esc(q.fetched_at)+'</td></tr>';
  }).join('') : '<tr><td colspan="7" class="muted">暂无额度数据</td></tr>';
}
function renderIncidents(items){
  document.getElementById('incidentRows').innerHTML = items.length ? items.map(i => '<tr><td>'+esc(i.kind)+'</td><td>'+pill(i.severity, i.severity==='critical'?'err':(i.severity==='warning'?'warn':'gray'))+'</td><td>'+esc(i.status)+'</td><td>'+esc(i.account_email||'')+'</td><td>'+esc(i.model||'')+'<br><span class="muted small">'+esc(i.path||'')+'</span></td><td>'+esc(i.message)+'</td><td class="small">'+esc(i.detected_at||i.first_seen_at||'')+'</td></tr>').join('') : '<tr><td colspan="7" class="muted">暂无事件</td></tr>';
}
async function load(){
  clearError();
  try {
    const [health, accounts, quota, incidents] = await Promise.all([
      getJSON('/health'), getJSON('/accounts'), getJSON('/quota?view='+encodeURIComponent(currentView)), getJSON('/incidents')
    ]);
    renderHealth(health);
    renderAccounts(accounts.data || []);
    renderQuota(quota);
    renderIncidents(incidents.data || []);
  } catch(err) { showError(err); }
}
const keyInput = document.getElementById('managementKey');
keyInput.value = localStorage.getItem(keyStorageName) || 'cpa';
document.getElementById('saveKeyBtn').addEventListener('click', () => { localStorage.setItem(keyStorageName, keyInput.value.trim()); load(); });
document.getElementById('clearKeyBtn').addEventListener('click', () => { localStorage.removeItem(keyStorageName); keyInput.value = ''; load(); });
if(!localStorage.getItem(keyStorageName) && keyInput.value) localStorage.setItem(keyStorageName, keyInput.value);
document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => { document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active')); btn.classList.add('active'); currentView = btn.dataset.view; load(); }));
document.getElementById('refreshBtn').addEventListener('click', load);
document.getElementById('syncBtn').addEventListener('click', async () => { clearError(); try { await postJSON('/sync', {}); await load(); } catch(err) { showError(err); } });
document.getElementById('accountForm').addEventListener('submit', async ev => {
  ev.preventDefault(); clearError();
  const fd = new FormData(ev.currentTarget); const body = Object.fromEntries(fd.entries());
  try { await postJSON('/accounts', body); ev.currentTarget.reset(); await load(); } catch(err) { showError(err); }
});
document.getElementById('importForm').addEventListener('submit', async ev => {
  ev.preventDefault(); clearError();
  try { const body = JSON.parse(new FormData(ev.currentTarget).get('payload')); const out = await postJSON('/import/accounts', body); alert(JSON.stringify(out.summary || out, null, 2)); await load(); } catch(err) { showError(err); }
});
load();
</script>
</body>
</html>`
}

func okEnvelope(value any) ([]byte, error) {
	result, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: result})
}

func okEnvelopeJSON(result string) ([]byte, error) {
	return json.Marshal(envelope{OK: true, Result: json.RawMessage(result)})
}

// ErrorEnvelope returns a plugin ABI error envelope.
func ErrorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

// ErrorEnvelopeHTTP returns a plugin ABI error envelope with a downstream HTTP status.
func ErrorEnvelopeHTTP(code, message string, status int) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message, HTTPStatus: status}})
	return raw
}
