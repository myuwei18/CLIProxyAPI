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
	case "real":
		filtered := make([]store.QuotaSnapshot, 0, len(items))
		for _, item := range items {
			if !item.IsTestAccount {
				filtered = append(filtered, item)
			}
		}
		return filtered
	case "test":
		filtered := make([]store.QuotaSnapshot, 0, len(items))
		for _, item := range items {
			if item.IsTestAccount {
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
	case "real", "real_accounts", "真实账号":
		return "real"
	case "test", "tests", "sample", "demo", "测试", "样例":
		return "test"
	case "unavailable", "other", "others", "其他", "不可用":
		return "other"
	default:
		return "all"
	}
}

func quotaSummary(items []store.QuotaSnapshot) map[string]any {
	var available int
	var other int
	var subscriptionExpired int
	var realAccounts int
	var testAccounts int
	var missingCookie int
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
		if item.IsTestAccount {
			testAccounts++
		} else {
			realAccounts++
		}
		if !item.HasDashboardCookie {
			missingCookie++
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
		"real_accounts":          realAccounts,
		"test_accounts":          testAccounts,
		"missing_cookie":         missingCookie,
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
:root{color-scheme:light dark;--bg:#f5f5f5;--panel:#fff;--panel2:#fafafa;--text:#1f2937;--muted:#6b7280;--line:#e5e7eb;--blue:#1677ff;--green:#16a34a;--amber:#d97706;--red:#dc2626;--track:#e5e7eb;--shadow:none}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--panel:#171a21;--panel2:#111827;--text:#f3f4f6;--muted:#9ca3af;--line:#2d3340;--blue:#60a5fa;--green:#34d399;--amber:#fbbf24;--red:#f87171;--track:#293241}}
:root[data-theme=dark]{--bg:#0f1115;--panel:#171a21;--panel2:#111827;--text:#f3f4f6;--muted:#9ca3af;--line:#2d3340;--blue:#60a5fa;--green:#34d399;--amber:#fbbf24;--red:#f87171;--track:#293241}
:root[data-theme=white],:root[data-theme=light]{--bg:#f5f5f5;--panel:#fff;--panel2:#fafafa;--text:#1f2937;--muted:#6b7280;--line:#e5e7eb;--blue:#1677ff;--green:#16a34a;--amber:#d97706;--red:#dc2626;--track:#e5e7eb}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}main{max-width:1320px;margin:0 auto;padding:24px 18px 32px}.page-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px}.page-kicker{font-size:12px;color:var(--muted);font-weight:600;margin-bottom:4px}h1{margin:0 0 4px;font-size:20px;font-weight:700;letter-spacing:-.01em}h2{margin:0;font-size:15px;font-weight:700}p{margin:0;color:var(--muted);line-height:1.5}.sub{font-size:12px}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:10px}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px;box-shadow:var(--shadow)}.span-2{grid-column:span 2}.span-3{grid-column:span 3}.span-4{grid-column:span 4}.span-8{grid-column:span 8}.span-12{grid-column:span 12}.tabs,.authbar,.toolbar,.actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.toolbar{justify-content:space-between;margin-bottom:10px}.toolbar-left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}button,input,textarea{font:inherit}button{border:1px solid var(--blue);border-radius:6px;padding:6px 10px;background:var(--blue);color:#fff;cursor:pointer;font-weight:600;font-size:13px;line-height:1.2}button:disabled{opacity:.58;cursor:not-allowed}button.secondary{background:var(--panel);color:var(--text);border:1px solid var(--line)}a.inline-link{color:var(--blue);text-decoration:none;font-size:12px}a.inline-link:hover{text-decoration:underline}button.tab,button.auth-tab{background:var(--panel);color:var(--muted);border:1px solid var(--line);box-shadow:none;font-weight:600}button.tab.active,button.auth-tab.active{background:var(--panel2);color:var(--text);border-color:var(--blue)}input,textarea{width:100%;border:1px solid var(--line);border-radius:6px;background:var(--panel2);color:var(--text);padding:7px 9px}label{display:block;color:var(--muted);font-size:12px;font-weight:600;margin:0 0 4px}.metric{min-height:78px}.metric .label{color:var(--muted);font-size:12px}.metric .value{font-size:22px;font-weight:750;margin-top:5px}.metric .hint{color:var(--muted);font-size:12px;margin-top:4px}.muted{color:var(--muted)}.small{font-size:12px}.status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--muted);margin-right:6px}.status-dot.ok{background:var(--green)}.status-dot.warn{background:var(--amber)}.status-dot.err{background:var(--red)}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}table{width:100%;border-collapse:separate;border-spacing:0}th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top;font-size:12px}th{color:var(--muted);font-size:11px;background:var(--panel2);font-weight:700}tr:last-child td{border-bottom:0}.quota-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:10px}.quota-card{border:1px solid var(--line);border-radius:8px;background:var(--panel);padding:12px}.quota-card.unavailable{opacity:.68;background:var(--panel2)}.quota-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:8px}.quota-email{font-weight:700;font-size:14px}.quota-meta{color:var(--muted);font-size:12px;margin-top:3px}.quota-status{font-size:12px;color:var(--muted);white-space:nowrap}.quota-status.ok{color:var(--green)}.quota-status.warn{color:var(--amber)}.quota-status.err{color:var(--red)}.quota-times{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px}.quota-time-right{text-align:right}.quota-main{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px;border:1px solid var(--line);border-radius:8px;background:var(--panel2);padding:10px}.quota-module{min-width:0}.quota-module+.quota-module{border-left:1px solid var(--line);padding-left:10px}.quota-module-head{display:block;margin-bottom:6px}.quota-module-title{font-size:12px;font-weight:650;color:var(--text);line-height:1.3}.quota-module-amount{font-size:12px;color:var(--muted);white-space:nowrap;margin-top:2px}.quota-module-reset{font-size:11px;color:var(--muted);margin-top:6px}.bar-row{display:grid;grid-template-columns:1fr 42px;align-items:center;gap:8px;margin-top:4px}.bar-title{font-weight:650;color:var(--muted);font-size:12px}.bar{height:7px;background:var(--track);border-radius:999px;overflow:hidden}.fill{height:100%;border-radius:999px}.fill.ok{background:var(--green)}.fill.warn{background:var(--amber)}.fill.err{background:var(--red)}.bar-meta{text-align:right;font-size:12px;color:var(--muted)}.errorbox{display:none;border:1px solid rgba(220,38,38,.35);background:rgba(220,38,38,.08);color:var(--red);border-radius:8px;padding:9px 11px;margin-bottom:10px;white-space:pre-wrap}.statusbox{display:none;border:1px solid var(--line);background:var(--panel2);color:var(--text);border-radius:8px;padding:8px 10px;margin:8px 0;font-size:12px}.statusbox.inline{margin:10px 0 0}.statusbox.ok{border-color:rgba(22,163,74,.35);color:var(--green)}.statusbox.err{border-color:rgba(220,38,38,.35);color:var(--red)}.notice{border:1px solid var(--line);background:var(--panel2);border-radius:8px;padding:9px;line-height:1.45}.form-row{margin-bottom:8px}.form-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.forms{display:grid;grid-template-columns:1fr 1fr;gap:10px}.auth-panel{display:none;margin-top:10px}.auth-panel.active{display:block}textarea{min-height:96px;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:11px}code{background:var(--panel2);border:1px solid var(--line);border-radius:6px;padding:1px 5px}.authbar{margin-bottom:10px}.authbar .field{width:190px}@media(max-width:980px){main{padding-top:18px}.page-head{display:block}.span-2,.span-3,.span-4,.span-8{grid-column:span 12}.forms{grid-template-columns:1fr}.quota-cards{grid-template-columns:1fr}.quota-main,.quota-times{grid-template-columns:1fr}.quota-module+.quota-module{border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:10px}.quota-time-right{text-align:left}}
</style>
</head>
<body>
<main>
<div class="page-head"><div><div class="page-kicker">CPA Plugin</div><h1>FreeModel 供应商运营面板</h1><p class="sub">脱敏运营视图 · executor 默认关闭 · 跟随管理后台/系统主题</p></div><a id="backLink" class="inline-link" href="/management.html#/plugins">返回插件管理</a></div>
<div id="errorBox" class="errorbox"></div><div id="statusBox" class="statusbox"></div>
<section class="grid" style="margin-bottom:10px"><div class="card metric span-2"><div class="label">插件状态</div><div id="pluginStatus" class="value">--</div><div id="pluginHint" class="hint">等待加载</div></div><div class="card metric span-2"><div class="label">账号总数</div><div id="accountCount" class="value">--</div><div class="hint">账号池</div></div><div class="card metric span-3"><div class="label">真实可用</div><div id="realAvailable" class="value">--</div><div id="quotaHint" class="hint">窗口 + 额外</div></div><div class="card metric span-2"><div class="label">不可用</div><div id="otherCount" class="value">--</div><div class="hint">需处理</div></div><div class="card metric span-3"><div class="label">更新时间</div><div id="updatedAt" class="value" style="font-size:18px">--</div><div class="hint">点击刷新后更新</div></div></section>
<section class="grid"><div class="card span-8"><div class="toolbar"><div class="toolbar-left"><h2>额度监控</h2><div class="tabs"><button class="tab active" data-view="available">可用(0)</button><button class="tab" data-view="real">全部(0)</button><button class="tab" data-view="other">不可用(0)</button></div></div><div class="actions"><button id="refreshBtn" class="secondary">刷新额度</button><button id="syncBtn" class="secondary">同步 CK 额度</button></div></div><div class="small muted" style="margin-bottom:8px">数据来自本地插件 DB 的最新 dashboard/quota 快照；卡片中的“同步”时间表示最后一次拉取时间。未同步或同步失败会在卡片中简洁灰态显示。若要刷新到最新状态，请使用验证码登录或 CK 登录后同步。</div><div id="quotaCards" class="quota-cards"><div class="muted small">加载中...</div></div></div><div class="card span-4"><div class="toolbar"><h2>添加 / 更新账号</h2><span class="muted small">默认管理 key：cpa</span></div><div class="tabs"><button id="otpTabBtn" type="button" class="auth-tab active" data-panel="otpPanel">验证码登录</button><button id="ckTabBtn" type="button" class="auth-tab" data-panel="ckPanel">CK 登录</button></div><div id="authStatus" class="statusbox inline"></div><div id="otpPanel" class="auth-panel active"><div class="notice small">先输入邮箱发送验证码，收到后填验证码保存账号；验证码和 CK 不会展示。</div><form id="otpForm" style="margin-top:10px"><div class="form-row"><label>Email</label><input name="email" placeholder="name@example.com" autocomplete="off" required></div><div class="form-row"><label>验证码</label><input name="code" placeholder="收到后填写" autocomplete="one-time-code"></div><div class="form-row"><label>Proxy（可选）</label><input name="proxy" placeholder="direct 或 http://user:pass@host:port" autocomplete="off"></div><div class="form-actions"><button id="sendOtpBtn" type="button" class="secondary">发送验证码</button><button id="verifyOtpBtn" type="submit">验证并保存</button></div></form></div><div id="ckPanel" class="auth-panel"><div class="notice small">手动保存 Dashboard Cookie / Model API Key；列表和导出默认脱敏。</div><form id="accountForm" style="margin-top:10px"><div class="form-row"><label>Email</label><input name="email" placeholder="name@example.com" autocomplete="off" required></div><div class="form-row"><label>Dashboard Cookie</label><input name="cookie" placeholder="bm_session=..." autocomplete="off"></div><div class="form-row"><label>Model API Key</label><input name="api_key" placeholder="fe_oa_..." autocomplete="off"></div><div class="form-row"><label>Proxy（可选）</label><input name="proxy" placeholder="direct 或 http://user:pass@host:port" autocomplete="off"></div><div class="form-actions"><button id="saveAccountBtn" type="submit">保存账号</button></div></form></div></div><div class="card span-4"><h2 style="margin-bottom:8px">账号池</h2><div class="table-wrap"><table><thead><tr><th>ID</th><th>账号</th><th>Key</th><th>代理</th></tr></thead><tbody id="accountRows"><tr><td colspan="4" class="muted">加载中...</td></tr></tbody></table></div></div><div class="card span-8"><div class="toolbar"><h2>运营事件</h2><span class="muted small">认证、代理、额度、上游错误</span></div><div class="table-wrap"><table><thead><tr><th>类型</th><th>级别</th><th>状态</th><th>账号</th><th>模型/路径</th><th>消息</th><th>时间</th></tr></thead><tbody id="incidentRows"><tr><td colspan="7" class="muted">加载中...</td></tr></tbody></table></div></div><div class="card span-12"><h2 style="margin-bottom:8px">导入 / 导出</h2><div class="forms"><div><p class="small">默认导出脱敏数据，不包含 cookie、session、token、API key、Authorization header。</p><p style="margin-top:8px"><a href="/v0/management/freemodel-plugin/export/accounts" target="_blank">导出账号</a></p><p><a href="/v0/management/freemodel-plugin/export/quota-snapshots" target="_blank">导出额度快照</a></p><p><a href="/v0/management/freemodel-plugin/export/incidents" target="_blank">导出运营事件</a></p></div><form id="importForm"><label>Import accounts JSON（建议 dry_run:true）</label><textarea name="payload">{"dry_run":true,"validate_only":false,"accounts":[]}</textarea><button type="submit" class="secondary" style="margin-top:8px">验证导入</button></form></div></div><div class="card span-12"><h2 style="margin-bottom:6px">RELAYX 对接边界</h2><p class="small">RELAYX 第一阶段读取 <code>health</code>、<code>accounts</code>、<code>quota</code>；运营 agent 可额外读取 incidents / snapshots / reconciliation。RELAYX 不持有 FreeModel cookie/session/API key，不直接读插件数据库。</p></div></section>
</main>
<script>
const api='/v0/management/freemodel-plugin';let currentView='available';let latestQuota=[];let latestSummary={};
function readPersisted(name){try{const raw=localStorage.getItem(name);if(!raw)return null;const obj=JSON.parse(raw);return obj&&obj.state?obj.state:obj}catch{return null}}
function applyTheme(){const st=readPersisted('cli-proxy-theme');let theme=(st&&st.theme)||localStorage.getItem('theme')||'auto';let resolved=theme==='dark'?'dark':theme==='white'?'white':theme==='light'?'light':(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'white');document.documentElement.dataset.theme=resolved}
applyTheme();setInterval(applyTheme,1000);try{matchMedia('(prefers-color-scheme: dark)').addEventListener('change',applyTheme)}catch{}
function managementHeaders(extra){const h=Object.assign({},extra||{});h['X-Management-Key']='cpa';return h}
function money(c){return '$'+(Number(c||0)/100).toFixed(2)}function text(v){return(v===undefined||v===null||v==='')?'--':String(v)}function esc(v){return text(v).replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]))}function fmtTime(v){if(!v)return'--';const d=new Date(v);if(Number.isNaN(d.getTime()))return esc(v);const p=n=>String(n).padStart(2,'0');return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+'-'+p(d.getMinutes())+'-'+p(d.getSeconds())}function zh(v){const m={active:'有效',expired:'已过期',canceled:'已取消',cancelled:'已取消',inactive:'未激活',available:'可用',other:'不可用',real_balance_available:'可用',subscription_expired:'已过期',pending_sync:'待同步',no_real_balance:'不可用',quota_exhausted:'不可用',window_available:'可用',cookie_expired:'不可用',sync_failed:'同步失败',stale_snapshot:'待同步',test_account:'不可用',missing_cookie:'不可用',ready:'已同步',syncing:'同步中',open:'未处理',resolved:'已解决',warning:'警告',critical:'严重',info:'信息'};return m[String(v||'').toLowerCase()]||text(v)}
function showError(e){const el=document.getElementById('errorBox');el.style.display='block';const msg=e&&e.message?e.message:String(e);el.textContent=msg}function clearError(){const el=document.getElementById('errorBox');el.style.display='none';el.textContent=''}function showStatus(msg,ok){const el=document.getElementById('statusBox');el.className='statusbox '+(ok?'ok':'err');el.style.display='block';el.textContent=msg}function clearStatus(){const el=document.getElementById('statusBox');el.style.display='none';el.textContent=''}function showAuthStatus(msg,ok){const el=document.getElementById('authStatus');if(!el)return;el.className='statusbox inline '+(ok?'ok':'err');el.style.display='block';el.textContent=msg}function clearAuthStatus(){const el=document.getElementById('authStatus');if(!el)return;el.style.display='none';el.textContent=''}async function withBusy(btn,label,fn){const old=btn.textContent;btn.disabled=true;btn.textContent=label;try{return await fn()}finally{btn.disabled=false;btn.textContent=old}}async function getJSON(path){const r=await fetch(api+path,{credentials:'same-origin',headers:managementHeaders()});const d=await r.json().catch(()=>({}));if(!r.ok||d.success===false||d.error)throw new Error(d.message||d.error||('HTTP '+r.status));return d}async function postJSON(path,body){const r=await fetch(api+path,{method:'POST',credentials:'same-origin',headers:managementHeaders({'Content-Type':'application/json'}),body:JSON.stringify(body)});const d=await r.json().catch(()=>({}));if(!r.ok||d.success===false||d.error)throw new Error(d.message||d.error||('HTTP '+r.status));return d}
function renderHealth(h){document.getElementById('pluginStatus').innerHTML=h.store_ok?'<span style="color:var(--green)">OK</span>':'<span style="color:var(--red)">FAIL</span>';document.getElementById('pluginHint').textContent='executor '+(h.model_executor_enabled?'实验开启':'关闭')+' · '+h.schema_version}
function renderAccounts(items){document.getElementById('accountCount').textContent=items.length;document.getElementById('accountRows').innerHTML=items.length?items.map(a=>'<tr><td>'+a.id+'</td><td>'+esc(a.email)+'<br><span class="muted small">'+esc((a.email_hash||'').slice(0,8))+'</span></td><td>'+(a.model_api_configured?'有':'无')+'</td><td>'+esc(a.proxy||'直连')+'</td></tr>').join(''):'<tr><td colspan="4" class="muted">暂无账号</td></tr>'}
function updateTabs(summary){document.querySelector('[data-view=available]').textContent='可用('+(summary.available||0)+')';document.querySelector('[data-view=real]').textContent='全部('+(summary.real_accounts||0)+')';document.querySelector('[data-view=other]').textContent='不可用('+(summary.other||0)+')'}
function fmtUnix(v){const n=Number(v||0);return n>0?fmtTime(new Date(n*1000).toISOString()):'--'}function quotaModule(title,free,total,resetAt){const t=Number(total||0);const f=Number(free||0);const percent=t>0?Math.max(0,Math.min(100,Math.round(f*100/t))):(f>0?100:0);const cls=percent>=50?'ok':(percent>=20?'warn':'err');const reset=title==='额外'?'--':fmtUnix(resetAt);return '<div class="quota-module"><div class="quota-module-head"><div class="quota-module-title">'+title+'</div><div class="quota-module-amount">'+money(f)+' / '+money(t)+'</div></div><div class="bar-row"><div class="bar"><div class="fill '+cls+'" style="width:'+percent+'%"></div></div><div class="bar-meta">'+percent+'%</div></div><div class="quota-module-reset">刷新 '+reset+'</div></div>'}
function quotaState(q){if(q.subscription_expired)return{label:'已过期',cls:'err'};if(q.availability_status==='available'&&q.real_available_cents>0)return{label:'可用',cls:'ok'};if(q.sync_status==='pending')return{label:'待同步',cls:'warn'};return{label:'不可用',cls:'warn'}}
function renderQuota(resp){latestSummary=resp.summary||{};latestQuota=resp.data||[];updateTabs(latestSummary);document.getElementById('realAvailable').textContent=money(latestSummary.real_available_cents);document.getElementById('quotaHint').textContent='窗口 '+money(latestSummary.window_available_cents)+' · 额外 '+money(latestSummary.extra_available_cents);document.getElementById('otherCount').textContent=text(latestSummary.other);document.getElementById('updatedAt').textContent=fmtTime(new Date().toISOString());const rows=latestQuota.filter(q=>!q.is_test_account);document.getElementById('quotaCards').innerHTML=rows.length?rows.map(q=>{const extra=q.extra_available_cents||0;const st=quotaState(q);const unavailable=st.cls==='ok'?'':' unavailable';return '<div class="quota-card'+unavailable+'"><div class="quota-head"><div style="min-width:0;flex:1"><div class="quota-email">#'+esc(q.account_id)+' '+esc(q.email)+'</div><div class="quota-times"><div class="quota-meta">有效至 '+fmtTime(q.current_period_end)+'</div><div class="quota-meta quota-time-right">同步 '+fmtTime(q.fetched_at)+'</div></div></div><div class="quota-status '+st.cls+'"><span class="status-dot '+st.cls+'"></span>'+st.label+' · '+money(q.real_available_cents)+'</div></div><div class="quota-main">'+quotaModule('5h',q.window_5h_remaining,q.window_5h&&q.window_5h.limit_cents,q.window_5h&&q.window_5h.resets_at)+quotaModule('7d',q.window_week_remaining,q.window_week&&q.window_week.limit_cents,q.window_week&&q.window_week.resets_at)+quotaModule('额外',extra,extra,0)+'</div></div>'}).join(''):'<div class="muted small">暂无额度数据</div>'}
function renderIncidents(items){document.getElementById('incidentRows').innerHTML=items.length?items.map(i=>'<tr><td>'+zh(i.kind)+'</td><td>'+zh(i.severity)+'</td><td>'+zh(i.status)+'</td><td>'+esc(i.account_email||'')+'</td><td>'+esc(i.model||'')+'<br><span class="muted small">'+esc(i.path||'')+'</span></td><td>'+esc(i.message)+'</td><td class="small">'+fmtTime(i.detected_at||i.first_seen_at||'')+'</td></tr>').join(''):'<tr><td colspan="7" class="muted">暂无事件</td></tr>'}
async function load(){clearError();try{const [h,a,q,i]=await Promise.all([getJSON('/health'),getJSON('/accounts'),getJSON('/quota?view='+encodeURIComponent(currentView)),getJSON('/incidents')]);renderHealth(h);renderAccounts(a.data||[]);renderQuota(q);renderIncidents(i.data||[])}catch(e){showError(e)}}
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));b.classList.add('active');currentView=b.dataset.view;load()});document.getElementById('backLink').onclick=e=>{e.preventDefault();try{window.top.location.href='/management.html#/plugins'}catch{window.location.href='/management.html#/plugins'}};document.querySelectorAll('.auth-tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.auth-tab').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.auth-panel').forEach(x=>x.classList.remove('active'));b.classList.add('active');document.getElementById(b.dataset.panel).classList.add('active');clearAuthStatus();clearError()});document.getElementById('refreshBtn').onclick=async ev=>{clearStatus();await withBusy(ev.currentTarget,'刷新中...',async()=>{await load();showStatus('额度已刷新。',true)})};document.getElementById('syncBtn').onclick=async ev=>{clearError();clearStatus();await withBusy(ev.currentTarget,'同步中...',async()=>{try{showStatus('正在同步 CK 额度...',true);await postJSON('/sync',{});await load();showStatus('同步完成，额度已更新。',true)}catch(e){showError(e);showStatus('同步失败，请查看错误信息。',false)}})};document.getElementById('sendOtpBtn').onclick=async ev=>{clearError();clearAuthStatus();const fd=new FormData(document.getElementById('otpForm'));const email=String(fd.get('email')||'').trim();if(!email){showAuthStatus('请先填写 Email。',false);return}await withBusy(ev.currentTarget,'发送中...',async()=>{try{await postJSON('/auth/send-otp',{email,proxy:fd.get('proxy')});showAuthStatus('验证码已发送，请查收邮箱。',true)}catch(e){showError(e);showAuthStatus('验证码发送失败：'+(e.message||e),false)}})};document.getElementById('otpForm').onsubmit=async ev=>{ev.preventDefault();clearError();clearAuthStatus();const fd=new FormData(ev.currentTarget);const email=String(fd.get('email')||'').trim();const code=String(fd.get('code')||'').trim();if(!email||!code){showAuthStatus('请填写 Email 和验证码。',false);return}await withBusy(document.getElementById('verifyOtpBtn'),'保存中...',async()=>{try{await postJSON('/auth/verify-otp',{email,code,proxy:fd.get('proxy')});ev.currentTarget.querySelector('[name=code]').value='';await load();showAuthStatus('保存成功，账号和额度已刷新。',true)}catch(e){showError(e);showAuthStatus('验证保存失败：'+(e.message||e),false)}})};document.getElementById('accountForm').onsubmit=async ev=>{ev.preventDefault();clearError();clearAuthStatus();const payload=Object.fromEntries(new FormData(ev.currentTarget).entries());payload.email=String(payload.email||'').trim();if(!payload.email){showAuthStatus('请填写 Email。',false);return}await withBusy(document.getElementById('saveAccountBtn'),'保存中...',async()=>{try{await postJSON('/accounts',payload);ev.currentTarget.reset();await load();showAuthStatus('保存成功，账号和额度已刷新。',true)}catch(e){showError(e);showAuthStatus('保存失败：'+(e.message||e),false)}})};document.getElementById('importForm').onsubmit=async ev=>{ev.preventDefault();clearError();try{const out=await postJSON('/import/accounts',JSON.parse(new FormData(ev.currentTarget).get('payload')));showStatus('导入校验完成：'+JSON.stringify(out.summary||out),true);await load()}catch(e){showError(e);showStatus('导入校验失败。',false)}};load();
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
