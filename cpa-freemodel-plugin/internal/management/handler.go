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
  <title>CPA FreeModel Plugin PoC</title>
  <style>
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7fb; color: #182033; }
    main { max-width: 900px; margin: 48px auto; padding: 0 24px; }
    .card { background: #fff; border: 1px solid #e5e7ef; border-radius: 18px; padding: 28px; box-shadow: 0 12px 32px rgba(24, 32, 51, .08); }
    h1 { margin: 0 0 12px; font-size: 28px; }
    p { line-height: 1.7; }
    code { background: #eef2ff; border-radius: 6px; padding: 2px 6px; }
    .ok { display: inline-block; color: #057a55; background: #def7ec; border-radius: 999px; padding: 6px 12px; font-weight: 700; }
    ul { line-height: 1.9; }
  </style>
</head>
<body>
  <main>
    <section class="card">
      <div class="ok">PoC loaded</div>
      <h1>CPA FreeModel Plugin</h1>
      <p>这是独立插件方向的可持久化 PoC，已验证 Management API、资源页面、菜单注册和插件自管 SQLite。</p>
      <ul>
        <li>插件 ID：<code>cpa-freemodel</code></li>
        <li>健康检查：<code>GET /v0/management/freemodel-plugin/health</code></li>
        <li>账号接口：<code>GET/POST/DELETE /v0/management/freemodel-plugin/accounts</code></li>
        <li>全部额度：<code>GET /v0/management/freemodel-plugin/quota?view=all</code></li>
        <li>真实可用：<code>GET /v0/management/freemodel-plugin/quota?view=available</code></li>
        <li>其他账号：<code>GET /v0/management/freemodel-plugin/quota?view=other</code>，例如订阅过期、待同步、无真实可用余额。</li>
        <li>测试快照：<code>POST /v0/management/freemodel-plugin/snapshots</code></li>
      </ul>
      <p>真实可用余额按 <code>窗口真实可用额度 + 额外可用额度</code> 计算；如果订阅已过期，则真实可用强制为 0 并归入“其他”。</p>
      <p>额度响应已同步新增字段：<code>topup_cents</code>、<code>referral_credits</code>、<code>referral_used</code>、<code>extra_available_cents</code>、<code>window_available_cents</code>、<code>real_available_cents</code>、<code>subscription_expired</code>。</p>
      <p>下一步可以迁移真实 FreeModel API 同步、OTP 登录、代理订阅、gost 启停和账号代理绑定逻辑。</p>
    </section>
  </main>
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
