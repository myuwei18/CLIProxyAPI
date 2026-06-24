package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	providerID           = "cpa-freemodel"
	defaultModelAPIBase  = "https://api.freemodel.dev/v1"
	formatChatCompletion = "chat-completions"
	formatResponses      = "responses"
)

type executorRequest struct {
	Model           string              `json:"Model"`
	Format          string              `json:"Format"`
	Stream          bool                `json:"Stream"`
	Headers         map[string][]string `json:"Headers"`
	Query           map[string][]string `json:"Query"`
	OriginalRequest []byte              `json:"OriginalRequest"`
	SourceFormat    string              `json:"SourceFormat"`
	Payload         []byte              `json:"Payload"`
}

type executorResponse struct {
	Payload []byte              `json:"Payload"`
	Headers map[string][]string `json:"Headers,omitempty"`
}

type executorStreamResponse struct {
	Headers map[string][]string   `json:"headers,omitempty"`
	Chunks  []executorStreamChunk `json:"chunks,omitempty"`
}

type executorStreamChunk struct {
	Payload []byte `json:"Payload,omitempty"`
}

type hostHTTPRequest struct {
	HostCallbackID string              `json:"host_callback_id,omitempty"`
	Method         string              `json:"method,omitempty"`
	URL            string              `json:"url,omitempty"`
	Headers        map[string][]string `json:"headers,omitempty"`
	Body           []byte              `json:"body,omitempty"`
}

type hostHTTPResponse struct {
	StatusCode int                 `json:"StatusCode"`
	Headers    map[string][]string `json:"Headers,omitempty"`
	Body       []byte              `json:"Body,omitempty"`
}

type hostHTTPStreamResponse struct {
	StatusCode int                 `json:"status_code"`
	Headers    map[string][]string `json:"headers,omitempty"`
	StreamID   string              `json:"stream_id,omitempty"`
}

type hostHTTPStreamReadRequest struct {
	StreamID string `json:"stream_id"`
}

type hostHTTPStreamReadResponse struct {
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
	Done    bool   `json:"done,omitempty"`
}

type hostHTTPStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
}

func staticModelsJSON() string {
	now := time.Now().Unix()
	models := []map[string]any{
		modelInfo("gpt-5.5", "GPT-5.5", now),
		modelInfo("gpt-5.4", "GPT-5.4", now),
		modelInfo("gpt-5.4-mini", "GPT-5.4 Mini", now),
	}
	payload, _ := json.Marshal(map[string]any{"Provider": providerID, "Models": models})
	return string(payload)
}

func modelInfo(id, display string, created int64) map[string]any {
	return map[string]any{
		"ID":                         id,
		"Object":                     "model",
		"Created":                    created,
		"OwnedBy":                    providerID,
		"Type":                       "chat",
		"DisplayName":                display,
		"Name":                       id,
		"SupportedGenerationMethods": []string{"chat", "responses"},
		"ContextLength":              int64(200000),
		"MaxCompletionTokens":        int64(32768),
		"SupportedInputModalities":   []string{"text"},
		"SupportedOutputModalities":  []string{"text"},
		"UserDefined":                true,
	}
}

func routeModel(raw []byte) ([]byte, error) {
	var req struct {
		RequestedModel string `json:"RequestedModel"`
	}
	_ = json.Unmarshal(raw, &req)
	if !isFreeModelModel(req.RequestedModel) {
		return okEnvelope(map[string]any{"Handled": false, "Reason": "model_not_owned_by_freemodel"})
	}
	return okEnvelope(map[string]any{"Handled": true, "TargetKind": "executor", "Target": pluginID, "Reason": "freemodel_model"})
}

func executeModel(raw []byte, stream bool) ([]byte, error) {
	var req executorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode executor request: %w", err)
	}
	if !isFreeModelModel(req.Model) {
		return nil, fmt.Errorf("unsupported FreeModel model %q", req.Model)
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = req.OriginalRequest
	}
	if len(payload) == 0 {
		payload = []byte(fmt.Sprintf(`{"model":%q}`, req.Model))
	}
	if req.Stream || stream {
		payload = ensureJSONStreamFlag(payload, true)
	}
	upstreamReq, accountHash, err := buildUpstreamRequest(payload, req, stream)
	if err != nil {
		body := openAIErrorBody(http.StatusServiceUnavailable, err.Error())
		return ErrorEnvelopeHTTP("freemodel_no_available_account", string(body), http.StatusServiceUnavailable), nil
	}
	if stream || req.Stream {
		callbackID, pluginStreamID := rawCallbackIDs(raw)
		resp, errForward := callHostHTTPStream(callbackID, upstreamReq)
		if errForward != nil {
			return nil, errForward
		}
		if resp.StatusCode >= 400 {
			body := openAIErrorBody(resp.StatusCode, "FreeModel upstream stream bootstrap failed")
			return ErrorEnvelopeHTTP("freemodel_upstream_error", string(body), resp.StatusCode), nil
		}
		if pluginStreamID != "" {
			go relayHostHTTPStream(resp.StreamID, pluginStreamID)
			headers := resp.Headers
			if headers == nil {
				headers = contentTypeSSE()
			}
			headers["x-cpa-freemodel-account"] = []string{accountHash}
			return okEnvelope(executorStreamResponse{Headers: headers})
		}
		chunks, errRead := readHostHTTPStream(resp.StreamID)
		if errRead != nil {
			return nil, errRead
		}
		headers := resp.Headers
		if headers == nil {
			headers = contentTypeSSE()
		}
		headers["x-cpa-freemodel-account"] = []string{accountHash}
		return okEnvelope(executorStreamResponse{Headers: headers, Chunks: chunks})
	}
	resp, errForward := callHostHTTPDo(rawHostCallbackID(raw), upstreamReq)
	if errForward != nil {
		return nil, errForward
	}
	body := resp.Body
	if resp.StatusCode >= 400 {
		if len(body) == 0 || !json.Valid(body) {
			body = openAIErrorBody(resp.StatusCode, string(body))
		}
		return ErrorEnvelopeHTTP("freemodel_upstream_error", string(body), resp.StatusCode), nil
	}
	headers := resp.Headers
	if headers == nil {
		headers = contentTypeJSON()
	}
	headers["x-cpa-freemodel-account"] = []string{accountHash}
	return okEnvelope(executorResponse{Payload: body, Headers: headers})
}

func buildUpstreamRequest(payload []byte, req executorRequest, stream bool) (hostHTTPRequest, string, error) {
	db, err := runtimeState.getStore()
	if err != nil {
		return hostHTTPRequest{}, "", err
	}
	account, _, errPick := db.PickExecutableAccount(context.Background())
	if errPick != nil {
		return hostHTTPRequest{}, "", fmt.Errorf("no available FreeModel account with model api key")
	}
	path := upstreamPath(req, stream)
	headers := cloneHeaderMap(req.Headers)
	setHeader(headers, "Content-Type", "application/json")
	setHeader(headers, "Accept", acceptHeader(stream || req.Stream))
	setHeader(headers, "User-Agent", "cpa-freemodel-plugin/0.2")
	setHeader(headers, "Authorization", "Bearer "+account.ModelAPIKey)
	return hostHTTPRequest{Method: http.MethodPost, URL: runtimeState.modelAPIBase() + path, Headers: headers, Body: payload}, hashStable(account.Email), nil
}

func normalizeModelAPIBaseURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = defaultModelAPIBase
	}
	value = strings.TrimRight(value, "/")
	if !strings.HasSuffix(value, "/v1") {
		value += "/v1"
	}
	return value
}

func upstreamPath(req executorRequest, stream bool) string {
	format := strings.ToLower(strings.TrimSpace(req.Format))
	if format == formatResponses || strings.Contains(strings.ToLower(req.SourceFormat), "responses") {
		return "/responses"
	}
	if strings.Contains(string(req.OriginalRequest), `"input"`) && !strings.Contains(string(req.OriginalRequest), `"messages"`) {
		return "/responses"
	}
	return "/chat/completions"
}

func isFreeModelModel(model string) bool {
	switch strings.TrimSpace(model) {
	case "gpt-5.5", "gpt-5.4", "gpt-5.4-mini":
		return true
	default:
		return false
	}
}

func callHostHTTPDo(callbackID string, req hostHTTPRequest) (hostHTTPResponse, error) {
	req.HostCallbackID = callbackID
	raw, err := callHostResult("host.http.do", req)
	if err != nil {
		return hostHTTPResponse{}, err
	}
	var resp hostHTTPResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return hostHTTPResponse{}, err
	}
	return resp, nil
}

func callHostHTTPStream(callbackID string, req hostHTTPRequest) (hostHTTPStreamResponse, error) {
	req.HostCallbackID = callbackID
	raw, err := callHostResult("host.http.do_stream", req)
	if err != nil {
		return hostHTTPStreamResponse{}, err
	}
	var resp hostHTTPStreamResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return hostHTTPStreamResponse{}, err
	}
	return resp, nil
}

func readHostHTTPStream(streamID string) ([]executorStreamChunk, error) {
	defer func() {
		_, _ = callHostResult("host.http.stream_close", hostHTTPStreamCloseRequest{StreamID: streamID})
	}()
	chunks := make([]executorStreamChunk, 0)
	for {
		raw, err := callHostResult("host.http.stream_read", hostHTTPStreamReadRequest{StreamID: streamID})
		if err != nil {
			return nil, err
		}
		var chunk hostHTTPStreamReadResponse
		if err := json.Unmarshal(raw, &chunk); err != nil {
			return nil, err
		}
		if len(chunk.Payload) > 0 {
			chunks = append(chunks, executorStreamChunk{Payload: chunk.Payload})
		}
		if chunk.Error != "" {
			chunks = append(chunks, executorStreamChunk{Payload: []byte("data: [DONE]\n\n")})
			return chunks, nil
		}
		if chunk.Done {
			return chunks, nil
		}
	}
}

func callHostResult(method string, payload any) (json.RawMessage, error) {
	rawReq, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if hostCaller == nil {
		return nil, fmt.Errorf("host callback bridge is unavailable")
	}
	rawResp, err := hostCaller(method, rawReq)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(rawResp, &env); err != nil {
		return nil, err
	}
	if !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return nil, fmt.Errorf("host callback failed")
	}
	return env.Result, nil
}

func rawHostCallbackID(raw []byte) string {
	callbackID, _ := rawCallbackIDs(raw)
	return callbackID
}

func rawCallbackIDs(raw []byte) (string, string) {
	var tmp struct {
		HostCallbackID string `json:"host_callback_id"`
		StreamID       string `json:"stream_id"`
	}
	_ = json.Unmarshal(raw, &tmp)
	return tmp.HostCallbackID, tmp.StreamID
}

func relayHostHTTPStream(hostStreamID, pluginStreamID string) {
	defer func() {
		_, _ = callHostResult("host.http.stream_close", hostHTTPStreamCloseRequest{StreamID: hostStreamID})
	}()
	defer closePluginStream(pluginStreamID, "")
	for {
		raw, err := callHostResult("host.http.stream_read", hostHTTPStreamReadRequest{StreamID: hostStreamID})
		if err != nil {
			closePluginStream(pluginStreamID, err.Error())
			return
		}
		var chunk hostHTTPStreamReadResponse
		if err := json.Unmarshal(raw, &chunk); err != nil {
			closePluginStream(pluginStreamID, err.Error())
			return
		}
		if len(chunk.Payload) > 0 {
			if err := emitPluginStreamChunk(pluginStreamID, chunk.Payload); err != nil {
				return
			}
		}
		if chunk.Error != "" {
			closePluginStream(pluginStreamID, chunk.Error)
			return
		}
		if chunk.Done {
			return
		}
	}
}

func emitPluginStreamChunk(streamID string, payload []byte) error {
	_, err := callHostResult("host.stream.emit", map[string]any{"stream_id": streamID, "payload": payload})
	return err
}

func closePluginStream(streamID, errMsg string) {
	_, _ = callHostResult("host.stream.close", map[string]any{"stream_id": streamID, "error": errMsg})
}

func cloneHeaderMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in)+4)
	for k, values := range in {
		lk := strings.ToLower(k)
		if lk == "authorization" || lk == "cookie" || lk == "content-length" {
			continue
		}
		out[k] = append([]string(nil), values...)
	}
	return out
}

func setHeader(headers map[string][]string, key, value string) {
	for k := range headers {
		if strings.EqualFold(k, key) {
			delete(headers, k)
		}
	}
	headers[key] = []string{value}
}

func acceptHeader(stream bool) string {
	if stream {
		return "text/event-stream"
	}
	return "application/json"
}

func contentTypeJSON() map[string][]string {
	return map[string][]string{"content-type": {"application/json"}}
}
func contentTypeSSE() map[string][]string {
	return map[string][]string{"content-type": {"text/event-stream"}}
}

func openAIErrorBody(status int, message string) []byte {
	if strings.TrimSpace(message) == "" {
		message = http.StatusText(status)
	}
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"message": message, "type": "upstream_error", "code": fmt.Sprintf("freemodel_http_%d", status)}})
	return body
}

func ensureJSONStreamFlag(body []byte, stream bool) []byte {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}
	payload["stream"] = stream
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}

func normalizeSessionCookieLocal(cookieValue string) string {
	cookieValue = strings.TrimSpace(cookieValue)
	for _, part := range strings.Split(cookieValue, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "bm_session=") {
			return strings.TrimPrefix(part, "bm_session=")
		}
	}
	return cookieValue
}
