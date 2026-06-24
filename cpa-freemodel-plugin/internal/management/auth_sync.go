package management

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	freemodelsvc "github.com/myuwei18/cpa-freemodel-plugin/internal/freemodel"
	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

type publicSyncResult struct {
	StartedAt  string                 `json:"started_at"`
	FinishedAt string                 `json:"finished_at"`
	Total      int                    `json:"total"`
	Succeeded  int                    `json:"succeeded"`
	Failed     int                    `json:"failed"`
	Items      []publicSyncResultItem `json:"items"`
}

type publicSyncResultItem struct {
	Email     string `json:"email"`
	EmailHash string `json:"email_hash"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
}

func handleSendOTP(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	var input struct {
		Email string `json:"email"`
		Proxy string `json:"proxy"`
	}
	if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	client, errClient := freemodelsvc.NewClient(input.Proxy)
	if errClient != nil {
		return jsonResponse(http.StatusBadRequest, failure(errClient.Error()))
	}
	if errSend := client.SendOTP(input.Email); errSend != nil {
		return jsonResponse(http.StatusBadGateway, failure("send verification code failed: "+safeUpstreamError(errSend)))
	}
	return jsonResponse(http.StatusOK, map[string]any{"success": true, "message": "verification code sent"})
}

func handleVerifyOTP(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var input struct {
		Email string `json:"email"`
		Code  string `json:"code"`
		Proxy string `json:"proxy"`
	}
	if errDecode := json.Unmarshal(req.Body, &input); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	client, errClient := freemodelsvc.NewClient(input.Proxy)
	if errClient != nil {
		return jsonResponse(http.StatusBadRequest, failure(errClient.Error()))
	}
	cookie, errVerify := client.VerifyOTP(input.Email, input.Code)
	if errVerify != nil {
		return jsonResponse(http.StatusUnauthorized, failure("verification failed: "+safeUpstreamError(errVerify)))
	}
	account, errSave := db.SaveAccount(context.Background(), store.AccountInput{Email: input.Email, Cookie: cookie, Proxy: input.Proxy})
	if errSave != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errSave.Error()))
	}
	return jsonResponse(http.StatusOK, success(accountPublic(*account)))
}

func handleSync(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	syncer := runtimeState.getSyncer()
	if syncer == nil {
		return jsonResponse(http.StatusServiceUnavailable, failure("sync service is not initialized"))
	}
	result, errSync := syncer.SyncAll(context.Background())
	if errSync != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errSync.Error()))
	}
	return jsonResponse(http.StatusOK, success(syncResultPublic(result)))
}

func handleSyncStatus(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	syncer := runtimeState.getSyncer()
	if syncer == nil {
		return jsonResponse(http.StatusServiceUnavailable, failure("sync service is not initialized"))
	}
	return jsonResponse(http.StatusOK, success(syncResultPublic(syncer.LastResult())))
}

func (s *state) getSyncer() *freemodelsvc.Service {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncer
}

func syncResultPublic(result *freemodelsvc.SyncResult) *publicSyncResult {
	if result == nil {
		return nil
	}
	out := &publicSyncResult{
		StartedAt:  formatTime(result.StartedAt),
		FinishedAt: formatTime(result.FinishedAt),
		Total:      result.Total,
		Succeeded:  result.Succeeded,
		Failed:     result.Failed,
		Items:      make([]publicSyncResultItem, 0, len(result.Items)),
	}
	for _, item := range result.Items {
		out.Items = append(out.Items, publicSyncResultItem{
			Email:     maskEmail(item.Email),
			EmailHash: hashStable(item.Email),
			Status:    item.Status,
			Message:   item.Message,
		})
	}
	return out
}

func safeUpstreamError(err error) string {
	if err == nil {
		return "unknown upstream error"
	}
	msg := strings.TrimSpace(err.Error())
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "cookie") || strings.Contains(lower, "session") || strings.Contains(lower, "token") || strings.Contains(lower, "authorization") || strings.Contains(lower, "password") || strings.Contains(lower, "fe_oa_") {
		return "upstream request failed"
	}
	if len(msg) > 160 {
		msg = msg[:160]
	}
	if msg == "" {
		return "upstream request failed"
	}
	return msg
}
