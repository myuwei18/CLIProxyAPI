package freemodel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

// SyncResult describes one sync run.
type SyncResult struct {
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at"`
	Total      int              `json:"total"`
	Succeeded  int              `json:"succeeded"`
	Failed     int              `json:"failed"`
	Items      []SyncResultItem `json:"items"`
}

// SyncResultItem describes one account sync result.
type SyncResultItem struct {
	Email   string `json:"email"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Service owns FreeModel account sync operations.
type Service struct {
	store *store.Store
	mu    sync.Mutex
	last  *SyncResult
}

// NewService creates a FreeModel sync service.
func NewService(store *store.Store) *Service {
	return &Service{store: store}
}

// LastResult returns the latest sync result.
func (s *Service) LastResult() *SyncResult {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return nil
	}
	copyResult := *s.last
	copyResult.Items = append([]SyncResultItem(nil), s.last.Items...)
	return &copyResult
}

// SyncAll fetches and stores quota snapshots for every configured account.
func (s *Service) SyncAll(ctx context.Context) (*SyncResult, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("sync service is not initialized")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	result := &SyncResult{StartedAt: time.Now().UTC()}
	accounts, err := s.store.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	result.Total = len(accounts)
	result.Items = make([]SyncResultItem, 0, len(accounts))
	for _, account := range accounts {
		item := SyncResultItem{Email: account.Email, Status: "ok"}
		client, errClient := NewClient(account.Proxy)
		if errClient != nil {
			item.Status = "error"
			item.Message = errClient.Error()
			result.Failed++
			result.Items = append(result.Items, item)
			continue
		}
		snapshot, userID, errFetch := client.FetchSnapshot(account)
		if errFetch != nil {
			item.Status = "error"
			item.Message = syncErrorMessage(errFetch)
			_, _ = s.store.SaveIncident(ctx, incidentFromSyncError(account.Email, errFetch))
			result.Failed++
			result.Items = append(result.Items, item)
			continue
		}
		if _, errSave := s.store.SaveSnapshot(ctx, snapshot); errSave != nil {
			item.Status = "error"
			item.Message = errSave.Error()
			result.Failed++
			result.Items = append(result.Items, item)
			continue
		}
		if userID > 0 {
			_ = s.store.UpdateAccountUserID(ctx, account.Email, userID)
		}
		result.Succeeded++
		result.Items = append(result.Items, item)
	}
	result.FinishedAt = time.Now().UTC()
	s.last = result
	return result, nil
}
