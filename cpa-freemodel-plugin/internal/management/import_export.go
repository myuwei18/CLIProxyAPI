package management

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

type importResult struct {
	Success      bool                  `json:"success"`
	DryRun       bool                  `json:"dry_run"`
	ValidateOnly bool                  `json:"validate_only"`
	Summary      importSummary         `json:"summary"`
	Items        []importResultItem    `json:"items"`
	Errors       []importValidationErr `json:"errors,omitempty"`
}

type importSummary struct {
	Total   int `json:"total"`
	Created int `json:"created"`
	Updated int `json:"updated"`
	Skipped int `json:"skipped"`
	Errors  int `json:"errors"`
}

type importResultItem struct {
	Index     int    `json:"index"`
	Action    string `json:"action"`
	AccountID int64  `json:"account_id,omitempty"`
	Email     string `json:"email,omitempty"`
	EmailHash string `json:"email_hash,omitempty"`
	Message   string `json:"message,omitempty"`
}

type importValidationErr struct {
	Index   int    `json:"index"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func handleExportAccounts(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodGet {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	accounts, errList := db.ListAccounts(context.Background())
	if errList != nil {
		return jsonResponse(http.StatusInternalServerError, failure(errList.Error()))
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"success":              true,
		"schema_version":       schemaVersion,
		"redacted":             true,
		"includes_secrets":     false,
		"operator_only_fields": []string{},
		"data":                 accountsPublic(accounts),
	})
}

func handleExportQuotaSnapshots(req managementRequest) ([]byte, error) {
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
		"success":        true,
		"schema_version": schemaVersion,
		"redacted":       true,
		"data":           usageSnapshotsPublic(items),
	})
}

func handleExportIncidents(req managementRequest) ([]byte, error) {
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
	return jsonResponse(http.StatusOK, map[string]any{
		"success":        true,
		"schema_version": schemaVersion,
		"redacted":       true,
		"data":           incidentsPublic(items),
	})
}

func handleImportAccounts(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var payload struct {
		DryRun       bool                 `json:"dry_run"`
		ValidateOnly bool                 `json:"validate_only"`
		Accounts     []store.AccountInput `json:"accounts"`
	}
	if errDecode := json.Unmarshal(req.Body, &payload); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	result := importResult{Success: true, DryRun: payload.DryRun, ValidateOnly: payload.ValidateOnly}
	result.Summary.Total = len(payload.Accounts)
	for i, account := range payload.Accounts {
		email := strings.TrimSpace(account.Email)
		if email == "" {
			result.Errors = append(result.Errors, importValidationErr{Index: i, Field: "email", Message: "email is required"})
			result.Summary.Errors++
			continue
		}
		existing, errExisting := db.GetAccount(context.Background(), email)
		action := "created"
		if errExisting == nil && existing != nil {
			action = "updated"
		}
		item := importResultItem{Index: i, Action: action, Email: maskEmail(email), EmailHash: hashStable(email)}
		if payload.DryRun || payload.ValidateOnly {
			result.Items = append(result.Items, item)
			if action == "created" {
				result.Summary.Created++
			} else {
				result.Summary.Updated++
			}
			continue
		}
		saved, errSave := db.SaveAccount(context.Background(), account)
		if errSave != nil {
			result.Errors = append(result.Errors, importValidationErr{Index: i, Message: errSave.Error()})
			result.Summary.Errors++
			continue
		}
		item.AccountID = saved.ID
		result.Items = append(result.Items, item)
		if action == "created" {
			result.Summary.Created++
		} else {
			result.Summary.Updated++
		}
	}
	result.Success = result.Summary.Errors == 0
	status := http.StatusOK
	if !result.Success {
		status = http.StatusBadRequest
	}
	return jsonResponse(status, result)
}

func handleImportSnapshots(req managementRequest) ([]byte, error) {
	if strings.ToUpper(req.Method) != http.MethodPost {
		return methodNotAllowed()
	}
	db, err := runtimeState.getStore()
	if err != nil {
		return jsonResponse(http.StatusServiceUnavailable, failure(err.Error()))
	}
	var payload struct {
		DryRun       bool                  `json:"dry_run"`
		ValidateOnly bool                  `json:"validate_only"`
		Snapshots    []store.QuotaSnapshot `json:"snapshots"`
	}
	if errDecode := json.Unmarshal(req.Body, &payload); errDecode != nil {
		return jsonResponse(http.StatusBadRequest, failure("invalid request: "+errDecode.Error()))
	}
	result := importResult{Success: true, DryRun: payload.DryRun, ValidateOnly: payload.ValidateOnly}
	result.Summary.Total = len(payload.Snapshots)
	for i, snap := range payload.Snapshots {
		email := strings.TrimSpace(snap.Email)
		if email == "" {
			result.Errors = append(result.Errors, importValidationErr{Index: i, Field: "email", Message: "email is required"})
			result.Summary.Errors++
			continue
		}
		item := importResultItem{Index: i, Action: "created", Email: maskEmail(email), EmailHash: hashStable(email)}
		if payload.DryRun || payload.ValidateOnly {
			result.Items = append(result.Items, item)
			result.Summary.Created++
			continue
		}
		stored, errSave := db.SaveSnapshot(context.Background(), snap)
		if errSave != nil {
			result.Errors = append(result.Errors, importValidationErr{Index: i, Message: errSave.Error()})
			result.Summary.Errors++
			continue
		}
		item.AccountID = stored.AccountID
		result.Items = append(result.Items, item)
		result.Summary.Created++
	}
	result.Success = result.Summary.Errors == 0
	status := http.StatusOK
	if !result.Success {
		status = http.StatusBadRequest
	}
	return jsonResponse(status, result)
}
