package freemodel

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

var httpStatusPattern = regexp.MustCompile(`HTTP (\d{3})`)

func incidentFromSyncError(accountEmail string, err error) store.Incident {
	message := ""
	if err != nil {
		message = err.Error()
	}
	lower := strings.ToLower(message)
	statusCode := extractStatusCode(message)
	incident := store.Incident{
		Kind:         "sync_error",
		Severity:     "warning",
		StatusCode:   statusCode,
		Message:      message,
		AccountEmail: accountEmail,
		DetectedAt:   time.Now().UTC(),
	}
	switch {
	case statusCode == 401:
		incident.Kind = "auth_invalid"
		incident.Severity = "critical"
	case statusCode == 402 && strings.Contains(lower, "insufficient"):
		incident.Kind = "insufficient_funds"
		incident.Severity = "critical"
	case statusCode == 402:
		incident.Kind = "usage_limit"
		incident.Severity = "warning"
	case statusCode == 403 && strings.Contains(lower, "ip_account_conflict"):
		incident.Kind = "ip_account_conflict"
		incident.Severity = "critical"
	case statusCode == 403:
		incident.Kind = "permission_error"
		incident.Severity = "critical"
	case statusCode == 429:
		incident.Kind = "quota_exhausted"
		incident.Severity = "warning"
	case statusCode >= 500:
		incident.Kind = "upstream_unavailable"
		incident.Severity = "warning"
	}
	return incident
}

func extractStatusCode(message string) int {
	match := httpStatusPattern.FindStringSubmatch(message)
	if len(match) != 2 {
		return 0
	}
	statusCode, err := strconv.Atoi(match[1])
	if err != nil {
		return 0
	}
	return statusCode
}

func syncErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 300 {
		return fmt.Sprintf("%s...", msg[:300])
	}
	return msg
}
