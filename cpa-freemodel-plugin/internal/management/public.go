package management

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"net/url"
	"strings"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

type publicAccount struct {
	ID                 int64  `json:"id"`
	Email              string `json:"email"`
	EmailHash          string `json:"email_hash"`
	UserID             int64  `json:"user_id"`
	ModelAPIConfigured bool   `json:"model_api_configured"`
	Proxy              string `json:"proxy"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

type publicQuotaSnapshot struct {
	AccountID               int64             `json:"account_id"`
	Email                   string            `json:"email"`
	EmailHash               string            `json:"email_hash"`
	PlanID                  string            `json:"plan_id"`
	PlanStatus              string            `json:"plan_status"`
	CreditCents             int64             `json:"credit_cents"`
	TopupCents              int64             `json:"topup_cents"`
	ReferralCredits         float64           `json:"referral_credits"`
	ReferralUsed            float64           `json:"referral_used"`
	CurrentPeriodEnd        string            `json:"current_period_end"`
	CancelAtPeriodEnd       bool              `json:"cancel_at_period_end"`
	Window5h                store.WindowUsage `json:"window_5h"`
	WindowWeek              store.WindowUsage `json:"window_week"`
	TotalRequests           int64             `json:"total_requests"`
	TotalTokens             int64             `json:"total_tokens"`
	FetchedAt               string            `json:"fetched_at"`
	Window5hRemaining       int64             `json:"window_5h_remaining"`
	WindowWeekRemaining     int64             `json:"window_week_remaining"`
	WindowAvailableCents    int64             `json:"window_available_cents"`
	ExtraAvailableCents     int64             `json:"extra_available_cents"`
	RealAvailableCents      int64             `json:"real_available_cents"`
	SubscriptionExpired     bool              `json:"subscription_expired"`
	AvailabilityStatus      string            `json:"availability_status"`
	AvailabilityReason      string            `json:"availability_reason"`
	SyncStatus              string            `json:"sync_status"`
	DashboardAuthConfigured bool              `json:"dashboard_auth_configured"`
	IsTestAccount           bool              `json:"is_test_account"`
	AccountKind             string            `json:"account_kind"`
	Proxy                   string            `json:"proxy"`
}

type publicUsageSnapshot struct {
	AccountID               int64   `json:"account_id"`
	EmailHash               string  `json:"email_hash"`
	Email                   string  `json:"email"`
	FetchedAt               string  `json:"fetched_at"`
	PlanID                  string  `json:"plan_id"`
	PlanStatus              string  `json:"plan_status"`
	CreditCents             int64   `json:"credit_cents"`
	TopupCents              int64   `json:"topup_cents"`
	ReferralCredits         float64 `json:"referral_credits"`
	ReferralUsed            float64 `json:"referral_used"`
	Window5hUsed            int64   `json:"window_5h_used"`
	Window5hLimit           int64   `json:"window_5h_limit"`
	WindowWeekUsed          int64   `json:"window_week_used"`
	WindowWeekLimit         int64   `json:"window_week_limit"`
	TotalRequests           int64   `json:"total_requests"`
	TotalTokens             int64   `json:"total_tokens"`
	RealAvailableCents      int64   `json:"real_available_cents"`
	ExtraAvailableCents     int64   `json:"extra_available_cents"`
	SubscriptionExpired     bool    `json:"subscription_expired"`
	AvailabilityStatus      string  `json:"availability_status"`
	AvailabilityReason      string  `json:"availability_reason"`
	DashboardAuthConfigured bool    `json:"dashboard_auth_configured"`
	IsTestAccount           bool    `json:"is_test_account"`
	AccountKind             string  `json:"account_kind"`
}

func accountPublic(account store.Account) publicAccount {
	return accountPublicWithRedaction(account, true)
}

func accountPublicWithRedaction(account store.Account, redacted bool) publicAccount {
	email := account.Email
	if redacted {
		email = maskEmail(account.Email)
	}
	return publicAccount{
		ID:                 account.ID,
		Email:              email,
		EmailHash:          hashStable(account.Email),
		UserID:             account.UserID,
		ModelAPIConfigured: account.ModelAPIConfigured,
		Proxy:              maskProxy(account.Proxy),
		CreatedAt:          formatTime(account.CreatedAt),
		UpdatedAt:          formatTime(account.UpdatedAt),
	}
}

func accountsPublic(accounts []store.Account) []publicAccount {
	out := make([]publicAccount, 0, len(accounts))
	for _, account := range accounts {
		out = append(out, accountPublic(account))
	}
	return out
}

func quotaPublic(snap store.QuotaSnapshot) publicQuotaSnapshot {
	return quotaPublicWithRedaction(snap, true)
}

func quotaPublicWithRedaction(snap store.QuotaSnapshot, redacted bool) publicQuotaSnapshot {
	email := snap.Email
	if redacted {
		email = maskEmail(snap.Email)
	}
	return publicQuotaSnapshot{
		AccountID:               snap.AccountID,
		Email:                   email,
		EmailHash:               hashStable(snap.Email),
		PlanID:                  snap.PlanID,
		PlanStatus:              snap.PlanStatus,
		CreditCents:             snap.CreditCents,
		TopupCents:              snap.TopupCents,
		ReferralCredits:         snap.ReferralCredits,
		ReferralUsed:            snap.ReferralUsed,
		CurrentPeriodEnd:        snap.CurrentPeriodEnd,
		CancelAtPeriodEnd:       snap.CancelAtPeriodEnd,
		Window5h:                snap.Window5h,
		WindowWeek:              snap.WindowWeek,
		TotalRequests:           snap.TotalRequests,
		TotalTokens:             snap.TotalTokens,
		FetchedAt:               formatTime(snap.FetchedAt),
		Window5hRemaining:       snap.Window5hRemaining,
		WindowWeekRemaining:     snap.WindowWeekRemaining,
		WindowAvailableCents:    snap.WindowAvailableCents,
		ExtraAvailableCents:     snap.ExtraAvailableCents,
		RealAvailableCents:      snap.RealAvailableCents,
		SubscriptionExpired:     snap.SubscriptionExpired,
		AvailabilityStatus:      snap.AvailabilityStatus,
		AvailabilityReason:      snap.AvailabilityReason,
		SyncStatus:              snap.SyncStatus,
		DashboardAuthConfigured: snap.HasDashboardCookie,
		IsTestAccount:           snap.IsTestAccount,
		AccountKind:             snap.AccountKind,
		Proxy:                   maskProxy(snap.Proxy),
	}
}

func quotasPublic(snaps []store.QuotaSnapshot) []publicQuotaSnapshot {
	return quotasPublicWithRedaction(snaps, true)
}

func quotasPublicWithRedaction(snaps []store.QuotaSnapshot, redacted bool) []publicQuotaSnapshot {
	out := make([]publicQuotaSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		out = append(out, quotaPublicWithRedaction(snap, redacted))
	}
	return out
}

func usageSnapshotPublic(snap store.QuotaSnapshot) publicUsageSnapshot {
	return publicUsageSnapshot{
		AccountID:               snap.AccountID,
		EmailHash:               hashStable(snap.Email),
		Email:                   maskEmail(snap.Email),
		FetchedAt:               formatTime(snap.FetchedAt),
		PlanID:                  snap.PlanID,
		PlanStatus:              snap.PlanStatus,
		CreditCents:             snap.CreditCents,
		TopupCents:              snap.TopupCents,
		ReferralCredits:         snap.ReferralCredits,
		ReferralUsed:            snap.ReferralUsed,
		Window5hUsed:            snap.Window5h.UsedCents,
		Window5hLimit:           snap.Window5h.LimitCents,
		WindowWeekUsed:          snap.WindowWeek.UsedCents,
		WindowWeekLimit:         snap.WindowWeek.LimitCents,
		TotalRequests:           snap.TotalRequests,
		TotalTokens:             snap.TotalTokens,
		RealAvailableCents:      snap.RealAvailableCents,
		ExtraAvailableCents:     snap.ExtraAvailableCents,
		SubscriptionExpired:     snap.SubscriptionExpired,
		AvailabilityStatus:      snap.AvailabilityStatus,
		AvailabilityReason:      snap.AvailabilityReason,
		DashboardAuthConfigured: snap.HasDashboardCookie,
		IsTestAccount:           snap.IsTestAccount,
		AccountKind:             snap.AccountKind,
	}
}

func usageSnapshotsPublic(snaps []store.QuotaSnapshot) []publicUsageSnapshot {
	out := make([]publicUsageSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		out = append(out, usageSnapshotPublic(snap))
	}
	return out
}

func maskEmail(email string) string {
	email = strings.TrimSpace(email)
	if email == "" {
		return ""
	}
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return "***"
	}
	local := []rune(parts[0])
	domain := parts[1]
	switch len(local) {
	case 0:
		return "***@" + domain
	case 1:
		return string(local[0]) + "***@" + domain
	case 2, 3, 4:
		return string(local[:1]) + "***" + string(local[len(local)-1:]) + "@" + domain
	default:
		return string(local[:2]) + "***" + string(local[len(local)-2:]) + "@" + domain
	}
}

func maskProxy(proxyURL string) string {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" || proxyURL == "direct" || proxyURL == "none" {
		return proxyURL
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return "代理地址已脱敏"
	}
	if parsed.RawQuery != "" {
		query := parsed.Query()
		for key := range query {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "token") || strings.Contains(lower, "key") || strings.Contains(lower, "secret") || strings.Contains(lower, "pass") || strings.Contains(lower, "auth") || strings.Contains(lower, "session") {
				query.Set(key, "redacted")
			}
		}
		parsed.RawQuery = query.Encode()
	}
	if parsed.User != nil {
		return parsed.Scheme + "://redacted@" + parsed.Host + parsed.EscapedPath() + querySuffix(parsed.RawQuery) + fragmentSuffix(parsed.EscapedFragment())
	}
	return parsed.String()
}

func querySuffix(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	return "?" + rawQuery
}

func fragmentSuffix(fragment string) string {
	if fragment == "" {
		return ""
	}
	return "#" + fragment
}

func hashStable(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func shortHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}

const timeFormatRFC3339 = time.RFC3339

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(timeFormatRFC3339)
}
