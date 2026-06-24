package store

import "time"

// Account stores a FreeModel account configuration without exposing secrets in list responses.
type Account struct {
	ID                 int64     `json:"id"`
	Email              string    `json:"email"`
	UserID             int64     `json:"user_id"`
	Cookie             string    `json:"-"`
	ModelAPIKey        string    `json:"-"`
	ModelAPIConfigured bool      `json:"model_api_configured"`
	Proxy              string    `json:"proxy"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// AccountInput is the persisted account input accepted by the plugin API.
type AccountInput struct {
	Email       string `json:"email"`
	Cookie      string `json:"cookie"`
	ModelAPIKey string `json:"api_key"`
	Proxy       string `json:"proxy"`
}

// WindowUsage stores one FreeModel quota window.
type WindowUsage struct {
	UsedCents  int64 `json:"used_cents"`
	LimitCents int64 `json:"limit_cents"`
	ResetsAt   int64 `json:"resets_at"`
}

// QuotaSnapshot stores a FreeModel quota snapshot compatible with the legacy table shape.
type QuotaSnapshot struct {
	ID                   int64       `json:"id,omitempty"`
	AccountID            int64       `json:"account_id,omitempty"`
	Email                string      `json:"email"`
	PlanID               string      `json:"plan_id"`
	PlanStatus           string      `json:"plan_status"`
	CreditCents          int64       `json:"credit_cents"`
	TopupCents           int64       `json:"topup_cents"`
	ReferralCredits      float64     `json:"referral_credits"`
	ReferralUsed         float64     `json:"referral_used"`
	CurrentPeriodEnd     string      `json:"current_period_end"`
	CancelAtPeriodEnd    bool        `json:"cancel_at_period_end"`
	Window5h             WindowUsage `json:"window_5h"`
	WindowWeek           WindowUsage `json:"window_week"`
	TotalRequests        int64       `json:"total_requests"`
	TotalTokens          int64       `json:"total_tokens"`
	FetchedAt            time.Time   `json:"fetched_at"`
	Window5hRemaining    int64       `json:"window_5h_remaining"`
	WindowWeekRemaining  int64       `json:"window_week_remaining"`
	WindowAvailableCents int64       `json:"window_available_cents"`
	ExtraAvailableCents  int64       `json:"extra_available_cents"`
	RealAvailableCents   int64       `json:"real_available_cents"`
	SubscriptionExpired  bool        `json:"subscription_expired"`
	AvailabilityStatus   string      `json:"availability_status"`
	AvailabilityReason   string      `json:"availability_reason"`
	SyncStatus           string      `json:"sync_status"`
	Proxy                string      `json:"proxy"`
}
