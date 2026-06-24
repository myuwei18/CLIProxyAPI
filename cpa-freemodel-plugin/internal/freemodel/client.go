// Package freemodel provides a FreeModel supplier API client for the plugin.
package freemodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/myuwei18/cpa-freemodel-plugin/internal/store"
)

const (
	defaultBaseURL   = "https://freemodel.dev"
	apiUsagePath     = "/api/usage"
	apiBillingPath   = "/api/billing"
	apiAuthMePath    = "/api/auth/me"
	apiReferralPath  = "/api/referral"
	apiSendOTPPath   = "/api/auth/send-otp"
	apiVerifyOTPPath = "/api/auth/verify-otp"
)

// Client calls the FreeModel dashboard APIs.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// UsageResponse represents the /api/usage endpoint response.
type UsageResponse struct {
	TotalRequests int64       `json:"totalRequests"`
	TotalTokens   int64       `json:"totalTokens"`
	Window5h      WindowUsage `json:"window5h"`
	WindowWeek    WindowUsage `json:"windowWeek"`
}

// WindowUsage represents a FreeModel sliding window.
type WindowUsage struct {
	UsedCents  int64 `json:"usedCents"`
	LimitCents int64 `json:"limitCents"`
	ResetsAt   int64 `json:"resetsAt"`
}

// BillingResponse represents the /api/billing endpoint response.
type BillingResponse struct {
	Subscription    Subscription `json:"subscription"`
	CreditCents     int64        `json:"creditCents"`
	TotalTopupPence int64        `json:"totalTopupGbpPence"`
}

// Subscription describes the current FreeModel plan subscription.
type Subscription struct {
	PlanID            string  `json:"planId"`
	Status            string  `json:"status"`
	CurrentPeriodEnd  *string `json:"currentPeriodEnd"`
	CancelAtPeriodEnd bool    `json:"cancelAtPeriodEnd"`
}

// ReferralResponse represents the /api/referral endpoint response.
type ReferralResponse struct {
	Credits float64 `json:"credits"`
	Used    float64 `json:"used"`
}

// AuthMeResponse represents the /api/auth/me endpoint response.
type AuthMeResponse struct {
	User UserInfo `json:"user"`
}

// UserInfo describes the authenticated FreeModel dashboard user.
type UserInfo struct {
	ID    int64  `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

// NewClient creates a FreeModel API client. The HTTP client intentionally has no
// request timeout because plugin sync should not impose post-connect network deadlines.
func NewClient(proxyURL string) (*Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL != "" && proxyURL != "direct" && proxyURL != "none" {
		parsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(parsed)
	}
	return &Client{httpClient: &http.Client{Transport: transport}, baseURL: defaultBaseURL}, nil
}

// SendOTP sends a FreeModel login verification code.
func (c *Client) SendOTP(email string) error {
	payload, err := json.Marshal(map[string]string{"email": strings.TrimSpace(email)})
	if err != nil {
		return fmt.Errorf("marshal send-otp: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+apiSendOTPPath, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "cpa-freemodel-plugin/0.1")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send otp: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("send otp HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// VerifyOTP verifies a login code and returns the bm_session cookie value.
func (c *Client) VerifyOTP(email, code string) (string, error) {
	payload, err := json.Marshal(map[string]string{"email": strings.TrimSpace(email), "code": strings.TrimSpace(code)})
	if err != nil {
		return "", fmt.Errorf("marshal verify-otp: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+apiVerifyOTPPath, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "cpa-freemodel-plugin/0.1")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("verify otp: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("verify otp HTTP %d: %s", resp.StatusCode, string(body))
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "bm_session" && cookie.Value != "" {
			return cookie.Value, nil
		}
	}
	return "", fmt.Errorf("no bm_session cookie in response")
}

// FetchSnapshot collects usage, billing, referral, and account identity data.
func (c *Client) FetchSnapshot(account store.Account) (store.QuotaSnapshot, int64, error) {
	var usage UsageResponse
	if err := c.doGet(apiUsagePath, account.Cookie, &usage); err != nil {
		return store.QuotaSnapshot{}, 0, fmt.Errorf("fetch usage: %w", err)
	}
	var billing BillingResponse
	if err := c.doGet(apiBillingPath, account.Cookie, &billing); err != nil {
		return store.QuotaSnapshot{}, 0, fmt.Errorf("fetch billing: %w", err)
	}
	var referral ReferralResponse
	if err := c.doGet(apiReferralPath, account.Cookie, &referral); err != nil {
		return store.QuotaSnapshot{}, 0, fmt.Errorf("fetch referral: %w", err)
	}
	var me AuthMeResponse
	if err := c.doGet(apiAuthMePath, account.Cookie, &me); err != nil {
		return store.QuotaSnapshot{}, 0, fmt.Errorf("fetch user info: %w", err)
	}
	email := account.Email
	if strings.TrimSpace(me.User.Email) != "" {
		email = me.User.Email
	}
	currentPeriodEnd := ""
	if billing.Subscription.CurrentPeriodEnd != nil {
		currentPeriodEnd = *billing.Subscription.CurrentPeriodEnd
	}
	return store.QuotaSnapshot{
		Email:             email,
		PlanID:            billing.Subscription.PlanID,
		PlanStatus:        billing.Subscription.Status,
		CreditCents:       billing.CreditCents,
		TopupCents:        billing.TotalTopupPence,
		ReferralCredits:   referral.Credits,
		ReferralUsed:      referral.Used,
		CurrentPeriodEnd:  currentPeriodEnd,
		CancelAtPeriodEnd: billing.Subscription.CancelAtPeriodEnd,
		Window5h: store.WindowUsage{
			UsedCents:  usage.Window5h.UsedCents,
			LimitCents: usage.Window5h.LimitCents,
			ResetsAt:   usage.Window5h.ResetsAt,
		},
		WindowWeek: store.WindowUsage{
			UsedCents:  usage.WindowWeek.UsedCents,
			LimitCents: usage.WindowWeek.LimitCents,
			ResetsAt:   usage.WindowWeek.ResetsAt,
		},
		TotalRequests: usage.TotalRequests,
		TotalTokens:   usage.TotalTokens,
		FetchedAt:     time.Now().UTC(),
	}, me.User.ID, nil
}

func (c *Client) doGet(path, cookieValue string, out any) error {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Cookie", "bm_session="+normalizeSessionCookie(cookieValue))
	req.Header.Set("User-Agent", "cpa-freemodel-plugin/0.1")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return upstreamHTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type upstreamHTTPError struct {
	StatusCode int
	Body       string
}

func (e upstreamHTTPError) Error() string {
	body := strings.ReplaceAll(e.Body, `"`, `'`)
	body = strings.ReplaceAll(body, "\n", " ")
	if len(body) > 240 {
		body = body[:240] + "..."
	}
	if body == "" {
		return fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, body)
}

func normalizeSessionCookie(cookieValue string) string {
	cookieValue = strings.TrimSpace(cookieValue)
	for _, part := range strings.Split(cookieValue, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "bm_session=") {
			return strings.TrimPrefix(part, "bm_session=")
		}
	}
	return cookieValue
}
