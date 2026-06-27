package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestListSnapshotsDerivesAccountState(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "freemodel.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = db.Close() }()

	cases := []struct {
		name          string
		account       AccountInput
		snap          QuotaSnapshot
		wantStatus    string
		wantReason    string
		wantKind      string
		wantCookie    bool
		wantAvailable bool
	}{
		{
			name:          "real account with dashboard cookie is available",
			account:       AccountInput{Email: "real@example.net", Cookie: "dashboard-cookie"},
			snap:          QuotaSnapshot{Email: "real@example.net", PlanID: "pro", PlanStatus: "active", CreditCents: 250, CurrentPeriodEnd: "2099-01-01", Window5h: WindowUsage{LimitCents: 1000}, WindowWeek: WindowUsage{LimitCents: 1000}, FetchedAt: time.Now().UTC()},
			wantStatus:    "available",
			wantReason:    "real_balance_available",
			wantKind:      "real",
			wantCookie:    true,
			wantAvailable: true,
		},
		{
			name:       "api key only account stays missing cookie",
			account:    AccountInput{Email: "apikey@example.net", ModelAPIKey: "model-key-only"},
			snap:       QuotaSnapshot{Email: "apikey@example.net", PlanID: "pro", PlanStatus: "active", CreditCents: 250, CurrentPeriodEnd: "2099-01-01", Window5h: WindowUsage{LimitCents: 1000}, WindowWeek: WindowUsage{LimitCents: 1000}, FetchedAt: time.Now().Add(time.Second).UTC()},
			wantStatus: "other",
			wantReason: "missing_cookie",
			wantKind:   "real",
			wantCookie: false,
		},
		{
			name:       "test account is never available",
			account:    AccountInput{Email: "demo@example.com", Cookie: "dashboard-cookie"},
			snap:       QuotaSnapshot{Email: "demo@example.com", PlanID: "pro", PlanStatus: "active", CreditCents: 250, CurrentPeriodEnd: "2099-01-01", Window5h: WindowUsage{LimitCents: 1000}, WindowWeek: WindowUsage{LimitCents: 1000}, FetchedAt: time.Now().Add(2 * time.Second).UTC()},
			wantStatus: "other",
			wantReason: "test_account",
			wantKind:   "test",
			wantCookie: true,
		},
	}

	for _, tc := range cases {
		if _, err := db.SaveAccount(ctx, tc.account); err != nil {
			t.Fatalf("save account %s: %v", tc.name, err)
		}
		if _, err := db.SaveSnapshot(ctx, tc.snap); err != nil {
			t.Fatalf("save snapshot %s: %v", tc.name, err)
		}
	}

	items, err := db.ListSnapshots(ctx, 10)
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	byEmail := map[string]QuotaSnapshot{}
	for _, item := range items {
		byEmail[item.Email] = item
	}

	for _, tc := range cases {
		got, ok := byEmail[tc.account.Email]
		if !ok {
			t.Fatalf("snapshot for %s not found", tc.account.Email)
		}
		if got.HasDashboardCookie != tc.wantCookie {
			t.Fatalf("%s HasDashboardCookie=%v want %v", tc.name, got.HasDashboardCookie, tc.wantCookie)
		}
		if got.AccountKind != tc.wantKind {
			t.Fatalf("%s AccountKind=%q want %q", tc.name, got.AccountKind, tc.wantKind)
		}
		if got.AvailabilityStatus != tc.wantStatus || got.AvailabilityReason != tc.wantReason {
			t.Fatalf("%s availability=%s/%s want %s/%s", tc.name, got.AvailabilityStatus, got.AvailabilityReason, tc.wantStatus, tc.wantReason)
		}
		if tc.wantAvailable && got.RealAvailableCents <= 0 {
			t.Fatalf("%s RealAvailableCents=%d want positive", tc.name, got.RealAvailableCents)
		}
	}
}
