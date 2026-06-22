package kiro

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestAuthManager_RefreshGuardLockHeldAdoptsReloadedToken verifies the
// single-flight refresh path: when the RefreshGuard reports the distributed lock
// is held by another instance, AuthManager adopts the freshly persisted token
// via ReloadCreds instead of performing its own HTTP refresh.
func TestAuthManager_RefreshGuardLockHeldAdoptsReloadedToken(t *testing.T) {
	refreshCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accessToken":"SHOULD_NOT_BE_USED","expiresIn":3600}`))
	}))
	defer srv.Close()

	// Expired creds so a refresh is needed.
	past := time.Now().Add(-time.Hour)
	am := NewAuthManager(Credentials{
		RefreshToken: "rt",
		AccessToken:  "stale",
		Region:       "us-east-1",
		ExpiresAt:    &past,
	}, srv.Client())

	freshExpiry := time.Now().Add(time.Hour)
	reloadCalled := false
	am.RefreshGuard = func(ctx context.Context, refresh func() error) error {
		// Simulate the lock being held by another instance.
		return ErrRefreshLockHeld
	}
	am.ReloadCreds = func(ctx context.Context) (Credentials, bool) {
		reloadCalled = true
		return Credentials{AccessToken: "fresh-from-db", ExpiresAt: &freshExpiry}, true
	}

	tok, err := am.AccessToken(context.Background())
	if err != nil {
		t.Fatalf("AccessToken error: %v", err)
	}
	if tok != "fresh-from-db" {
		t.Fatalf("token = %q, want fresh-from-db", tok)
	}
	if !reloadCalled {
		t.Fatal("expected ReloadCreds to be called when lock held")
	}
	if refreshCalls != 0 {
		t.Fatalf("expected no HTTP refresh when lock held, got %d", refreshCalls)
	}
}

// TestAuthManager_RefreshGuardSerializesRefresh verifies the guard actually
// wraps the refresh closure (single-flight): only one HTTP refresh occurs even
// under concurrent AccessToken calls because the in-process mutex + guard
// serialize and the second call sees a now-valid cached token.
func TestAuthManager_RefreshGuardSerializesRefresh(t *testing.T) {
	var mu sync.Mutex
	refreshCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		refreshCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accessToken":"refreshed","expiresIn":3600}`))
	}))
	defer srv.Close()

	past := time.Now().Add(-time.Hour)
	am := NewAuthManager(Credentials{
		RefreshToken: "rt",
		AccessToken:  "stale",
		Region:       "us-east-1",
		ExpiresAt:    &past,
	}, srv.Client())
	am.refreshURLOverride = srv.URL

	guardCalls := 0
	am.RefreshGuard = func(ctx context.Context, refresh func() error) error {
		guardCalls++
		return refresh()
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := am.AccessToken(context.Background()); err != nil {
				t.Errorf("AccessToken error: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	calls := refreshCalls
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected exactly 1 HTTP refresh under concurrency, got %d", calls)
	}
	if guardCalls != 1 {
		t.Fatalf("expected guard invoked once, got %d", guardCalls)
	}
}
