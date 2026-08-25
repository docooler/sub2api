package kiro

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// AuthType identifies which token-refresh mechanism a Kiro account uses.
type AuthType string

const (
	// AuthKiroDesktop refreshes via https://prod.{region}.auth.desktop.kiro.dev/refreshToken
	// with JSON {"refreshToken": ...}.
	AuthKiroDesktop AuthType = "kiro_desktop"
	// AuthAWSSSOOIDC refreshes via https://oidc.{region}.amazonaws.com/token with
	// camelCase JSON {grantType, clientId, clientSecret, refreshToken}.
	AuthAWSSSOOIDC AuthType = "aws_sso_oidc"
)

// URL templates and tuning, mirrored from kiro-gateway config.py.
const (
	kiroRefreshURLTemplate = "https://prod.%s.auth.desktop.kiro.dev/refreshToken"
	awsSSOOIDCURLTemplate  = "https://oidc.%s.amazonaws.com/token"
	kiroAPIHostTemplate    = "https://runtime.%s.kiro.dev"
	kiroGenerateEndpoint   = "/generateAssistantResponse"

	// tokenRefreshThreshold: refresh when the token expires within this window.
	tokenRefreshThreshold = 600 * time.Second
	// tokenExpiryBuffer: subtracted from expiresIn so we refresh slightly early.
	tokenExpiryBuffer = 60 * time.Second

	kiroIDEVersion = "0.7.45"
)

var regionRE = regexp.MustCompile(`^[a-z]+-[a-z]+-\d+$`)

// Credentials holds the per-account secrets needed to talk to Kiro. The caller
// (sub2api) loads these from the account's credential map and persists any
// refreshed values back to the DB after a successful refresh.
type Credentials struct {
	RefreshToken string
	AccessToken  string // optional, may be pre-populated
	ProfileArn   string
	Region       string // API region (used for runtime.{region}.kiro.dev)
	SSORegion    string // SSO region for OIDC refresh; defaults to Region
	ClientID     string // AWS SSO OIDC only
	ClientSecret string // AWS SSO OIDC only
	ExpiresAt    *time.Time
}

// AuthType returns the detected auth type for these credentials.
func (c Credentials) AuthType() AuthType {
	if c.ClientID != "" && c.ClientSecret != "" {
		return AuthAWSSSOOIDC
	}
	return AuthKiroDesktop
}

// RefreshResult is returned to the caller after a successful refresh so it can
// persist the new values back to the account credentials.
type RefreshResult struct {
	AccessToken  string
	RefreshToken string
	ProfileArn   string
	ExpiresAt    time.Time
}

// AuthManager manages the access-token lifecycle for a single Kiro account.
// It caches the access token in memory and refreshes it on demand, guarding
// refreshes with a mutex. Refreshed credentials are surfaced via OnRefresh so
// the caller can persist them.
type AuthManager struct {
	mu          sync.Mutex
	creds       Credentials
	httpClient  *http.Client
	fingerprint string

	// refreshURLOverride, when non-empty, replaces the computed refresh endpoint.
	// Test seam only; production leaves it empty.
	refreshURLOverride string

	// OnRefresh, if set, is invoked (while holding the lock) with the refreshed
	// credentials whenever a refresh succeeds. Callers use it to persist tokens.
	OnRefresh func(RefreshResult)

	// RefreshGuard, if set, wraps each actual token refresh+writeback so callers
	// can serialize it across processes (e.g. a Redis distributed lock for
	// single-flight refresh). It is invoked while the in-process mutex is held;
	// it must call the provided refresh closure to perform the HTTP refresh, or
	// return ErrRefreshLockHeld to signal that another instance is refreshing
	// (in which case AuthManager reloads credentials via ReloadCreds instead).
	RefreshGuard func(ctx context.Context, refresh func() error) error

	// ReloadCreds, if set, reloads the latest persisted credentials (e.g. from
	// the DB) for this account. It is used when RefreshGuard reports the refresh
	// lock is held by another instance: that instance has likely already written
	// a fresh token, so AuthManager adopts it rather than refreshing again.
	ReloadCreds func(ctx context.Context) (Credentials, bool)
}

// ErrRefreshLockHeld is returned by a RefreshGuard when the distributed refresh
// lock is held by another instance; AuthManager then reloads credentials.
var ErrRefreshLockHeld = fmt.Errorf("kiro: refresh lock held by another instance")

// NewAuthManager builds an AuthManager. If httpClient is nil, http.DefaultClient
// is used. The region defaults to us-east-1 when unset.
func NewAuthManager(creds Credentials, httpClient *http.Client) *AuthManager {
	if creds.Region == "" {
		// Try to auto-detect API region from the profile ARN
		// (arn:aws:codewhisperer:REGION:account:profile/id).
		if r := regionFromARN(creds.ProfileArn); r != "" {
			creds.Region = r
		} else {
			creds.Region = "us-east-1"
		}
	}
	if creds.SSORegion == "" {
		creds.SSORegion = creds.Region
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &AuthManager{
		creds:       creds,
		httpClient:  httpClient,
		fingerprint: machineFingerprint(),
	}
}

// APIHost returns the runtime host for the configured API region.
func (a *AuthManager) APIHost() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return fmt.Sprintf(kiroAPIHostTemplate, a.creds.Region)
}

// GenerateURL returns the full generateAssistantResponse endpoint URL.
func (a *AuthManager) GenerateURL() string {
	return a.APIHost() + kiroGenerateEndpoint
}

// ProfileArn returns the current profile ARN.
func (a *AuthManager) ProfileArn() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.creds.ProfileArn
}

// Region returns the configured API region.
func (a *AuthManager) Region() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.creds.Region
}

// Fingerprint returns the machine fingerprint used for the User-Agent.
func (a *AuthManager) Fingerprint() string { return a.fingerprint }

// AccessToken returns a valid access token, refreshing if expired/expiring soon.
func (a *AuthManager) AccessToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.creds.AccessToken != "" && !a.isExpiringSoonLocked() {
		return a.creds.AccessToken, nil
	}
	if err := a.refreshLocked(ctx); err != nil {
		return "", err
	}
	if a.creds.AccessToken == "" {
		return "", fmt.Errorf("kiro: failed to obtain access token")
	}
	return a.creds.AccessToken, nil
}

// ForceRefresh forces a token refresh (used on a 403 from the API).
func (a *AuthManager) ForceRefresh(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.refreshLocked(ctx); err != nil {
		return "", err
	}
	return a.creds.AccessToken, nil
}

func (a *AuthManager) isExpiringSoonLocked() bool {
	if a.creds.ExpiresAt == nil {
		return true
	}
	return time.Now().Add(tokenRefreshThreshold).After(*a.creds.ExpiresAt)
}

// refreshLocked performs a token refresh under the in-process mutex, optionally
// serialized across instances by RefreshGuard. When the guard reports the lock
// is held elsewhere, it adopts freshly persisted credentials via ReloadCreds.
func (a *AuthManager) refreshLocked(ctx context.Context) error {
	if a.RefreshGuard == nil {
		return a.doRefreshLocked(ctx)
	}
	err := a.RefreshGuard(ctx, func() error { return a.doRefreshLocked(ctx) })
	if err == ErrRefreshLockHeld {
		// Another instance is/was refreshing: adopt its persisted token.
		if a.ReloadCreds != nil {
			if creds, ok := a.ReloadCreds(ctx); ok {
				a.adoptReloadedCredsLocked(creds)
			}
		}
		if a.creds.AccessToken != "" && !a.isExpiringSoonLocked() {
			return nil
		}
		// Reloaded token still stale/missing: fall back to a direct refresh.
		return a.doRefreshLocked(ctx)
	}
	return err
}

// adoptReloadedCredsLocked merges freshly reloaded token fields into the current
// credentials without clobbering static config (region/client_id/secret).
func (a *AuthManager) adoptReloadedCredsLocked(creds Credentials) {
	if creds.AccessToken != "" {
		a.creds.AccessToken = creds.AccessToken
	}
	if creds.RefreshToken != "" {
		a.creds.RefreshToken = creds.RefreshToken
	}
	if creds.ProfileArn != "" {
		a.creds.ProfileArn = creds.ProfileArn
	}
	if creds.ExpiresAt != nil {
		a.creds.ExpiresAt = creds.ExpiresAt
	}
}

func (a *AuthManager) doRefreshLocked(ctx context.Context) error {
	if a.creds.AuthType() == AuthAWSSSOOIDC {
		return a.refreshAWSSSOOIDCLocked(ctx)
	}
	return a.refreshKiroDesktopLocked(ctx)
}

func (a *AuthManager) refreshKiroDesktopLocked(ctx context.Context) error {
	if a.creds.RefreshToken == "" {
		return fmt.Errorf("kiro: refresh token is not set")
	}
	url := fmt.Sprintf(kiroRefreshURLTemplate, a.creds.SSORegion)
	if a.refreshURLOverride != "" {
		url = a.refreshURLOverride
	}
	body, _ := json.Marshal(map[string]string{"refreshToken": a.creds.RefreshToken})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("KiroIDE-%s-%s", kiroIDEVersion, a.fingerprint))

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kiro: desktop refresh failed: status %d", resp.StatusCode)
	}
	var data struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int    `json:"expiresIn"`
		ProfileArn   string `json:"profileArn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	if data.AccessToken == "" {
		return fmt.Errorf("kiro: desktop refresh response missing accessToken")
	}
	expiresIn := data.ExpiresIn
	if expiresIn == 0 {
		expiresIn = 3600
	}
	a.creds.AccessToken = data.AccessToken
	if data.RefreshToken != "" {
		a.creds.RefreshToken = data.RefreshToken
	}
	if data.ProfileArn != "" {
		a.creds.ProfileArn = data.ProfileArn
	}
	expiresAt := time.Now().Add(time.Duration(expiresIn)*time.Second - tokenExpiryBuffer)
	a.creds.ExpiresAt = &expiresAt
	a.emitRefreshLocked(expiresAt)
	return nil
}

func (a *AuthManager) refreshAWSSSOOIDCLocked(ctx context.Context) error {
	if a.creds.RefreshToken == "" {
		return fmt.Errorf("kiro: refresh token is not set")
	}
	if a.creds.ClientID == "" || a.creds.ClientSecret == "" {
		return fmt.Errorf("kiro: client_id/client_secret required for AWS SSO OIDC")
	}
	url := fmt.Sprintf(awsSSOOIDCURLTemplate, a.creds.SSORegion)
	body, _ := json.Marshal(map[string]string{
		"grantType":    "refresh_token",
		"clientId":     a.creds.ClientID,
		"clientSecret": a.creds.ClientSecret,
		"refreshToken": a.creds.RefreshToken,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kiro: AWS SSO OIDC refresh failed: status %d", resp.StatusCode)
	}
	var data struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int    `json:"expiresIn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	if data.AccessToken == "" {
		return fmt.Errorf("kiro: AWS SSO OIDC refresh response missing accessToken")
	}
	expiresIn := data.ExpiresIn
	if expiresIn == 0 {
		expiresIn = 3600
	}
	a.creds.AccessToken = data.AccessToken
	if data.RefreshToken != "" {
		a.creds.RefreshToken = data.RefreshToken
	}
	expiresAt := time.Now().Add(time.Duration(expiresIn)*time.Second - tokenExpiryBuffer)
	a.creds.ExpiresAt = &expiresAt
	a.emitRefreshLocked(expiresAt)
	return nil
}

func (a *AuthManager) emitRefreshLocked(expiresAt time.Time) {
	if a.OnRefresh == nil {
		return
	}
	a.OnRefresh(RefreshResult{
		AccessToken:  a.creds.AccessToken,
		RefreshToken: a.creds.RefreshToken,
		ProfileArn:   a.creds.ProfileArn,
		ExpiresAt:    expiresAt,
	})
}

// regionFromARN extracts the region from an AWS CodeWhisperer profile ARN.
// ARN format: arn:aws:codewhisperer:REGION:account:profile/id
func regionFromARN(arn string) string {
	if arn == "" {
		return ""
	}
	parts := strings.Split(arn, ":")
	if len(parts) >= 4 && regionRE.MatchString(parts[3]) {
		return parts[3]
	}
	return ""
}

// machineFingerprint mirrors utils.get_machine_fingerprint: a sha256 of
// "{hostname}-{username}-kiro-gateway".
func machineFingerprint() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	username := "unknown"
	if u := os.Getenv("USER"); u != "" {
		username = u
	} else if u := os.Getenv("USERNAME"); u != "" {
		username = u
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%s-kiro-gateway", hostname, username)))
	return hex.EncodeToString(sum[:])
}
