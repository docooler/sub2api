package kiro

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	maxRetries     = 3
	baseRetryDelay = time.Second
)

// Client performs requests against the Kiro generateAssistantResponse endpoint
// with retry handling, mirroring kiro-gateway http_client.py.
type Client struct {
	auth       *AuthManager
	httpClient *http.Client
}

// NewClient builds a Client. If httpClient is nil, http.DefaultClient is used.
func NewClient(auth *AuthManager, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{auth: auth, httpClient: httpClient}
}

// kiroHeaders builds the request headers, mirroring utils.get_kiro_headers.
func kiroHeaders(fingerprint, token string) http.Header {
	ua := fmt.Sprintf("aws-sdk-js/1.0.27 ua/2.1 os/win32#10.0.19044 lang/js md/nodejs#22.21.1 api/codewhispererstreaming#1.0.27 m/E KiroIDE-%s-%s", kiroIDEVersion, fingerprint)
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	h.Set("Content-Type", "application/x-amz-json-1.0")
	h.Set("x-amz-target", "AmazonCodeWhispererStreamingService.GenerateAssistantResponse")
	h.Set("User-Agent", ua)
	h.Set("x-amz-user-agent", fmt.Sprintf("aws-sdk-js/1.0.27 KiroIDE-%s-%s", kiroIDEVersion, fingerprint))
	h.Set("x-amzn-codewhisperer-optout", "true")
	h.Set("x-amzn-kiro-agent-mode", "vibe")
	h.Set("amz-sdk-invocation-id", uuid.NewString())
	h.Set("amz-sdk-request", "attempt=1; max=3")
	return h
}

// GenerateAssistantResponse POSTs the given payload and returns the raw HTTP
// response (caller is responsible for reading/closing the body and parsing the
// stream with Parser). Retries on 403 (refresh token), 429 and 5xx (backoff).
//
// On success the returned *http.Response has status 200 and an open Body. On a
// non-retryable / exhausted error, it returns the last response (so the caller
// can inspect status/body) and a nil error, or a non-nil error for transport
// failures.
func (c *Client) GenerateAssistantResponse(ctx context.Context, payload map[string]any) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("kiro: marshal payload: %w", err)
	}
	url := c.auth.GenerateURL()

	var lastResp *http.Response
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		token, err := c.auth.AccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("kiro: access token: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header = kiroHeaders(c.auth.Fingerprint(), token)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				if waitErr := sleepCtx(ctx, backoff(attempt)); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			break
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			return resp, nil
		case resp.StatusCode == http.StatusForbidden:
			resp.Body.Close()
			if _, err := c.auth.ForceRefresh(ctx); err != nil {
				lastErr = fmt.Errorf("kiro: token refresh after 403: %w", err)
			}
			continue
		case resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode < 600):
			if lastResp != nil {
				lastResp.Body.Close()
			}
			lastResp = resp
			if attempt < maxRetries-1 {
				if waitErr := sleepCtx(ctx, backoff(attempt)); waitErr != nil {
					return nil, waitErr
				}
			}
			continue
		default:
			// Other errors: return as-is for caller classification.
			return resp, nil
		}
	}

	if lastResp != nil {
		return lastResp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("kiro: request failed after %d attempts", maxRetries)
}

func backoff(attempt int) time.Duration {
	return baseRetryDelay * time.Duration(1<<uint(attempt))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
