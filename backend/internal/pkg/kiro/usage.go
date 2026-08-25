package kiro

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// GetUsageLimits queries the account's credit/quota consumption. Unlike
// generation, this API lives on the CodeWhisperer service host
// (codewhisperer.{region}.amazonaws.com) rather than runtime.{region}.kiro.dev,
// and is addressed via the X-Amz-Target header — the same convention as
// ListAvailableProfiles.
const (
	usageLimitsHostTemplate = "https://codewhisperer.%s.amazonaws.com/"
	usageLimitsTarget       = "AmazonCodeWhispererService.GetUsageLimits"
)

// UsageBreakdown is one entry of usageBreakdownList in the GetUsageLimits
// response. For Kiro subscriptions the interesting entry is
// resourceType=="CREDIT".
type UsageBreakdown struct {
	ResourceType           string  `json:"resourceType"`
	DisplayName            string  `json:"displayName"`
	Unit                   string  `json:"unit"`
	Currency               string  `json:"currency"`
	CurrentUsage           float64 `json:"currentUsage"`
	CurrentUsagePrecise    float64 `json:"currentUsageWithPrecision"`
	UsageLimit             float64 `json:"usageLimit"`
	UsageLimitPrecise      float64 `json:"usageLimitWithPrecision"`
	CurrentOverages        float64 `json:"currentOverages"`
	CurrentOveragesPrecise float64 `json:"currentOveragesWithPrecision"`
	OverageCap             float64 `json:"overageCap"`
	OverageCapPrecise      float64 `json:"overageCapWithPrecision"`
	OverageCharges         float64 `json:"overageCharges"`
	OverageRate            float64 `json:"overageRate"`
	NextDateReset          float64 `json:"nextDateReset"` // epoch seconds
}

// BestCurrentUsage prefers the WithPrecision value when present.
func (b UsageBreakdown) BestCurrentUsage() float64 {
	if b.CurrentUsagePrecise > 0 {
		return b.CurrentUsagePrecise
	}
	return b.CurrentUsage
}

// BestUsageLimit prefers the WithPrecision value when present.
func (b UsageBreakdown) BestUsageLimit() float64 {
	if b.UsageLimitPrecise > 0 {
		return b.UsageLimitPrecise
	}
	return b.UsageLimit
}

// UsageLimitsResponse mirrors the GetUsageLimits response fields sub2api
// surfaces. Unknown fields are ignored.
type UsageLimitsResponse struct {
	DaysUntilReset int     `json:"daysUntilReset"`
	NextDateReset  float64 `json:"nextDateReset"` // epoch seconds

	SubscriptionInfo struct {
		SubscriptionTitle string `json:"subscriptionTitle"`
		Type              string `json:"type"`
	} `json:"subscriptionInfo"`

	OverageConfiguration struct {
		OverageStatus string `json:"overageStatus"` // e.g. ENABLED / DISABLED
	} `json:"overageConfiguration"`

	UsageBreakdownList []UsageBreakdown `json:"usageBreakdownList"`

	UserInfo struct {
		Email string `json:"email"`
	} `json:"userInfo"`
}

// CreditBreakdown returns the CREDIT usage breakdown, falling back to the
// first entry when the upstream ever renames the resource type.
func (r *UsageLimitsResponse) CreditBreakdown() *UsageBreakdown {
	for i := range r.UsageBreakdownList {
		if r.UsageBreakdownList[i].ResourceType == "CREDIT" {
			return &r.UsageBreakdownList[i]
		}
	}
	if len(r.UsageBreakdownList) > 0 {
		return &r.UsageBreakdownList[0]
	}
	return nil
}

// GetUsageLimits fetches the account's usage limits. A 403 triggers one forced
// token refresh + retry (same semantics as GenerateAssistantResponse); other
// non-200 statuses are returned as errors with the (truncated) upstream body.
func (c *Client) GetUsageLimits(ctx context.Context) (*UsageLimitsResponse, error) {
	payload := map[string]any{"isEmailRequired": true}
	if arn := c.auth.ProfileArn(); arn != "" {
		payload["profileArn"] = arn
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("kiro: marshal usage payload: %w", err)
	}
	url := fmt.Sprintf(usageLimitsHostTemplate, c.auth.Region())

	refreshed := false
	for {
		token, err := c.auth.AccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("kiro: access token: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		h := kiroHeaders(c.auth.Fingerprint(), token)
		h.Set("x-amz-target", usageLimitsTarget)
		req.Header = h

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("kiro: usage request failed: %w", err)
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		resp.Body.Close()

		if resp.StatusCode == http.StatusForbidden && !refreshed {
			refreshed = true
			if _, err := c.auth.ForceRefresh(ctx); err != nil {
				return nil, fmt.Errorf("kiro: token refresh after 403: %w", err)
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("kiro: usage limits upstream status %d: %s", resp.StatusCode, truncateForError(respBody))
		}
		if readErr != nil {
			return nil, fmt.Errorf("kiro: read usage response: %w", readErr)
		}

		var out UsageLimitsResponse
		if err := json.Unmarshal(respBody, &out); err != nil {
			return nil, fmt.Errorf("kiro: parse usage response: %w", err)
		}
		return &out, nil
	}
}

func truncateForError(b []byte) string {
	const max = 512
	if len(b) > max {
		b = b[:max]
	}
	return string(b)
}
