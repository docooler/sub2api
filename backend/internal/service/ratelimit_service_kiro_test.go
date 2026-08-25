//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type kiroRateLimitRepoStub struct {
	mockAccountRepoForGemini
	setRateLimitedCalls int
	lastRateLimitedID   int64
	lastResetAt         time.Time
	setErrorCalls       int
	lastErrorMsg        string
}

func (r *kiroRateLimitRepoStub) SetRateLimited(_ context.Context, id int64, resetAt time.Time) error {
	r.setRateLimitedCalls++
	r.lastRateLimitedID = id
	r.lastResetAt = resetAt
	return nil
}

func (r *kiroRateLimitRepoStub) SetError(_ context.Context, id int64, errorMsg string) error {
	r.setErrorCalls++
	r.lastErrorMsg = errorMsg
	return nil
}

func kiroTestAccount() *Account {
	return &Account{ID: 42, Platform: PlatformKiro, Type: AccountTypeAPIKey, Status: StatusActive}
}

func TestRateLimitService_HandleUpstreamError_Kiro402MonthlyCreditExhausted(t *testing.T) {
	repo := &kiroRateLimitRepoStub{}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	body := []byte(`{"message":"You have reached the limit.","reason":"MONTHLY_REQUEST_COUNT"}`)
	shouldDisable := service.HandleUpstreamError(context.Background(), kiroTestAccount(), 402, http.Header{}, body)

	require.True(t, shouldDisable)
	require.Equal(t, 1, repo.setRateLimitedCalls)
	require.Equal(t, int64(42), repo.lastRateLimitedID)
	require.Equal(t, 0, repo.setErrorCalls, "credit exhaustion must not permanently disable the account")

	// 重置点应为次月 1 日 00:00 UTC（Kiro 月度积分重置边界）
	nowUTC := time.Now().UTC()
	wantReset := time.Date(nowUTC.Year(), nowUTC.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	require.Equal(t, wantReset, repo.lastResetAt)
	require.True(t, repo.lastResetAt.After(time.Now()))
}

func TestRateLimitService_HandleUpstreamError_Kiro402DailyCreditExhausted(t *testing.T) {
	repo := &kiroRateLimitRepoStub{}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	body := []byte(`{"message":"You have reached the limit.","reason":"DAILY_REQUEST_COUNT"}`)
	shouldDisable := service.HandleUpstreamError(context.Background(), kiroTestAccount(), 402, http.Header{}, body)

	require.True(t, shouldDisable)
	require.Equal(t, 1, repo.setRateLimitedCalls)
	require.Equal(t, 0, repo.setErrorCalls)

	nowUTC := time.Now().UTC()
	wantReset := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	require.Equal(t, wantReset, repo.lastResetAt)
}

func TestRateLimitService_HandleUpstreamError_Kiro402UnknownReasonFallsBack(t *testing.T) {
	repo := &kiroRateLimitRepoStub{}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	// 非积分限额类 402（如订阅计费问题）仍走通用 handleAuthError 停调度
	body := []byte(`{"message":"Payment method declined","reason":"BILLING_ISSUE"}`)
	shouldDisable := service.HandleUpstreamError(context.Background(), kiroTestAccount(), 402, http.Header{}, body)

	require.True(t, shouldDisable)
	require.Equal(t, 0, repo.setRateLimitedCalls)
	require.Equal(t, 1, repo.setErrorCalls)
}
