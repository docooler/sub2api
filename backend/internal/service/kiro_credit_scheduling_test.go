//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubKiroCreditProvider 以固定的 accountID -> 使用百分比 表作为数据源；
// 表中不存在的账号视为用量未知。
type stubKiroCreditProvider map[int64]float64

func (p stubKiroCreditProvider) KiroCreditUtilization(account *Account) (float64, bool) {
	v, ok := p[account.ID]
	return v, ok
}

func kiroAccWithLoad(id int64, priority int) accountWithLoad {
	return accountWithLoad{
		account:  &Account{ID: id, Platform: PlatformKiro, Priority: priority, Schedulable: true, Status: StatusActive},
		loadInfo: &AccountLoadInfo{AccountID: id},
	}
}

func idsOfLoads(accounts []accountWithLoad) []int64 {
	ids := make([]int64, 0, len(accounts))
	for _, acc := range accounts {
		ids = append(ids, acc.account.ID)
	}
	return ids
}

func idsOfAccounts(accounts []*Account) []int64 {
	ids := make([]int64, 0, len(accounts))
	for _, acc := range accounts {
		ids = append(ids, acc.ID)
	}
	return ids
}

func TestFilterByLowestKiroCreditUsage_PicksLowestUtilization(t *testing.T) {
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 80.4, 2: 1.6, 3: 45}}
	got := s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithLoad(1, 1), kiroAccWithLoad(2, 1), kiroAccWithLoad(3, 1)}, 0)
	require.Equal(t, []int64{2}, idsOfLoads(got))
}

func TestFilterByLowestKiroCreditUsage_SameBucketIsATie(t *testing.T) {
	// 12.1% 与 12.9% 落在同一个 1% 桶内，应同时保留，交给后续负载率 / LRU 分散。
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 12.9, 2: 12.1, 3: 13.0}}
	got := s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithLoad(1, 1), kiroAccWithLoad(2, 1), kiroAccWithLoad(3, 1)}, 0)
	require.Equal(t, []int64{1, 2}, idsOfLoads(got))
}

func TestFilterByLowestKiroCreditUsage_UnknownIsKept(t *testing.T) {
	// 账号 3 用量未知：fail-open，与最低档一起保留，而不是被饿死或独占流量。
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 90, 2: 10}}
	got := s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithLoad(1, 1), kiroAccWithLoad(2, 1), kiroAccWithLoad(3, 1)}, 0)
	require.Equal(t, []int64{2, 3}, idsOfLoads(got))
}

func TestFilterByLowestKiroCreditUsage_NoDataOrNoProviderIsNoop(t *testing.T) {
	accounts := []accountWithLoad{kiroAccWithLoad(1, 1), kiroAccWithLoad(2, 1)}

	noData := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{}}
	require.Equal(t, []int64{1, 2}, idsOfLoads(noData.filterByLowestKiroCreditUsage(accounts, 0)))

	noProvider := &GatewayService{}
	require.Equal(t, []int64{1, 2}, idsOfLoads(noProvider.filterByLowestKiroCreditUsage(accounts, 0)))

	require.Empty(t, noData.filterByLowestKiroCreditUsage(nil, 0))
}

func TestFilterByLowestKiroCreditUsage_IgnoresNonKiroAccounts(t *testing.T) {
	// 非 Kiro 账号即便 provider 有数据也不参与比较（视为未知 → 保留）。
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 99, 2: 50, 3: 10}}
	other := kiroAccWithLoad(1, 1)
	other.account.Platform = PlatformAnthropic
	got := s.filterByLowestKiroCreditUsage([]accountWithLoad{other, kiroAccWithLoad(2, 1), kiroAccWithLoad(3, 1)}, 0)
	require.Equal(t, []int64{1, 3}, idsOfLoads(got))
}

func kiroAccWithConcurrency(id int64, current int) accountWithLoad {
	acc := kiroAccWithLoad(id, 1)
	acc.loadInfo.CurrentConcurrency = current
	acc.loadInfo.LoadRate = current * 10
	return acc
}

func TestFilterByLowestKiroCreditUsage_SoftConcurrencyCap(t *testing.T) {
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 2, 2: 40, 3: 70}}

	// 最低用量账号并发未达上限：仍然选它。
	got := s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 2), kiroAccWithConcurrency(2, 0), kiroAccWithConcurrency(3, 0)}, 3)
	require.Equal(t, []int64{1}, idsOfLoads(got))

	// 最低用量账号并发已达上限：改选其余账号里用量最低的。
	got = s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 3), kiroAccWithConcurrency(2, 0), kiroAccWithConcurrency(3, 0)}, 3)
	require.Equal(t, []int64{2}, idsOfLoads(got))

	// 前两个都达到上限：轮到第三个。
	got = s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 5), kiroAccWithConcurrency(2, 3), kiroAccWithConcurrency(3, 1)}, 3)
	require.Equal(t, []int64{3}, idsOfLoads(got))

	// 全部达到上限：不再按积分筛选，交给负载率 → LRU 在全部账号间均摊。
	got = s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 4), kiroAccWithConcurrency(2, 3), kiroAccWithConcurrency(3, 6)}, 3)
	require.Equal(t, []int64{1, 2, 3}, idsOfLoads(got))

	// 上限为 0 表示不限制。
	got = s.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 9), kiroAccWithConcurrency(2, 0), kiroAccWithConcurrency(3, 0)}, 0)
	require.Equal(t, []int64{1}, idsOfLoads(got))

	// 用量未知且未达上限的账号与最低档一起保留。
	unknown := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 2, 2: 40}}
	got = unknown.filterByLowestKiroCreditUsage([]accountWithLoad{kiroAccWithConcurrency(1, 3), kiroAccWithConcurrency(2, 0), kiroAccWithConcurrency(3, 0)}, 3)
	require.Equal(t, []int64{2, 3}, idsOfLoads(got))
}

func TestSortByKiroCreditUsageWithinPriority(t *testing.T) {
	s := &GatewayService{kiroCreditProvider: stubKiroCreditProvider{1: 70, 2: 20, 3: 20.5, 4: 5, 5: 60}}
	accounts := []*Account{
		{ID: 1, Platform: PlatformKiro, Priority: 1},
		{ID: 2, Platform: PlatformKiro, Priority: 1},
		{ID: 3, Platform: PlatformKiro, Priority: 1},
		{ID: 6, Platform: PlatformKiro, Priority: 1}, // 未知 → 按该优先级最低档处理
		{ID: 5, Platform: PlatformKiro, Priority: 2},
		{ID: 4, Platform: PlatformKiro, Priority: 2},
	}
	s.sortByKiroCreditUsageWithinPriority(accounts)
	// 优先级仍是第一排序键；同优先级内低使用率在前，同档（2、3、6）保持原相对顺序。
	require.Equal(t, []int64{2, 3, 6, 1, 4, 5}, idsOfAccounts(accounts))
}

func TestKiroCreditBucket(t *testing.T) {
	require.Equal(t, 0, kiroCreditBucket(0))
	require.Equal(t, 0, kiroCreditBucket(-3))
	require.Equal(t, 0, kiroCreditBucket(0.99))
	require.Equal(t, 1, kiroCreditBucket(1))
	require.Equal(t, 80, kiroCreditBucket(80.45))
	require.Equal(t, 120, kiroCreditBucket(120.2))
}

func TestAccountUsageService_KiroCreditUtilization(t *testing.T) {
	cache := NewUsageCache()
	// kiroGatewayService 为 nil：不会触发后台刷新，只验证读取语义。
	svc := &AccountUsageService{cache: cache}
	kiroAcc := &Account{ID: 7, Platform: PlatformKiro}
	now := time.Now()

	_, ok := svc.KiroCreditUtilization(kiroAcc)
	require.False(t, ok, "no sample yet")

	usage := &UsageInfo{KiroCredit: &KiroCreditUsage{CurrentUsage: 250, UsageLimit: 1000, Utilization: 25}}
	cache.storeKiroCreditSample(7, usage, now)
	got, ok := svc.KiroCreditUtilization(kiroAcc)
	require.True(t, ok)
	require.InDelta(t, 25.0, got, 1e-9)

	// 失败/无额度的结果不能覆盖上一次成功的样本。
	cache.storeKiroCreditSample(7, &UsageInfo{Error: "upstream 500"}, now)
	cache.storeKiroCreditSample(7, &UsageInfo{KiroCredit: &KiroCreditUsage{}}, now)
	got, ok = svc.KiroCreditUtilization(kiroAcc)
	require.True(t, ok)
	require.InDelta(t, 25.0, got, 1e-9)

	// 过期样本视为未知。
	cache.storeKiroCreditSample(7, usage, now.Add(-kiroCreditSchedMaxStaleness-time.Minute))
	_, ok = svc.KiroCreditUtilization(kiroAcc)
	require.False(t, ok, "stale sample must be ignored")

	// 非 Kiro 账号不参与。
	cache.storeKiroCreditSample(8, usage, now)
	_, ok = svc.KiroCreditUtilization(&Account{ID: 8, Platform: PlatformAnthropic})
	require.False(t, ok)
}

// 端到端验证 Layer 2 的选择顺序：优先级 → Kiro 积分使用率 → 负载率 → LRU。
func TestGatewayService_SelectAccountWithLoadAwareness_KiroCreditUsage(t *testing.T) {
	ctx := context.Background()
	groupID := int64(31)
	old := time.Now().Add(-2 * time.Hour)
	recent := time.Now().Add(-1 * time.Minute)

	build := func(accounts []Account, provider KiroCreditUsageProvider, enabled bool) *GatewayService {
		repo := &mockAccountRepoForPlatform{accounts: accounts, accountsByID: map[int64]*Account{}}
		for i := range repo.accounts {
			repo.accountsByID[repo.accounts[i].ID] = &repo.accounts[i]
		}
		cfg := testConfig()
		cfg.Gateway.Scheduling.LoadBatchEnabled = true
		cfg.Gateway.Scheduling.PreferLowCreditUsage = enabled
		return &GatewayService{
			accountRepo: repo,
			groupRepo: &mockGroupRepoForGateway{groups: map[int64]*Group{
				groupID: {ID: groupID, Platform: PlatformKiro, Status: StatusActive, Hydrated: true},
			}},
			cache: &mockGatewayCacheForPlatform{},
			cfg:   cfg,
			concurrencyService: NewConcurrencyService(&mockConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{
				1: {AccountID: 1, LoadRate: 0},
				2: {AccountID: 2, LoadRate: 40},
			}}),
			kiroCreditProvider: provider,
		}
	}
	// 账号 1：最久未用且空闲（原算法必选它），但积分已用 80%。
	// 账号 2：刚用过且负载更高，但积分只用了 2%。
	accounts := func() []Account {
		return []Account{
			{ID: 1, Platform: PlatformKiro, Type: AccountTypeAPIKey, Priority: 1, Status: StatusActive, Schedulable: true, Concurrency: 10, LastUsedAt: &old},
			{ID: 2, Platform: PlatformKiro, Type: AccountTypeAPIKey, Priority: 1, Status: StatusActive, Schedulable: true, Concurrency: 10, LastUsedAt: &recent},
		}
	}
	usage := stubKiroCreditProvider{1: 80, 2: 2}

	t.Run("同优先级-积分使用率低的优先", func(t *testing.T) {
		svc := build(accounts(), usage, true)
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(2), result.Account.ID)
	})

	t.Run("开关关闭-保持原有负载率与LRU顺序", func(t *testing.T) {
		svc := build(accounts(), usage, false)
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Account.ID)
	})

	t.Run("用量未知-退回原有顺序", func(t *testing.T) {
		svc := build(accounts(), stubKiroCreditProvider{}, true)
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Account.ID)
	})

	t.Run("优先级仍然高于积分使用率", func(t *testing.T) {
		accs := accounts()
		accs[0].Priority = 0 // 账号 1 优先级更高，即便积分用得多也先用它
		svc := build(accs, usage, true)
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Account.ID)
	})

	t.Run("低使用率账号并发达到软上限-改选其他账号", func(t *testing.T) {
		svc := build(accounts(), usage, true)
		svc.cfg.Gateway.Scheduling.CreditUsageSoftConcurrency = 3
		svc.concurrencyService = NewConcurrencyService(&mockConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{
			1: {AccountID: 1, CurrentConcurrency: 0, LoadRate: 0},
			2: {AccountID: 2, CurrentConcurrency: 3, LoadRate: 30},
		}})
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Account.ID)

		// 并发 2（未达上限）时仍优先低使用率账号。
		svc.concurrencyService = NewConcurrencyService(&mockConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{
			1: {AccountID: 1, CurrentConcurrency: 0, LoadRate: 0},
			2: {AccountID: 2, CurrentConcurrency: 2, LoadRate: 20},
		}})
		result, err = svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", nil, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(2), result.Account.ID)
	})

	t.Run("低使用率账号被排除时回退到次低", func(t *testing.T) {
		svc := build(accounts(), usage, true)
		result, err := svc.SelectAccountWithLoadAwareness(ctx, &groupID, "", "claude-sonnet-4-5", map[int64]struct{}{2: {}}, "", int64(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Account.ID)
	})
}
