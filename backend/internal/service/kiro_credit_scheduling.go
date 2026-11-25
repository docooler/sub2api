package service

import (
	"context"
	"math"
	"sort"
	"time"
)

// Kiro 积分感知调度：同优先级的 Kiro 账号之间，优先调用积分使用百分比更低的账号，
// 让各席位的积分消耗趋于均衡（否则纯 LRU 轮转会让剩余积分少的账号和新账号
// 以同样的速度被消耗）。
//
// 数据来源是上游 GetUsageLimits（与账号页的积分用量展示同源）。调度热路径上
// 只做非阻塞的内存读取；数据过期时在后台异步刷新，绝不阻塞选号。
const (
	// kiroCreditSchedRefreshInterval 调度用积分样本的刷新间隔，同时也是失败后的
	// 重试退避（每个账号每个间隔最多向上游查询一次）。
	kiroCreditSchedRefreshInterval = 60 * time.Second
	// kiroCreditSchedMaxStaleness 样本超过该时长仍未刷新成功则视为未知，
	// 不再参与比较（fail-open，退回原有的负载率 → LRU 行为）。
	kiroCreditSchedMaxStaleness = 15 * time.Minute
	// kiroCreditBucketWidth 使用百分比的分桶宽度（百分点）。同一桶内的账号视为
	// 持平，继续按负载率 → LRU 分散，避免所有新会话都压到「恰好最低」的那一个
	// 账号上，也避免样本的微小抖动导致选号来回翻转。
	kiroCreditBucketWidth = 1.0
)

// KiroCreditUsageProvider 向调度器提供 Kiro 账号的积分使用百分比（0-100+）。
// 实现必须是非阻塞的；ok=false 表示暂时未知。
type KiroCreditUsageProvider interface {
	KiroCreditUtilization(account *Account) (utilization float64, ok bool)
}

// kiroCreditSample 最近一次成功获取的积分使用百分比。
type kiroCreditSample struct {
	utilization float64
	at          time.Time
}

// storeKiroCreditSample 记录一次成功的积分用量查询结果，供调度器读取。
func (c *UsageCache) storeKiroCreditSample(accountID int64, usage *UsageInfo, now time.Time) {
	if c == nil || usage == nil || usage.Error != "" || usage.KiroCredit == nil || usage.KiroCredit.UsageLimit <= 0 {
		return
	}
	c.kiroCreditSamples.Store(accountID, &kiroCreditSample{utilization: usage.KiroCredit.Utilization, at: now})
}

// KiroCreditUtilization 实现 KiroCreditUsageProvider：返回缓存的积分使用百分比，
// 样本缺失或过期时触发一次后台刷新。调用本身不做任何网络/数据库 IO。
func (s *AccountUsageService) KiroCreditUtilization(account *Account) (float64, bool) {
	if s == nil || s.cache == nil || account == nil || account.Platform != PlatformKiro {
		return 0, false
	}
	now := time.Now()
	var sample *kiroCreditSample
	if v, ok := s.cache.kiroCreditSamples.Load(account.ID); ok {
		sample, _ = v.(*kiroCreditSample)
	}
	if sample == nil || now.Sub(sample.at) >= kiroCreditSchedRefreshInterval {
		s.refreshKiroCreditAsync(account.ID, now)
	}
	if sample == nil || now.Sub(sample.at) > kiroCreditSchedMaxStaleness {
		return 0, false
	}
	return sample.utilization, true
}

// refreshKiroCreditAsync 后台刷新某账号的积分样本。每个账号每个刷新间隔最多
// 发起一次（无论成功失败），避免上游故障时按请求速率重试。
func (s *AccountUsageService) refreshKiroCreditAsync(accountID int64, now time.Time) {
	if s.kiroGatewayService == nil || s.accountRepo == nil {
		return
	}
	if v, ok := s.cache.kiroCreditAttempts.Load(accountID); ok {
		if last, _ := v.(time.Time); now.Sub(last) < kiroCreditSchedRefreshInterval {
			return
		}
	}
	s.cache.kiroCreditAttempts.Store(accountID, now)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// 调度快照里的账号可能不带完整凭据，这里按 ID 重新加载。
		account, err := s.accountRepo.GetByID(ctx, accountID)
		if err != nil || account == nil || account.Platform != PlatformKiro {
			return
		}
		// 成功时 getKiroUsage 会写入 kiroCreditSamples；失败则保留旧样本。
		_, _ = s.getKiroUsage(ctx, account, true)
	}()
}

// SetKiroCreditProvider 注入 Kiro 积分用量来源（wire 装配阶段调用）。
func (s *GatewayService) SetKiroCreditProvider(p KiroCreditUsageProvider) {
	s.kiroCreditProvider = p
}

// kiroCreditBucket 将使用百分比映射为分桶序号（越小表示用得越少）。
func kiroCreditBucket(utilization float64) int {
	if utilization <= 0 || math.IsNaN(utilization) {
		return 0
	}
	return int(math.Floor(utilization / kiroCreditBucketWidth))
}

// kiroCreditBucketOf 返回账号的积分分桶；ok=false 表示不参与比较
// （功能关闭、非 Kiro 账号或用量未知）。
func (s *GatewayService) kiroCreditBucketOf(account *Account) (int, bool) {
	if s == nil || s.kiroCreditProvider == nil || account == nil || account.Platform != PlatformKiro {
		return 0, false
	}
	utilization, ok := s.kiroCreditProvider.KiroCreditUtilization(account)
	if !ok {
		return 0, false
	}
	return kiroCreditBucket(utilization), true
}

// filterByLowestKiroCreditUsage 在（同优先级的）候选集合中，保留积分使用百分比
// 最低一档的账号。用量未知的账号视为与最低档持平而保留（fail-open）；
// 没有任何账号的用量已知时原样返回。
//
// softConcurrency > 0 时启用并发软上限，避免新会话全部压到用量最低的那一个账号：
// 当前并发已达到上限的账号暂不参与「低用量优先」，改从其余账号里选用量最低的；
// 所有账号都达到上限时原样返回，由后续的负载率 → LRU 在全部账号间均摊。
func (s *GatewayService) filterByLowestKiroCreditUsage(accounts []accountWithLoad, softConcurrency int) []accountWithLoad {
	if len(accounts) <= 1 || s == nil || s.kiroCreditProvider == nil {
		return accounts
	}
	buckets := make([]int, len(accounts))
	known := make([]bool, len(accounts))
	anyKnown := false
	for i, acc := range accounts {
		if bucket, ok := s.kiroCreditBucketOf(acc.account); ok {
			buckets[i], known[i], anyKnown = bucket, true, true
		}
	}
	if !anyKnown {
		return accounts
	}

	// 并发软上限：只在未达上限的账号里比较积分使用率。
	eligible := make([]bool, len(accounts))
	eligibleCount := 0
	for i, acc := range accounts {
		if softConcurrency <= 0 || acc.loadInfo == nil || acc.loadInfo.CurrentConcurrency < softConcurrency {
			eligible[i] = true
			eligibleCount++
		}
	}
	if eligibleCount == 0 {
		return accounts
	}

	minBucket, hasMin := 0, false
	for i := range accounts {
		if eligible[i] && known[i] && (!hasMin || buckets[i] < minBucket) {
			minBucket, hasMin = buckets[i], true
		}
	}
	result := make([]accountWithLoad, 0, eligibleCount)
	for i, acc := range accounts {
		if eligible[i] && (!known[i] || !hasMin || buckets[i] == minBucket) {
			result = append(result, acc)
		}
	}
	return result
}

// sortByKiroCreditUsageWithinPriority 对已排好序的账号做稳定重排：同优先级内
// 积分使用百分比低的排前面，同档账号保持原有相对顺序（LRU / 随机）。
// 用量未知的账号按该优先级内的最低档处理（与 filter 的 fail-open 语义一致）。
func (s *GatewayService) sortByKiroCreditUsageWithinPriority(accounts []*Account) {
	if len(accounts) <= 1 || s == nil || s.kiroCreditProvider == nil {
		return
	}
	buckets := make(map[int64]int, len(accounts))
	minByPriority := make(map[int]int)
	for _, acc := range accounts {
		bucket, ok := s.kiroCreditBucketOf(acc)
		if !ok {
			continue
		}
		buckets[acc.ID] = bucket
		if cur, seen := minByPriority[acc.Priority]; !seen || bucket < cur {
			minByPriority[acc.Priority] = bucket
		}
	}
	if len(buckets) == 0 {
		return
	}
	key := func(acc *Account) int {
		if bucket, ok := buckets[acc.ID]; ok {
			return bucket
		}
		return minByPriority[acc.Priority]
	}
	sort.SliceStable(accounts, func(i, j int) bool {
		a, b := accounts[i], accounts[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		return key(a) < key(b)
	})
}
