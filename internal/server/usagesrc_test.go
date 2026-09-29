package server

import (
	"testing"
	"time"
)

// 按上游记账的关键不变量：所有来源相加 == 总计（池子 + 各反代）。
// 这条就是用户要的"这些所有的都要记录到总的今日 token 和累计消耗"。
func TestUsageBySourceSumsToTotal(t *testing.T) {
	h := &Handler{}
	u := func(prompt, completion, total float64) map[string]any {
		return map[string]any{
			"prompt_tokens":     prompt,
			"completion_tokens": completion,
			"total_tokens":      total,
		}
	}
	// 池子 2 单 + Tier 1 单 + dsds 1 单
	h.noteUsageFrom("lobster", u(100, 10, 110))
	h.noteUsageFrom("lobster", u(200, 20, 220))
	h.noteUsageFrom("ch_tier", u(30, 3, 33))
	h.noteUsageFrom("ch_dsds", u(40, 4, 44))
	// 空 src 应归到 lobster
	h.noteUsageFrom("", u(5, 1, 6))

	var sumTotal, sumReq int64
	for _, s := range h.usageSrc {
		sumTotal += s.TotalTokens
		sumReq += s.Requests
	}
	if sumTotal != h.usage.TotalTokens {
		t.Fatalf("各来源 token 之和 %d != 总计 %d", sumTotal, h.usage.TotalTokens)
	}
	if sumReq != h.usage.Requests {
		t.Fatalf("各来源单数之和 %d != 总计 %d", sumReq, h.usage.Requests)
	}
	if h.usage.TotalTokens != 413 { // 110+220+33+44+6
		t.Fatalf("总计应为 413，实际 %d", h.usage.TotalTokens)
	}
	if got := h.usageSrc["ch_tier"].TotalTokens; got != 33 {
		t.Fatalf("Tier 应为 33，实际 %d", got)
	}
	if got := h.usageSrc["lobster"].Requests; got != 3 {
		t.Fatalf("池子应为 3 单（含空 src 那条），实际 %d", got)
	}

	// 按天同样成立
	day := time.Now().Format("2006-01-02")
	var daySum int64
	for _, s := range h.usageSrcDaily[day] {
		daySum += s.TotalTokens
	}
	if daySum != h.usageDaily[day].TotalTokens {
		t.Fatalf("当天各来源之和 %d != 当天总计 %d", daySum, h.usageDaily[day].TotalTokens)
	}

	// nil usage 不该记账
	before := h.usage.Requests
	h.noteUsageFrom("ch_tier", nil)
	if h.usage.Requests != before {
		t.Fatal("nil usage 不该计数")
	}
}
