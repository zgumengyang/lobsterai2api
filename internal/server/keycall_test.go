package server

import (
	"testing"
	"time"
)

// 「这个 token 在干什么」的记账：今日/累计/模型分布/最近明细 + 跨天清零 + 深拷贝
func TestNoteKeyCallAndSnapshot(t *testing.T) {
	h := &Handler{keyStats: map[string]*KeyUsage{"k1": {}}}
	now := time.Now()
	h.noteKeyCall("k1", KeyCall{At: now, Model: "glm-5.3", Source: "lobster", Tokens: 1200, OK: true})
	h.noteKeyCall("k1", KeyCall{At: now, Model: "glm-5.3", Source: "ch_x", Tokens: 800, OK: false, Note: "上游拒绝"})

	snap := h.keyStatsSnapshot()
	u := snap["k1"]
	if u.TodayCalls != 2 {
		t.Fatalf("今日调用数 = %d，期望 2", u.TodayCalls)
	}
	if u.TodayTokens != 2000 || u.TotalTokens != 2000 {
		t.Fatalf("token 记账不对: today=%d total=%d，期望都是 2000", u.TodayTokens, u.TotalTokens)
	}
	if u.Models["glm-5.3"] != 2 {
		t.Fatalf("模型分布不对: %v", u.Models)
	}
	if len(u.Calls) != 2 || u.Calls[1].Note != "上游拒绝" || u.Calls[1].OK {
		t.Fatalf("明细不对: %+v", u.Calls)
	}
	if u.Calls[0].Source != "lobster" || u.Calls[1].Source != "ch_x" {
		t.Fatalf("上游来源没记上: %+v", u.Calls)
	}

	// 快照必须是深拷贝：面板读的时候请求热路径还在 append，不能共享底层数组
	u.Calls[0].Model = "changed-by-caller"
	u.Models["glm-5.3"] = 999
	if h.keyStats["k1"].Calls[0].Model != "glm-5.3" || h.keyStats["k1"].Models["glm-5.3"] != 2 {
		t.Fatal("快照不是深拷贝（改了快照会污染内部状态）")
	}

	// 明细最多留 30 条
	for i := 0; i < 40; i++ {
		h.noteKeyCall("k1", KeyCall{At: now})
	}
	if got := len(h.keyStats["k1"].Calls); got != 30 {
		t.Fatalf("明细条数 = %d，期望上限 30", got)
	}

	// 跨天：今日计数自动清零，累计不动
	h.keyStats["k1"].TodayDay = "2000-01-01"
	before := h.keyStats["k1"].TotalTokens
	h.noteKeyCall("k1", KeyCall{At: now, Tokens: 100})
	if h.keyStats["k1"].TodayCalls != 1 || h.keyStats["k1"].TodayTokens != 100 {
		t.Fatalf("跨天没清零: calls=%d tokens=%d", h.keyStats["k1"].TodayCalls, h.keyStats["k1"].TodayTokens)
	}
	if h.keyStats["k1"].TotalTokens != before+100 {
		t.Fatal("跨天不该动累计 token")
	}

	// 没统计过的 key 不该被凭空建条目（面板内部探活请求不该造行）
	h.noteKeyCall("never-used", KeyCall{At: now})
	if _, ok := h.keyStats["never-used"]; ok {
		t.Fatal("不该给没统计过的 key 建条目")
	}
}
