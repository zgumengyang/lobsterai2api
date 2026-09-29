package pool

import (
	"testing"
	"time"

	"lobsterai2api/internal/auth"
)

func newTestPool(uids ...string) *Pool {
	p := &Pool{byUID: map[string]*entry{}}
	for _, u := range uids {
		p.byUID[u] = &entry{a: &auth.Auth{UID: u}, credits: 300}
	}
	return p
}

// 用户 2026-09-26 要求：不再轮询 —— 按账号顺序用，**一直用顺序上的第一个号**
// （把它的积分用完），它没了（0 分/冷却/禁用）才自动切下一个。
func TestPickUsesFirstAccountInOrderUntilItIsGone(t *testing.T) {
	p := newTestPool("94981", "96546", "96686")
	for i := 0; i < 20; i++ {
		a := p.Pick() // hold=false = 上一个请求已经结束
		if a == nil {
			t.Fatal("Pick 返回 nil，池里明明有 3 个可用账号")
		}
		if a.UID != "94981" {
			t.Fatalf("第 %d 次选了 %s，期望一直用顺序第一个 94981", i, a.UID)
		}
	}
	// 第一个号用完（上游确认无积分 → credits 归零）→ 自动切到第二个
	p.byUID["94981"].credits = 0
	if a := p.Pick(); a == nil || a.UID != "96546" {
		t.Fatalf("第一个号 0 分后选了 %v，期望切到 96546", a)
	}
	// 第二个号被限流冷却 → 自动切到第三个
	p.byUID["96546"].until = time.Now().Add(time.Hour)
	if a := p.Pick(); a == nil || a.UID != "96686" {
		t.Fatalf("第二个号冷却后选了 %v，期望切到 96686", a)
	}
}

// 顺序口径 = 账号首次登录时间（auth.FirstKeyfrom）升序，不是 uid 顺序。
// 这条锁住"面板显示顺序 == 选号顺序"。
func TestPickOrderFollowsFirstLoginTime(t *testing.T) {
	p := newTestPool("300", "100", "200")  // uid 顺序故意跟时间顺序不一致
	p.byUID["300"].a.FirstKeyfrom = "1000" // 最早登录（uid 最大，登录最早）
	p.byUID["200"].a.FirstKeyfrom = "2000"
	p.byUID["100"].a.FirstKeyfrom = "3000" // 最晚登录（uid 最小，登录最晚）
	want := []string{"300", "200", "100"}
	for i, exp := range want {
		a := p.Pick()
		if a == nil || a.UID != exp {
			t.Fatalf("第 %d 个用了 %v，期望 %s（按首次登录时间升序）", i+1, a, exp)
		}
		p.byUID[exp].credits = 0 // 用完这个号，看是否切到下一个
	}
	// 三个号都 0 分时不该返回 nil：pick 的既有兜底语义是"没有正分号 → 0 分号也进候选"
	// （见 TestHasPickableMirrorsPick）。此时仍按时间升序取第一个 → 300（登录最早）。
	if a := p.Pick(); a == nil || a.UID != "300" {
		t.Fatalf("全 0 分时应按时间顺序取第一个（300），实际 %v", a)
	}
}

// 并发请求也要压在**同一个号**上（顺序用的语义：先把第一个号的积分用光）。
// 2026-09-21 那版是按 inflight 摊开的，2026-09-26 用户明确改回"顺序用"。
func TestPickKeepsConcurrentRequestsOnSameAccount(t *testing.T) {
	p := newTestPool("a", "b", "c", "d")
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		a := p.PickExcluding(map[string]bool{}) // hold=true，占住并发位不释放
		if a == nil {
			t.Fatal("PickExcluding 返回 nil")
		}
		seen[a.UID] = true
	}
	if len(seen) != 1 {
		t.Fatalf("3 个并发请求落到了 %d 个账号上：%v（顺序用时应压在同一个号）", len(seen), seen)
	}
	if !seen["a"] {
		t.Fatalf("顺序第一个是 a，实际落到 %v", seen)
	}
}

// 有正分账号时，0 分账号永不参与选号。
func TestPickSkipsZeroCreditWhenPositiveExists(t *testing.T) {
	p := newTestPool("zero")
	p.byUID["zero"].credits = 0
	p.byUID["rich"] = &entry{a: &auth.Auth{UID: "rich"}, credits: 100}

	for i := 0; i < 10; i++ {
		a := p.Pick()
		if a == nil || a.UID != "rich" {
			t.Fatalf("第 %d 次选了 %v，期望 rich（0 分账号必须被跳过）", i, a)
		}
	}
}

// tried 里的账号必须跳过；都试完了返回 nil；Release 能正确归还并发位。
func TestPickExcludingSkipsTriedAndReleases(t *testing.T) {
	p := newTestPool("a", "b", "c")

	a := p.PickExcluding(map[string]bool{"a": true, "b": true})
	if a == nil || a.UID != "c" {
		t.Fatalf("选了 %v，期望 c", a)
	}
	if got := p.byUID["c"].inflight; got != 1 {
		t.Fatalf("占位后 inflight=%d，期望 1", got)
	}
	p.Release("c")
	if got := p.byUID["c"].inflight; got != 0 {
		t.Fatalf("Release 后 inflight=%d，期望 0", got)
	}

	if got := p.PickExcluding(map[string]bool{"a": true, "b": true, "c": true}); got != nil {
		t.Fatalf("全部 tried 后应返回 nil，实际 %v", got)
	}
}

// 全池冷却时没有可用账号，必须返回 nil（调用方转 503），不能硬塞一个死号。
func TestPickReturnsNilWhenAllCooling(t *testing.T) {
	p := newTestPool("a", "b")
	p.byUID["a"].disabled = true
	p.byUID["b"].until = time.Now().Add(time.Hour)
	if got := p.Pick(); got != nil {
		t.Fatalf("全池不可用时应返回 nil，实际 %v", got)
	}
}

// TestHasPickableMirrorsPick 不可分离：HasPickable 是 handler
// “池空 → 改走反代”的唯一触发器（双向互通）。
// 两者口径一旦分歧，要么在有号时偷跑流量到反代，
// 要么在真空时报 503 而不去找可用渠道。
func TestHasPickableMirrorsPick(t *testing.T) {
	p := newTestPool("a", "b") // 默认 credits=300、未禁用、未冷却
	if !p.HasPickable() {
		t.Fatal("HasPickable() = false，池里明明有 2 个可用号")
	}
	if p.Pick() == nil {
		t.Fatal("Pick() = nil，但 HasPickable() = true —— 口径已分歧")
	}

	// 全部禁用（封号）→ 真空
	for _, e := range p.byUID {
		e.disabled = true
	}
	if p.HasPickable() {
		t.Fatal("HasPickable() = true，但全部账号已禁用")
	}
	if p.Pick() != nil {
		t.Fatal("Pick() != nil，但全部账号已禁用 —— 口径已分歧")
	}

	// 全部冷却中 → 真空
	p2 := newTestPool("c")
	for _, e := range p2.byUID {
		e.until = time.Now().Add(time.Hour)
	}
	if p2.HasPickable() {
		t.Fatal("HasPickable() = true，但卡在冷却期内")
	}

	// 全部 0 分（但未禁用未冷却）→ 仍应可选
	// （pick() 的语义：没有正分号时，0 分号也进入候选）
	p3 := newTestPool("d")
	for _, e := range p3.byUID {
		e.credits = 0
	}
	if !p3.HasPickable() {
		t.Fatal("HasPickable() = false，但全 0 分时 0 分号应参与选号")
	}
	if p3.Pick() == nil {
		t.Fatal("Pick() = nil，但 HasPickable() = true —— 口径已分歧")
	}
}
