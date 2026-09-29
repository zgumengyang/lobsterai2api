package pool

import (
	"path/filepath"
	"testing"

	"lobsterai2api/internal/auth"
)

// 今日用量记账：单数/token 累加、余额下降算消耗、余额上升（签到）不算、重启后还在。
func TestUseStatAccounting(t *testing.T) {
	dir := t.TempDir()
	p := New(filepath.Join(dir, "state.json"))
	uid := "1001"
	p.byUID[uid] = &entry{a: &auth.Auth{UID: uid}, credits: 500}

	p.NoteRequest(uid, 12_000)
	p.NoteRequest(uid, 8_000)
	p.SetCredits(uid, 480)        // 掉 20 → 消耗 20
	p.ReenableIfCredits(uid, 570) // 涨 90（签到）→ 不算消耗
	p.ReenableIfCredits(uid, 560) // 掉 10 → 消耗 10

	u := p.UseSnapshot("")[uid]
	if u == nil {
		t.Fatal("没有记账行")
	}
	if u.Requests != 2 || u.Tokens != 20_000 || u.Burn != 30 {
		t.Fatalf("记账不对: %+v", u)
	}

	// 落盘 + 重启后读回
	p.saveUseLocked(true)
	p2 := New(filepath.Join(dir, "state.json"))
	u2 := p2.UseSnapshot("")[uid]
	if u2 == nil || u2.Requests != 2 || u2.Tokens != 20_000 || u2.Burn != 30 {
		t.Fatalf("重启后记账丢了: %+v", u2)
	}

	// 跨天自动清零
	p2.use[uid].Day = "2000-01-01"
	if got := p2.UseSnapshot(""); got[uid] != nil {
		t.Fatalf("跨天应清零，实际 %+v", got[uid])
	}
}
