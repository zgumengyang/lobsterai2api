package server

import (
	"testing"
	"time"
)

// 每日额度是硬闸门：用完直接拒（不排队）；跨天自动清零；strict 让每分钟限速也不排队。
func TestDailyQuotaAndStrict(t *testing.T) {
	h := &Handler{}
	h.loadLimits() // limitsFile 为空 → 纯内存态

	// ① 每日上限 2
	h.limitDaily["k"] = 2
	for i := 1; i <= 2; i++ {
		if ok, kind, _ := h.allowKey("k"); !ok {
			t.Fatalf("第 %d 次应放行，实际被拒 kind=%s", i, kind)
		}
	}
	if ok, kind, limit := h.allowKey("k"); ok || kind != "daily_quota" || limit != 2 {
		t.Fatalf("超额度应直接拒: ok=%v kind=%s limit=%d", ok, kind, limit)
	}
	// ② 跨天自动清零
	h.limitUsedDay = "2000-01-01"
	if ok, kind, _ := h.allowKey("k"); !ok {
		t.Fatalf("跨天后应重新放行，实际 kind=%s", kind)
	}

	// ③ strict：rpm=1，第二次直接拒且不等待
	h.limits["s"] = 1
	h.limitStrict["s"] = true
	if ok, _, _ := h.allowKey("s"); !ok {
		t.Fatal("strict 第一次应放行")
	}
	t0 := time.Now()
	ok, kind, limit := h.allowKey("s")
	if ok || kind != "rate_limited" || limit != 1 {
		t.Fatalf("strict 应直接拒: ok=%v kind=%s limit=%d", ok, kind, limit)
	}
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("strict 不该排队等待，实际等了 %v", d)
	}

	// ④ 没配额度的 Key 不受影响
	if ok, _, _ := h.allowKey("free"); !ok {
		t.Fatal("没配限速的 Key 应永远放行")
	}
}
