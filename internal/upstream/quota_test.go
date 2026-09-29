package upstream

import "testing"

// 上游的"额度用完"文案要能被识别成 ErrHardCredit（否则号不会冷却、一直白撞）。
// 特别是带推广口吻的「免费额度已用完，请升级套餐」—— 2026-09-22 实测漏网。
func TestClassifyQuotaExhausted(t *testing.T) {
	cases := []string{
		"免费额度已用完，请升级套餐",
		"额度已用完",
		"额度用完",
		"quota exceeded",
		"insufficient balance",
		"积分不足",
	}
	for _, body := range cases {
		if k := Classify(400, body); k != ErrHardCredit {
			t.Errorf("Classify(400, %q) = %s，期望 hard_credit", body, k)
		}
	}
}
