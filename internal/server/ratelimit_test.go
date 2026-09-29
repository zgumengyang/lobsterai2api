package server

import (
	"errors"
	"testing"
)

// 2026-09-22：Tier 反代 3 把令牌连挂 6 单，全是 429 tpm。
// 这组用例盯住「限速要触发换号」这个判据，防止以后又被当成普通错误 break 掉。
func TestIsRateLimitErr(t *testing.T) {
	yes := []string{
		`渠道 Tier http 429: {"error":{"message":"rate_limit_exceeded: tpm (request id: 2026...)"}}`,
		`渠道 X http 429: too many requests`,
		`upstream rate-limit hit`,
		`渠道 Y http 429: {"error":{"message":"rpm limit exceeded"}}`,
	}
	for _, s := range yes {
		if !isRateLimitErr(errors.New(s)) {
			t.Errorf("应该判为限速，但没判出来: %s", s)
		}
	}

	no := []string{
		`渠道 X http 500: internal error`,
		`渠道 X http 404: model not found`,
		`渠道 X http 401: invalid api key`,
		`context deadline exceeded`,
	}
	for _, s := range no {
		if isRateLimitErr(errors.New(s)) {
			t.Errorf("不该判为限速，却判成了: %s", s)
		}
	}
	if isRateLimitErr(nil) {
		t.Error("nil 不该判为限速")
	}
	// 额度用完和限速是两条判据，别串味
	if isRateLimitErr(errors.New(`渠道 X http 402: insufficient credit`)) {
		t.Error("额度不足不是限速")
	}
	if !isQuotaErr(errors.New(`渠道 X http 402: insufficient credit`)) {
		t.Error("额度不足应该被 isQuotaErr 认出来")
	}
}
