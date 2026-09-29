package upstream

import (
	"net/http"
	"testing"
)

// TestClassifyBanned 2026-09-21 回归：有道批量封号后上游返回
//
//	HTTP 403 {"code":40302,"message":"账号已被禁用","data":null}
//
// 旧版本把它归 ErrClient（“请求形态错误，不惩罚账号”），
// 结果死号永不被禁用，每次请求都白撞。
func TestClassifyBanned(t *testing.T) {
	bannedBody := `{"code":40302,"message":"账号已被禁用","data":null}`
	cases := []struct {
		name   string
		status int
		body   string
		want   ErrKind
	}{
		{"banned 403 + 40302", http.StatusForbidden, bannedBody, ErrBanned},
		{"banned 403 + 中文提示", http.StatusForbidden, `{"code":40302,"message":"账号已被禁用"}`, ErrBanned},
		{"中性 403 仍为 client", http.StatusForbidden, `{"error":"forbidden"}`, ErrClient},
		{"普通 400 仍为 client", http.StatusBadRequest, `{"error":"bad request"}`, ErrClient},
		{"余额不足仍为 hard_credit", http.StatusForbidden, `{"message":"积分不足"}`, ErrHardCredit},
		{"40101 仍为 session_dead", http.StatusUnauthorized, `{"code":40101}`, ErrSessionDead},
	}
	for _, c := range cases {
		got := Classify(c.status, c.body)
		if got != c.want {
			t.Errorf("%s: Classify(%d, %s) = %v, want %v", c.name, c.status, c.body, got, c.want)
		}
	}
}
