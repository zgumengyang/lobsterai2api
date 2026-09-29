package server

import (
	"errors"
	"testing"
)

// 上游的推广/额度文案不能原样透传给客户端（用户要求"不要显示这个"）
func TestSanitizeUpstreamMsg(t *testing.T) {
	if got := sanitizeUpstreamMsg("免费额度已用完，请升级套餐"); got == "免费额度已用完，请升级套餐" || got == "" {
		t.Fatalf("应被换成中立话术，实际 %q", got)
	}
	if got := sanitizeUpstreamMsg("渠道 Tier http 500: internal error"); got != "渠道 Tier http 500: internal error" {
		t.Fatalf("普通错误不该被改，实际 %q", got)
	}
	if got := sanitizeUpstreamMsg(""); got != "" {
		t.Fatalf("空串原样返回，实际 %q", got)
	}
}

// isQuotaErr：额度类错误要能被认出来（用来决定"换下一把 key"）
func TestIsQuotaErr(t *testing.T) {
	if !isQuotaErr(errors.New("渠道 Tier http 403: 免费额度已用完")) {
		t.Fatal("额度错误应被判为 true")
	}
	if !isQuotaErr(errors.New("http 402: insufficient balance")) {
		t.Fatal("insufficient balance 应被判为 true")
	}
	if isQuotaErr(errors.New("渠道 Tier http 500: timeout")) {
		t.Fatal("超时不该被判为额度错误")
	}
	if isQuotaErr(nil) {
		t.Fatal("nil 应为 false")
	}
}
