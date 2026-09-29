package upstream

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"lobsterai2api/internal/auth"
)

func quotaSrv(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/profile-summary" {
			t.Errorf("路径不对: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)
	return srv
}

// 上游明确回答"没积分"时必须是 ErrNoCredits（= 余额 0 的权威答案），
// 因为调度器靠它把积分归零 + 硬冷却。当成普通 error 只打日志，
// 号就会顶着过期积分一直被选号 → 用户看到"正在重新连接"（2026-09-22 实测）。
func TestQuotaUsageNoCreditsSentinel(t *testing.T) {
	srv := quotaSrv(t, `{"code":0,"data":{"totalCreditsRemaining":0}}`)
	defer srv.Close()

	_, _, err := New().QuotaUsage(&auth.Auth{AccessToken: "fake"})
	if !errors.Is(err, ErrNoCredits) {
		t.Fatalf("err = %v，期望 ErrNoCredits", err)
	}
}

// 字段整个缺失也走同一条路
func TestQuotaUsageMissingField(t *testing.T) {
	srv := quotaSrv(t, `{"code":0,"data":{}}`)
	defer srv.Close()

	if _, _, err := New().QuotaUsage(&auth.Auth{AccessToken: "fake"}); !errors.Is(err, ErrNoCredits) {
		t.Fatalf("err = %v，期望 ErrNoCredits", err)
	}
}

// 有积分时必须正常返回（别把好号也判成没积分）
func TestQuotaUsageOK(t *testing.T) {
	srv := quotaSrv(t, `{"code":0,"data":{"totalCreditsRemaining":5297.72}}`)
	defer srv.Close()

	remain, _, err := New().QuotaUsage(&auth.Auth{AccessToken: "fake"})
	if err != nil {
		t.Fatalf("不该报错: %v", err)
	}
	if remain != 5297 {
		t.Fatalf("remain = %d，期望 5297", remain)
	}
}

// Stream 的 quota 标记：流里出现过"额度用完"文案 → true（调用方据此冷却账号）；
// 同时上游的推广原文绝不能透传给客户。
func TestStreamQuotaFlag(t *testing.T) {
	normal := "data: {\"choices\":[{\"delta\":{\"content\":\"好的\"}}]}\n\ndata: [DONE]\n\n"
	rec := httptest.NewRecorder()
	_, quota, err := Stream(rec, strings.NewReader(normal))
	if err != nil {
		t.Fatalf("正常流不该报错: %v", err)
	}
	if quota {
		t.Fatal("正常流不该标成 quota")
	}
	if !strings.Contains(rec.Body.String(), "好的") {
		t.Fatal("正常内容应该原样转发")
	}

	bad := "data: {\"choices\":[{\"delta\":{\"content\":\"部分\"}}]}\n\n" +
		"data: {\"error\":{\"message\":\"免费额度已用完，请升级套餐\"}}\n\n" +
		"data: [DONE]\n\n"
	rec2 := httptest.NewRecorder()
	_, quota2, err2 := Stream(rec2, strings.NewReader(bad))
	if err2 != nil {
		t.Fatalf("err = %v", err2)
	}
	if !quota2 {
		t.Fatal("出现额度文案时 quota 必须为 true（否则账号不会被冷却）")
	}
	if strings.Contains(rec2.Body.String(), "升级套餐") || strings.Contains(rec2.Body.String(), "免费额度") {
		t.Fatal("上游推广文案不该原样透传给客户")
	}
	if !strings.Contains(rec2.Body.String(), "upstream_quota") {
		t.Fatal("被换掉的那行应该是中立的 upstream_quota 事件")
	}
}
