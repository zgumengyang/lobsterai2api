package upstream

import (
	"strings"
	"testing"
)

// 流式里上游常把额度错误塞在 data 块里 —— 必须整行换成中立 error 事件，
// 否则客户端会显示"stream disconnected before completion: 免费额度已用完，请升级套餐"。
func TestSanitizeSSELine(t *testing.T) {
	bad := "data: {\"error\":{\"message\":\"免费额度已用完，请升级套餐\"}}\n\n"
	got := SanitizeSSELine(bad)
	if got == bad {
		t.Fatalf("额度文案没被替换: %q", got)
	}
	if !strings.Contains(got, "data:") || !strings.Contains(got, "upstream_quota") || !strings.Contains(got, QuotaNotice) {
		t.Fatalf("替换后不是合法的中立 error 事件: %q", got)
	}
	// 正常内容不动
	ok := "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\n"
	if SanitizeSSELine(ok) != ok {
		t.Fatal("正常内容不该被改")
	}
	// 非 data 行不动
	if SanitizeSSELine("event: ping\n") != "event: ping\n" {
		t.Fatal("非 data 行不该被改")
	}
}
