package upstream

import (
	"encoding/json"
	"testing"
)

// 上游对"思考级别不对"的两类真实回复必须被认出来（否则会白报错给客户）
func TestNeedsThinkingRetry(t *testing.T) {
	yes := []string{
		`{"error":{"message":"该模型始终思考，不支持关闭思考；请使用 low、high 或 max。","code":50201}}`,
		`{"error":{"message":"lobsterai_options: model does not have a valid thinkingConfig","code":4000}}`,
	}
	for _, s := range yes {
		if !needsThinkingRetry([]byte(s)) {
			t.Errorf("应该判为需要换思考级别重试: %s", s)
		}
	}
	no := []string{
		`{"error":{"message":"rate_limit_exceeded: tpm"}}`,
		`{"error":{"message":"免费额度已用完，请升级套餐"}}`,
		`{"error":{"message":"model not found"}}`,
	}
	for _, s := range no {
		if needsThinkingRetry([]byte(s)) {
			t.Errorf("不该判为思考级别问题: %s", s)
		}
	}
}

// 默认注入 off；显式强制 low 时必须真的写进 body
func TestPrepareChatBodyThinkingLevel(t *testing.T) {
	raw := []byte(`{"model":"glm-5.3","messages":[{"role":"user","content":"hi"}]}`)

	read := func(b []byte) map[string]any {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("产出不是 JSON: %v (%s)", err, b)
		}
		return m
	}
	levelOf := func(m map[string]any) string {
		o, _ := m["lobsterai_options"].(map[string]any)
		th, _ := o["thinking"].(map[string]any)
		s, _ := th["level"].(string)
		return s
	}

	t.Setenv("LB2A_THINKING_LEVEL", "")
	if got := levelOf(read(prepareChatBody(raw))); got != "off" {
		t.Fatalf("默认级别 = %q，期望 off", got)
	}
	// 强制 low（重试用）：normalizeThinkingLevel 会把 low 归成 off，
	// 所以重试必须走"不过归一化"的那条路，否则又回到被拒的 off
	if got := levelOf(read(prepareChatBodyLevel(raw, "low"))); got != "low" {
		t.Fatalf("强制级别 = %q，期望 low", got)
	}
	if !read(prepareChatBody(raw))["stream"].(bool) {
		t.Fatal("stream 必须被强制成 true")
	}
}

// 形态记忆：同一个模型第二次请求应该直接用上次成功的档，不再白跑重试
func TestShapeCache(t *testing.T) {
	if got := modelOf([]byte(`{"model":"glm-5.3","messages":[]}`)); got != "glm-5.3" {
		t.Fatalf("modelOf = %q", got)
	}
	if got := modelOf([]byte("不是 JSON")); got != "" {
		t.Fatalf("非法请求体应返回空 model，得到 %q", got)
	}
	const m = "测试-形态记忆模型"
	if shapeOf(m) != "" {
		t.Fatal("不该有初始记忆")
	}
	rememberShape(m, shapeHigh)
	if shapeOf(m) != shapeHigh {
		t.Fatalf("记忆没存上: %q", shapeOf(m))
	}
	// 空模型名不该被记忆（否则所有解析失败的请求会互相污染）
	rememberShape("", shapeNone)
	if shapeOf("") != "" {
		t.Fatal("空模型名不该被记忆")
	}
}

// 40301 既可能是"形态不对"也可能是"这号没权限" —— isAccessError 要能认出来
func TestIsAccessError(t *testing.T) {
	yes := []string{
		`data:{"type":"error","error":{"message":"模型不可见或无访问权限: qwen3.8-flash","code":40301}}`,
		`{"code":40301,"message":"没有访问权限"}`,
	}
	for _, s := range yes {
		if !isAccessError([]byte(s)) {
			t.Errorf("该判为权限问题: %s", s)
		}
	}
	no := []string{
		`{"error":{"message":"该模型始终思考，不支持关闭思考；请使用 low、high 或 max。","code":50201}}`,
		`{"error":{"message":"lobsterai_options: model does not have a valid thinkingConfig","code":4000}}`,
		`{"error":{"message":"rate_limit_exceeded: tpm"}}`,
	}
	for _, s := range no {
		if isAccessError([]byte(s)) {
			t.Errorf("不该判为权限问题: %s", s)
		}
	}
}
