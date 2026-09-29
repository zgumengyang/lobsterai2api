package server

import "testing"

// 「模型名不存在」和「这个号没这个模型的权限」必须分开：
//
//	前者换号没用（应该 404 直接告诉客户端）
//	后者是按账号算的（必须继续换号）
//
// 2026-09-22 体检实测：客户端发 no-such-model-xyz，旧代码换了 5 个号、白打 5 次上游，
// 最后报 503 "all accounts unavailable"，客户端以为线路全挂了。
func TestIsModelMissingErr(t *testing.T) {
	yes := []string{
		`event:error data:{"type":"error","error":{"type":"proxy_error","message":"不支持的模型: no-such-model-xyz (allowed providers: LobsterAI)","code":40300}}`,
		`{"error":{"message":"model not found"}}`,
		`{"message":"模型不存在"}`,
		`{"message":"unknown model: foo"}`,
	}
	for _, s := range yes {
		if !isModelMissingErr([]byte(s)) {
			t.Errorf("应该判为「模型不存在」: %s", s)
		}
	}
	no := []string{
		`{"message":"模型不可见或无访问权限: qwen3.8-flash","code":40301}`,
		`{"message":"该模型始终思考，不支持关闭思考；请使用 low、high 或 max。","code":50201}`,
		`{"message":"免费额度已用完，请升级套餐"}`,
		`{"message":"rate_limit_exceeded: tpm"}`,
	}
	for _, s := range no {
		if isModelMissingErr([]byte(s)) {
			t.Errorf("不该判为「模型不存在」（40301 是按账号的，必须继续换号）: %s", s)
		}
	}
}
