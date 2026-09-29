package relay

import "testing"

// 冻结的后台账号不能参与转发轮询（2026-09-22 用户要求加的功能：
// 反代里每个后台账号都能单独冻结/解冻；冻结 = 不参与转发，配置全留）
func TestAccountKeysSkipsFrozen(t *testing.T) {
	c := &Channel{
		ID: "ch-test-frozen", APIKey: "sk-main",
		Logins: []ChannelLogin{
			{Phone: "A", APIKey: "sk-a"},
			{Phone: "B", APIKey: "sk-b", Frozen: true},
			{Phone: "C", Frozen: true}, // 冻结 + 本来就没令牌
			{Phone: "D", APIKey: "sk-d"},
		},
	}
	got := c.AccountKeys()
	if len(got) != 2 {
		t.Fatalf("参与轮询的令牌数 = %d，期望 2（%v）", len(got), got)
	}
	for _, k := range got {
		if k == "sk-b" {
			t.Fatal("冻结的 sk-b 不该在轮询列表里")
		}
	}

	// 全冻住 → 回落到渠道自己那把主 key（不能因为全冻就变成没 key 可用）
	c2 := &Channel{ID: "ch-test-frozen2", APIKey: "sk-main", Logins: []ChannelLogin{{APIKey: "sk-x", Frozen: true}}}
	if len(c2.AccountKeys()) != 0 {
		t.Fatal("全冻结时 AccountKeys 应该为空")
	}
	if k := c2.PickAPIKey(); k != "sk-main" {
		t.Fatalf("全冻结时应回落到渠道主 key，得到 %q", k)
	}
}
