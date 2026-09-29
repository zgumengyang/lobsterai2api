package relay

import "testing"

// A 模式：一个渠道下多个「配了 sk- 令牌」的后台账号 → 转发时轮流用；没配则回落渠道自己的 key。
func TestPickAPIKeyRoundRobin(t *testing.T) {
	c := &Channel{ID: "ch_rr_test_1", APIKey: "primary",
		Logins: []ChannelLogin{{APIKey: "k1"}, {APIKey: ""}, {APIKey: " k2 "}}}
	got := []string{c.PickAPIKey(), c.PickAPIKey(), c.PickAPIKey(), c.PickAPIKey()}
	want := []string{"k1", "k2", "k1", "k2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 次应取 %s，实际 %s（全序列 %v）", i+1, want[i], got[i], got)
		}
	}

	// 一个令牌都没配 → 用渠道自己那个 key（行为跟以前一致）
	c2 := &Channel{ID: "ch_rr_test_2", APIKey: "primary2", Logins: []ChannelLogin{{APIKey: "  "}}}
	if k := c2.PickAPIKey(); k != "primary2" {
		t.Fatalf("没配令牌应回落渠道 key，实际 %q", k)
	}

	// 只配了一个 → 一直用它
	c3 := &Channel{ID: "ch_rr_test_3", APIKey: "primary3", Logins: []ChannelLogin{{APIKey: "only"}}}
	for i := 0; i < 3; i++ {
		if k := c3.PickAPIKey(); k != "only" {
			t.Fatalf("只有一个令牌时应一直用它，实际 %q", k)
		}
	}

	// nil 不 panic
	var nilCh *Channel
	if nilCh.PickAPIKey() != "" {
		t.Fatal("nil 渠道应返回空")
	}
}
