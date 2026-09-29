package relay

import "testing"

// channelDeclares：只认「明确写出来」的接管（Models ∪ SeenModels / "*" / ModelMap 命中），
// 跟 Serves 的"没声明 = 什么都接"不同。
func TestChannelDeclares(t *testing.T) {
	cases := []struct {
		name   string
		ch     *Channel
		model  string
		expect bool
	}{
		{"nil", nil, "x", false},
		{"空模型名", &Channel{Models: []string{"a"}}, "", false},
		{"没声明任何模型 → 不算明确接管", &Channel{}, "随便", false},
		{"Models 命中", &Channel{Models: []string{"tierflow"}}, "tierflow", true},
		{"见模型名（实测）也算", &Channel{SeenModels: []string{"tierflow"}}, "tierflow", true},
		{"大小写不敏感", &Channel{Models: []string{"DeepSeek-V4.1-Flash"}}, "deepseek-v4.1-flash", true},
		{"通配算明确接管全部", &Channel{Models: []string{"*"}}, "随便什么", true},
		{"没命中", &Channel{Models: []string{"a"}, SeenModels: []string{"b"}}, "c", false},
		{"映射命中算", &Channel{Models: []string{"x"}, ModelMap: map[string]string{"glm-5.3": "glm-5.3-0731"}}, "glm-5.3", true},
	}
	for _, c := range cases {
		if got := channelDeclares(c.ch, c.model); got != c.expect {
			t.Errorf("%s: channelDeclares = %v, 期望 %v", c.name, got, c.expect)
		}
	}
}

// MatchExplicit：按优先级顺序取第一个「明确声明」的启用渠道；停用/没声明的跳过。
func TestMatchExplicit(t *testing.T) {
	s := NewStore("")
	a := s.Add(&Channel{Name: "a", BaseURL: "https://a/v1", APIKey: "k", Enabled: true, Priority: 10})
	b := s.Add(&Channel{Name: "b", BaseURL: "https://b/v1", APIKey: "k", Enabled: true, Priority: 20})
	// a 声明 tierflow，b 什么都没声明
	a.Models = []string{"tierflow"}
	s.Replace(a)

	if got := s.MatchExplicit("tierflow"); got == nil || got.ID != a.ID {
		t.Fatalf("tierflow 应当命中 a，实际 %+v", got)
	}
	// b 没声明 → 不算明确接管 → nil（不能让它把任意模型都抢走）
	if got := s.MatchExplicit("kimi-k3"); got != nil {
		t.Fatalf("没声明的渠道不该被 MatchExplicit 命中，实际 %s", got.Name)
	}

	// 停用后不再命中
	s.SetEnabled(a.ID, false)
	if got := s.MatchExplicit("tierflow"); got != nil {
		t.Fatalf("停用的渠道不该命中，实际 %s", got.Name)
	}
	s.SetEnabled(a.ID, true)

	// 多个都声明 → 按优先级顺序取第一个（a 的 Priority 更小）
	b.Models = []string{"tierflow"}
	s.Replace(b)
	if got := s.MatchExplicit("tierflow"); got == nil || got.ID != a.ID {
		t.Fatalf("两个都声明时应取优先级小的 a，实际 %+v", got)
	}
	if got := s.MatchExplicit(""); got != nil {
		t.Fatal("空模型名应返回 nil")
	}
}
