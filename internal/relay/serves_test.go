package relay

import "testing"

// Serves 的判定规则：没声明模型=不限制；声明了就看命中；配了映射也算命中；通配全接。
func TestServes(t *testing.T) {
	cases := []struct {
		name   string
		ch     Channel
		model  string
		expect bool
	}{
		{"没声明模型=不限制", Channel{}, "glm-5.3", true},
		{"空模型名=放行", Channel{Models: []string{"a"}}, "", true},
		{"声明了且命中", Channel{Models: []string{"deepseek-v4-flash"}}, "deepseek-v4-flash", true},
		{"声明了但没命中", Channel{Models: []string{"deepseek-v4-flash"}}, "glm-5.3", false},
		{"大小写不敏感", Channel{Models: []string{"DeepSeek-V4-Flash"}}, "deepseek-v4-flash", true},
		{"通配全接", Channel{Models: []string{"*"}}, "随便什么", true},
		{"模型名带空格", Channel{Models: []string{" deepseek-v4-pro "}}, " deepseek-v4-pro ", true},
		{"配了映射算命中", Channel{
			Models:   []string{"deepseek-v4-flash"},
			ModelMap: map[string]string{"deepseek-flash": "deepseek-v4-flash"},
		}, "deepseek-flash", true},
		{"映射没命中则按声明判", Channel{
			Models:   []string{"deepseek-v4-flash"},
			ModelMap: map[string]string{"xxx": "yyy"},
		}, "glm-5.3", false},
	}
	for _, c := range cases {
		ch := c.ch
		if got := ch.Serves(c.model); got != c.expect {
			t.Errorf("%s: Serves(%q) = %v, 期望 %v", c.name, c.model, got, c.expect)
		}
	}
	var nilCh *Channel
	if nilCh.Serves("x") {
		t.Error("nil 渠道必须返回 false")
	}
}
