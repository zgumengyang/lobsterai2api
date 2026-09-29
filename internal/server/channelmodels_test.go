package server

import (
	"testing"

	"lobsterai2api/internal/relay"
)

// channelModelList = 接管声明 ∪ 最近实测；去重（忽略大小写）、保留原拼写、丢空串。
// 核心用途：没填「接管模型」的反代，靠实测模型也能在 /v1/models 和面板模型测试里出现。
func TestChannelModelList(t *testing.T) {
	cases := []struct {
		name   string
		ch     *relay.Channel
		expect []string
	}{
		{"nil 渠道", nil, nil},
		{"只有声明", &relay.Channel{Models: []string{"m1", "m2"}}, []string{"m1", "m2"}},
		{"只有实测（用户报的 bug 场景）", &relay.Channel{SeenModels: []string{"t1", "t2"}}, []string{"t1", "t2"}},
		{"合并 + 忽略大小写去重", &relay.Channel{Models: []string{"A"}, SeenModels: []string{"a", "b"}}, []string{"A", "b"}},
		{"通配原样返回（由调用方跳过）", &relay.Channel{Models: []string{"*"}}, []string{"*"}},
		{"丢空串", &relay.Channel{Models: []string{"", "  "}, SeenModels: []string{"x"}}, []string{"x"}},
	}
	for _, c := range cases {
		got := channelModelList(c.ch)
		if len(got) != len(c.expect) {
			t.Errorf("%s: got %v, 期望 %v", c.name, got, c.expect)
			continue
		}
		for i := range got {
			if got[i] != c.expect[i] {
				t.Errorf("%s: got %v, 期望 %v", c.name, got, c.expect)
				break
			}
		}
	}
}
