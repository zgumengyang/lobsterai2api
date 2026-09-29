package relay

import (
	"path/filepath"
	"testing"
)

// SetSeen：测试实测到的模型要能落盘、重开后还在、List/Get 拷贝必须带上，
// 而且面板「编辑保存」(Replace) 不能把它冲掉 —— 否则模型测试页又会看不到这个反代。
func TestSetSeenPersistAndCopy(t *testing.T) {
	f := filepath.Join(t.TempDir(), "channels.json")
	s := NewStore(f)
	c := s.Add(&Channel{Name: "Tier", BaseURL: "https://t.example/v1", APIKey: "sk-x", Enabled: true})
	if got := s.Get(c.ID).SeenModels; len(got) != 0 {
		t.Fatalf("新建渠道不该有 seen_models，实际 %v", got)
	}

	// 归一化：去空白、忽略大小写去重、丢空串
	s.SetSeen(c.ID, []string{" a-1 ", "b-2", "A-1", ""})
	got := s.Get(c.ID).SeenModels
	if len(got) != 2 || got[0] != "a-1" || got[1] != "b-2" {
		t.Fatalf("SetSeen 归一化失败: %v", got)
	}

	// 重开存储 → 必须还在（落盘生效）
	s2 := NewStore(f)
	if g := s2.Get(c.ID); g == nil || len(g.SeenModels) != 2 {
		t.Fatalf("重开存储后 seen_models 丢了: %+v", g)
	}

	// List 的拷贝也要带，否则面板读不到
	l := s2.List()
	if len(l) != 1 || len(l[0].SeenModels) != 2 {
		t.Fatalf("List 没带 seen_models: %+v", l)
	}

	// 面板编辑保存走的是 Get → 改字段 → Replace，不能把 seen_models 冲掉
	cur := s2.Get(c.ID)
	cur.Models = []string{"a-1"}
	if !s2.Replace(cur) {
		t.Fatal("Replace 应当成功")
	}
	if g := s2.Get(c.ID); len(g.SeenModels) != 2 || len(g.Models) != 1 {
		t.Fatalf("Replace 后 seen_models 丢了: models=%v seen=%v", g.Models, g.SeenModels)
	}

	// 未知 ID / 传空 = 清掉，不 panic
	s2.SetSeen("不存在的ID", []string{"x"})
	s2.SetSeen(c.ID, nil)
	if g := s2.Get(c.ID); len(g.SeenModels) != 0 {
		t.Fatalf("SetSeen(nil) 应当清空: %v", g.SeenModels)
	}
}

// sameFoldList：顺序敏感、忽略大小写与空白
func TestSameFoldList(t *testing.T) {
	cases := []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{[]string{}, []string{}, true},
		{[]string{"A"}, []string{"a"}, true},
		{[]string{" A "}, []string{"a"}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, false},
		{[]string{"a"}, []string{"a", "b"}, false},
	}
	for _, c := range cases {
		if got := sameFoldList(c.a, c.b); got != c.want {
			t.Errorf("sameFoldList(%v,%v)=%v 期望 %v", c.a, c.b, got, c.want)
		}
	}
}
