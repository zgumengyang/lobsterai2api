package relay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// 多个后台账号：去空、按 token 去重、顺序保持；List/Get 拷贝必须带上（面板才能列出来）
func TestNormalizeAndCopyLogins(t *testing.T) {
	s := NewStore("")
	c := s.Add(&Channel{Name: "Tier", BaseURL: "https://t.example/v1", APIKey: "sk-x", Enabled: true})
	c.Logins = []ChannelLogin{
		{Token: "A", Phone: "187****5260"},
		{Token: "  "},              // 空 → 丢
		{Token: "A", Phone: "dup"}, // 重复 → 丢
		{Token: "B", Phone: "171****5638"},
	}
	if !s.Replace(c) {
		t.Fatal("Replace 应成功")
	}
	got := s.Get(c.ID).Logins
	if len(got) != 2 || got[0].Token != "A" || got[1].Token != "B" {
		t.Fatalf("归一化失败: %+v", got)
	}
	if l := s.List(); len(l) != 1 || len(l[0].Logins) != 2 {
		t.Fatalf("List 没带 Logins: %+v", l)
	}
}

// 老文件只有 login_token → 加载时迁移成 Logins[0]（多账号改动之前配的登录态不能丢）
func TestLoadMigratesLoginToken(t *testing.T) {
	f := filepath.Join(t.TempDir(), "channels.json")
	raw := map[string]any{
		"channels": []map[string]any{{
			"id": "ch_old", "name": "Tier", "base_url": "https://t.example/v1",
			"api_key": "sk-x", "enabled": true, "priority": 10,
			"login_token": `{"cookie":"session=abc","uid":"7"}`,
		}},
		"order": []string{"ch_old", "lobster"},
	}
	b, _ := json.MarshalIndent(raw, "", "  ")
	if err := os.WriteFile(f, b, 0644); err != nil {
		t.Fatal(err)
	}
	s := NewStore(f)
	c := s.Get("ch_old")
	if c == nil {
		t.Fatal("渠道没加载出来")
	}
	if len(c.Logins) != 1 || c.Logins[0].Token == "" {
		t.Fatalf("login_token 没迁移成 Logins: %+v", c.Logins)
	}
}
