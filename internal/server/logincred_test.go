package server

import "testing"

// parseLoginCred：面板里粘进来的东西可能有三种形态（自定义前端的打包 JSON / 裸 token / 裸 Cookie），
// 必须都能解析成可用的凭据 —— 特别是 tierflow.cn 那种"根本没有 access_token、只认 Cookie"的站。
func TestParseLoginCred(t *testing.T) {
	cases := []struct {
		name       string
		raw        string
		wantToken  string
		wantCookie string
		wantUID    string
	}{
		{"控制台打包 JSON（tierflow 这种）",
			`{"user":"{\"id\":7,\"username\":\"demo\"}","uid":"7","cookie":"session=abc123; other=z"}`,
			"", "session=abc123; other=z", "7"},
		{"标准 new-api：user 里有 access_token",
			`{"user":"{\"access_token\":\"eyJhbGciOi\",\"id\":42}","uid":"","cookie":""}`,
			"eyJhbGciOi", "", "42"},
		{"裸 JWT", "eyJhbGciOiJIUzI1NiJ9.abc.def", "eyJhbGciOiJIUzI1NiJ9.abc.def", "", ""},
		{"裸 sk- key", "sk-abc123", "sk-abc123", "", ""},
		{"裸 Cookie 串", "session=zzz; theme=dark", "", "session=zzz; theme=dark", ""},
		{"带 Cookie: 前缀", "Cookie: session=zzz", "", "session=zzz", ""},
		{"空串", "", "", "", ""},
	}
	for _, c := range cases {
		got := parseLoginCred(c.raw)
		if got.Token != c.wantToken || got.Cookie != c.wantCookie || got.UID != c.wantUID {
			t.Errorf("%s: got token=%q cookie=%q uid=%q; 期望 token=%q cookie=%q uid=%q",
				c.name, got.Token, got.Cookie, got.UID, c.wantToken, c.wantCookie, c.wantUID)
		}
	}
}

// attempts：cookie 优先；带上 TF-User / New-Api-User；不产生空头。
func TestLoginCredAttempts(t *testing.T) {
	c := parseLoginCred(`{"user":"{\"access_token\":\"tok\"}","uid":"9","cookie":"session=abc"}`)
	as := c.attempts()
	if len(as) != 2 {
		t.Fatalf("应当产生 2 组尝试（cookie + bearer），实际 %d", len(as))
	}
	if as[0]["Cookie"] != "session=abc" || as[0]["TF-User"] != "9" {
		t.Fatalf("第一组应是 Cookie 优先: %v", as[0])
	}
	if as[1]["Authorization"] != "Bearer tok" || as[1]["New-Api-User"] != "9" {
		t.Fatalf("第二组应是 Bearer: %v", as[1])
	}
	// 只有 token 时只出一组
	if n := len(parseLoginCred("sk-only").attempts()); n != 1 {
		t.Fatalf("只有 token 应只 1 组，实际 %d", n)
	}
	// 空凭据不产生任何尝试（避免拿空头去撞上游）
	if n := len(parseLoginCred("").attempts()); n != 0 {
		t.Fatalf("空凭据应 0 组，实际 %d", n)
	}
}
