// lobster-auth-helper：本机扫码登录回调助手
// 监听 127.0.0.1:18367（龙虾登录页硬校验 redirect_uri 必须是 127.0.0.1，回调只能落本机）
// 面板发起扫码 → 助手生成链接 → 用户扫码 → 回调到本机 → 助手 exchange → 自动提交面板池子
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	listen       = flag.String("listen", "127.0.0.1:18367", "助手监听地址")
	upstreamBase = flag.String("upstream", "https://lobsterai-server.youdao.com", "龙虾上游 API")
	portalBase   = flag.String("portal", "https://lobsterai.youdao.com", "龙虾登录门户")
	panelURL     = flag.String("panel", "http://124.221.39.166:8368", "面板地址（提交账号用）")
)

type pendingLogin struct {
	State        string
	Uuid         string
	FirstKeyfrom string
	CreatedAt    time.Time
	Done         bool
	Err          string
}

var (
	mu      sync.Mutex
	pending = map[string]*pendingLogin{}
)

func newUuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func nowMillis() string {
	return fmt.Sprintf("%d", time.Now().UnixMilli())
}

func cors(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func main() {
	flag.Parse()

	// 探活：面板检测助手是否在线
	http.HandleFunc("/ping", cors(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"ok": true, "service": "lobster-auth-helper"})
	}))

	// 面板发起：生成登录链接
	http.HandleFunc("/begin", cors(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			InvitationCode string `json:"invitationCode"`
		}
		if r.Body != nil {
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &req)
		}
		state := randomHex(16)
		uuid := newUuid()
		mu.Lock()
		pending[state] = &pendingLogin{State: state, Uuid: uuid, FirstKeyfrom: nowMillis(), CreatedAt: time.Now()}
		mu.Unlock()

		redirectURI := fmt.Sprintf("http://127.0.0.1:%s/auth/callback", strings.SplitN(*listen, ":", 2)[1])
		loginURL := fmt.Sprintf("%s/portal#/login?source=electron&redirect_uri=%s&state=%s",
			*portalBase, urlQueryEscape(redirectURI), state)
		if req.InvitationCode != "" {
			loginURL += "&invitationCode=" + urlQueryEscape(req.InvitationCode)
		}
		writeJSON(w, map[string]any{"ok": true, "url": loginURL, "state": state})
	}))

	// 龙虾登录页回调（必须 127.0.0.1，登录页硬校验）
	http.HandleFunc("/auth/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		code := q.Get("code")
		state := q.Get("state")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		mu.Lock()
		p, ok := pending[state]
		mu.Unlock()
		if !ok || code == "" {
			io.WriteString(w, "<html><body><h2>回调参数无效或登录已超时</h2></body></html>")
			return
		}
		// 同步 exchange（阻塞几秒，直接返回结果页）
		err := doExchange(p, code)
		mu.Lock()
		p.Done = true
		if err != nil {
			p.Err = err.Error()
		}
		mu.Unlock()
		if err != nil {
			io.WriteString(w, "<html><body><h2>登录失败</h2><pre>"+err.Error()+"</pre><p>请回到面板重试</p></body></html>")
			return
		}
		io.WriteString(w, "<html><body><h2>✅ 登录成功，账号已加入池子</h2><p>可以关闭此窗口，回到面板查看。</p></body></html>")
	})

	// 面板轮询登录状态
	http.HandleFunc("/status", cors(func(w http.ResponseWriter, r *http.Request) {
		state := r.URL.Query().Get("state")
		mu.Lock()
		p, ok := pending[state]
		mu.Unlock()
		if !ok {
			writeJSON(w, map[string]any{"ok": false, "error": "state not found"})
			return
		}
		writeJSON(w, map[string]any{"ok": true, "done": p.Done, "error": p.Err})
	}))

	log.Printf("auth-helper listening on %s (panel=%s)", *listen, *panelURL)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

// doExchange 用 code 换 token，成功后自动提交到面板 /api/accounts
func doExchange(p *pendingLogin, code string) error {
	body := map[string]any{
		"authCode":      code,
		"firstKeyfrom":  p.FirstKeyfrom,
		"latestKeyfrom": nowMillis(),
		"uuid":          p.Uuid,
		"version":       "0.1.0",
	}
	raw, _ := json.Marshal(body)
	data, err := postJSON(*upstreamBase+"/api/auth/exchange", raw)
	if err != nil {
		return fmt.Errorf("exchange: %w", err)
	}
	var env struct {
		Code int             `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &env) != nil {
		return fmt.Errorf("exchange 响应解析失败")
	}
	if env.Code != 0 {
		return fmt.Errorf("exchange code=%d msg=%s", env.Code, env.Msg)
	}
	var ex struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"`
		User         struct {
			ID       string `json:"id"`
			Yid      string `json:"yid"`
			UserId   string `json:"userId"`
			Nickname string `json:"nickname"`
		} `json:"user"`
	}
	if json.Unmarshal(env.Data, &ex) != nil {
		return fmt.Errorf("exchange data 解析失败")
	}
	if ex.AccessToken == "" {
		return fmt.Errorf("exchange 无 accessToken")
	}
	uid := ex.User.ID
	if uid == "" {
		uid = ex.User.UserId
	}
	if uid == "" {
		uid = ex.User.Yid
	}
	expiresAt := time.Now().Add(30 * 24 * time.Hour).Unix()
	if ex.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(ex.ExpiresIn) * time.Second).Unix()
	} else if exp := jwtExpiry(ex.AccessToken); exp > 0 {
		expiresAt = exp
	}

	// 嵌套形 auth（面板 /api/accounts 兼容）
	authDoc := map[string]any{
		"auth": map[string]any{
			"accessToken":   ex.AccessToken,
			"refreshToken":  ex.RefreshToken,
			"expiresAt":     expiresAt,
			"uuid":          p.Uuid,
			"firstKeyfrom":  p.FirstKeyfrom,
			"latestKeyfrom": nowMillis(),
		},
		"account": map[string]any{
			"uid":      uid,
			"userId":   ex.User.UserId,
			"nickname": ex.User.Nickname,
		},
	}
	authRaw, _ := json.Marshal(authDoc)
	resp, err := postJSON(*panelURL+"/api/accounts", authRaw)
	if err != nil {
		return fmt.Errorf("提交面板失败: %w", err)
	}
	var pr struct {
		OK      bool   `json:"ok"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(resp, &pr)
	if !pr.OK {
		return fmt.Errorf("面板拒绝: %s", pr.Error)
	}
	log.Printf("账号 %s (%s) 已提交面板: %s", uid, ex.User.Nickname, pr.Message)
	return nil
}

func postJSON(url string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "LobsterAI/0.1.0")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(raw)[:min(200, len(raw))])
	}
	return raw, nil
}

func jwtExpiry(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return 0
	}
	return claims.Exp
}

func urlQueryEscape(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' || c == '/' || c == ':':
			out = append(out, c)
		default:
			out = append(out, '%', hexDigits[c>>4], hexDigits[c&0x0f])
		}
	}
	return string(out)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
