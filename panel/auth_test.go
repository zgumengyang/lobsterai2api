package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 密码校验 + 会话签发/校验/防伪造/防过期
func TestPanelAuthSession(t *testing.T) {
	a := &panelAuth{fp: filepath.Join(t.TempDir(), "panel-auth.json")}
	a.User = "admin"
	a.Salt = randHex(16)
	a.Secret = randHex(32)
	a.Hash = hashPass(a.Salt, "blueapi2026")

	if !a.checkPass("admin", "blueapi2026") {
		t.Fatal("正确账号密码没通过")
	}
	if a.checkPass("admin", "wrong") {
		t.Fatal("错密码通过了")
	}
	if a.checkPass("root", "blueapi2026") {
		t.Fatal("错账号通过了")
	}

	s := a.newSession()
	if !a.validSession(s) {
		t.Fatal("自己签发的会话不被承认")
	}
	for _, bad := range []string{"", "abc", s + "x", strings.Replace(s, ".", "", 1)} {
		if a.validSession(bad) {
			t.Fatalf("伪造/畸形会话被认了: %q", bad)
		}
	}

	// 过期会话（签名对、时间过期）
	exp := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
	mac := hmac.New(sha256.New, []byte(a.Secret))
	_, _ = mac.Write([]byte(exp + "|" + a.User))
	old := exp + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if a.validSession(old) {
		t.Fatal("过期会话应该无效")
	}

	// 换密钥（= 改密码）后，旧会话立刻失效
	s2 := a.newSession()
	a.mu.Lock()
	a.Secret = randHex(32)
	a.mu.Unlock()
	if a.validSession(s2) {
		t.Fatal("换密钥后旧会话应该失效")
	}
}

// 闸门行为：HTML 跳登录页、/api/* 返 401、/login 与 /healthz 放行、带 cookie 放行
func TestAuthGate(t *testing.T) {
	// 借用全局 store（别的测试不用它），用完恢复
	oldUser, oldSalt, oldSecret, oldHash, oldFp, oldDisabled :=
		panelAuthStore.User, panelAuthStore.Salt, panelAuthStore.Secret,
		panelAuthStore.Hash, panelAuthStore.fp, panelAuthStore.Disabled
	defer func() {
		panelAuthStore.mu.Lock()
		panelAuthStore.User, panelAuthStore.Salt, panelAuthStore.Secret = oldUser, oldSalt, oldSecret
		panelAuthStore.Hash, panelAuthStore.fp, panelAuthStore.Disabled = oldHash, oldFp, oldDisabled
		panelAuthStore.mu.Unlock()
	}()
	panelAuthStore.User = "admin"
	panelAuthStore.Salt = randHex(8)
	panelAuthStore.Secret = randHex(16)
	panelAuthStore.Hash = hashPass(panelAuthStore.Salt, "x")
	panelAuthStore.fp = ""
	panelAuthStore.Disabled = false

	h := authGate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	// ① 没登录打开页面 → 302 到 /login?next=
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusFound || !strings.Contains(rec.Header().Get("Location"), "/login") {
		t.Fatalf("未登录访问页面应该跳登录页，得到 %d %q", rec.Code, rec.Header().Get("Location"))
	}

	// ② 没登录调接口 → 401 JSON
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/api/status", nil))
	if rec2.Code != http.StatusUnauthorized || !strings.Contains(rec2.Body.String(), "not_logged_in") {
		t.Fatalf("未登录调接口应该 401，得到 %d %s", rec2.Code, rec2.Body.String())
	}

	// ③ 登录页 / 健康检查放行
	for _, p := range []string{"/login", "/healthz", "/api/login", "/api/captcha", "/api/authstate"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s 应该放行，得到 %d", p, rec.Code)
		}
	}

	// ④ 带有效会话 → 放行
	req := httptest.NewRequest("GET", "/api/status", nil)
	req.AddCookie(&http.Cookie{Name: authCookieName, Value: panelAuthStore.newSession()})
	rec4 := httptest.NewRecorder()
	h.ServeHTTP(rec4, req)
	if rec4.Code != http.StatusOK {
		t.Fatalf("带有效会话应该放行，得到 %d", rec4.Code)
	}

	// ⑤ 带伪造会话 → 还是 401
	req5 := httptest.NewRequest("GET", "/api/status", nil)
	req5.AddCookie(&http.Cookie{Name: authCookieName, Value: "9999999999.badsig"})
	rec5 := httptest.NewRecorder()
	h.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusUnauthorized {
		t.Fatalf("伪造会话应该 401，得到 %d", rec5.Code)
	}

	// ⑥ 登录被关掉（disabled=true）→ 全部放行（用户 2026-09-22 要求"把这个面板登录去掉"）
	panelAuthStore.mu.Lock()
	panelAuthStore.Disabled = true
	panelAuthStore.mu.Unlock()
	for _, p := range []string{"/", "/api/status", "/api/channels"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("登录关掉后 %s 应该直接放行，得到 %d", p, rec.Code)
		}
	}
}

// 数字验证码（2026-09-22 用户要求加）：4 位数字、一次性、过期/未知 id 一律不通过
func TestCaptchaOneShot(t *testing.T) {
	id, code := newCaptcha()
	if len(code) != 4 {
		t.Fatalf("验证码应该是 4 位数字，得到 %q", code)
	}
	for _, c := range code {
		if c < '0' || c > '9' {
			t.Fatalf("验证码必须是纯数字，得到 %q", code)
		}
	}
	if !checkCaptcha(id, code) {
		t.Fatal("正确验证码没通过")
	}
	if checkCaptcha(id, code) {
		t.Fatal("验证码是一次性的，第二次不该通过")
	}
	if checkCaptcha("not-exist", "1234") {
		t.Fatal("不存在的 id 不该通过")
	}
	if checkCaptcha("", "") || checkCaptcha(id, "") {
		t.Fatal("空值不该通过")
	}
	// 过期
	id2, code2 := newCaptcha()
	captchaMu.Lock()
	captchaMap[id2] = captchaEntry{code: code2, exp: time.Now().Add(-time.Second)}
	captchaMu.Unlock()
	if checkCaptcha(id2, code2) {
		t.Fatal("过期的验证码不该通过")
	}
}

// 按 IP 限速 + 连错锁定（2026-09-22 用户要求）
func TestLoginGuard(t *testing.T) {
	t.Setenv("LB2A_LOGIN_FAILS", "3")
	t.Setenv("LB2A_LOCK_MIN", "1")
	t.Setenv("LB2A_LOGIN_RPM", "5")
	t.Setenv("LB2A_CAPTCHA_RPM", "2")
	g := &ipGuard{ips: map[string]*ipState{}}

	ip := "1.2.3.4"
	if locked, _ := g.noteLoginFail(ip); locked {
		t.Fatal("第 1 次失败不该锁")
	}
	if locked, _ := g.noteLoginFail(ip); locked {
		t.Fatal("第 2 次失败不该锁")
	}
	locked, d := g.noteLoginFail(ip)
	if !locked || d <= 0 {
		t.Fatalf("第 3 次失败应该锁 IP，得到 locked=%v d=%v", locked, d)
	}
	if ok, reason, wait := g.allowLogin(ip); ok || reason != "locked" || wait <= 0 {
		t.Fatalf("锁定期间登录应被拒（locked），得到 ok=%v reason=%s wait=%v", ok, reason, wait)
	}
	if g.allowCaptcha(ip) {
		t.Fatal("锁定期间不该继续发验证码")
	}
	g.noteLoginOK(ip) // 成功登录 = 解锁
	if ok, _, _ := g.allowLogin(ip); !ok {
		t.Fatal("解锁后应该放行")
	}

	// 发码限速：RPM=2 → 第 3 次被限
	ip2 := "5.6.7.8"
	if !g.allowCaptcha(ip2) || !g.allowCaptcha(ip2) {
		t.Fatal("前两次发码应该放行")
	}
	if g.allowCaptcha(ip2) {
		t.Fatal("第 3 次发码应该被限速")
	}

	// 登录尝试限速：RPM=5 → 第 6 次被限
	ip3 := "9.9.9.9"
	for i := 0; i < 5; i++ {
		if ok, _, _ := g.allowLogin(ip3); !ok {
			t.Fatalf("第 %d 次登录不该被限", i+1)
		}
	}
	if ok, reason, _ := g.allowLogin(ip3); ok || reason != "rate" {
		t.Fatalf("第 6 次登录应该被限速（rate），得到 ok=%v reason=%s", ok, reason)
	}
	// 别的 IP 不受影响
	if ok, _, _ := g.allowLogin("8.8.8.8"); !ok {
		t.Fatal("另一个 IP 不该被连坐")
	}
}
