package main

// 面板登录（2026-09-22 用户要求「做一个登录页面接口」）。
//
// 背景：面板 8368 一直是裸奔的 —— 谁知道 IP:端口 就能打开，然后看见/改掉
// 所有 API Key、账号、反代令牌。这在"要卖中转"的场景下是致命的。
//
// 现在的口径：
//   - GET  /login          登录页（不需要登录）
//   - POST /api/login      {user,pass} → 下发 HttpOnly Cookie（HMAC 签名，7 天）
//   - POST /api/logout     清 Cookie
//   - POST /api/passwd     改密码（要已登录 + 旧密码）
//   - 其它所有路径（HTML 与 /api/*）都要带有效 Cookie：
//       HTML 没登录 → 302 跳 /login?next=...
//       /api/* 没登录 → 401 JSON（面板前端见到 401 会自动跳登录页）
//   - /healthz 与 /login、/api/login 例外（探活/登录本身不能被挡）
//
// 密码存 <data>/panel-auth.json：salt + sha256(salt+pass)，不存明文。
// 首次启动自动生成，默认账号 admin / 初始密码 blueapi2026，并打进日志提醒改密码。

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	authCookieName = "lb2a_sess"
	authTTL        = 7 * 24 * time.Hour
	defaultAuthPwd = "blueapi2026"
)

type panelAuth struct {
	mu      sync.Mutex
	fp      string
	User    string `json:"user"`
	Salt    string `json:"salt"`
	Hash    string `json:"pass_hash"`
	Secret  string `json:"secret"`  // 会话签名密钥
	Changed bool   `json:"changed"` // 用户自己改过密码没有（没改过就在日志里一直提醒）
	// Disabled 关掉登录闸门（2026-09-22 用户要求"把这个面板登录去掉"）。
	// 用"反向字段"是为了兼容：老文件里没这个 key → 零值 false → 登录照旧生效；
	// 想关掉：往 data/panel-auth.json 里加一句 "disabled": true 再重启面板任务。
	// 也可以用启动环境变量 LB2A_PANEL_AUTH=off（优先级更高，临时开门用）。
	Disabled bool `json:"disabled,omitempty"`
	// CaptchaDisabled 关掉数字验证码（登录页就不显示那一行）—— 后台管理页可以切
	CaptchaDisabled bool `json:"captcha_disabled,omitempty"`
	// Serial 页面上那个"第几个用户"的绿色圆牌编号（2026-09-22 用户要求：
	// 头像牌要显示"第几个注册的用户"，跟参考图里那个 13 一样）。
	// 面板只有一个管理员账号，所以做成**可自定义**的展示编号，默认 1，后台管理页能改。
	Serial int `json:"serial,omitempty"`
	// SerialSeq 开号计数的高水位（见过的最大序号）。现在只有一个管理员：
	// 创建时就 +1 并写进 Serial，所以第一个账号 = 1。
	// （2026-09-22 用户更正口径：牌子上的数字要按"第几个注册"算，不是随便填的）
	SerialSeq int `json:"serial_seq,omitempty"`
}

var panelAuthStore = &panelAuth{}

// hashPass salt + 明文 → hex(sha256)
func hashPass(salt, pass string) string {
	sum := sha256.Sum256([]byte(salt + "|" + pass))
	return hex.EncodeToString(sum[:])
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// initPanelAuth 载入（或首次生成）登录凭据
func initPanelAuth(fp string) {
	panelAuthStore.mu.Lock()
	defer panelAuthStore.mu.Unlock()
	panelAuthStore.fp = fp
	if raw, err := os.ReadFile(fp); err == nil {
		var a panelAuth
		if json.Unmarshal(raw, &a) == nil && a.User != "" && a.Hash != "" && a.Secret != "" {
			panelAuthStore.User, panelAuthStore.Salt = a.User, a.Salt
			panelAuthStore.Hash, panelAuthStore.Secret = a.Hash, a.Secret
			panelAuthStore.Changed = a.Changed
			panelAuthStore.Disabled = a.Disabled
			panelAuthStore.CaptchaDisabled = a.CaptchaDisabled
			panelAuthStore.Serial = a.Serial
			if panelAuthStore.Serial <= 0 {
				panelAuthStore.Serial = 1
			}
			panelAuthStore.SerialSeq = a.SerialSeq
			if panelAuthStore.SerialSeq < panelAuthStore.Serial {
				panelAuthStore.SerialSeq = panelAuthStore.Serial
			}
			applyAuthEnableOverride()
			if panelAuthStore.Disabled {
				log.Printf("面板登录：已关闭（凭据文件 %s 里 disabled=true）—— 打开面板不再需要密码", fp)
			} else {
				log.Printf("面板登录已启用：账号 %s（凭据文件 %s）%s", a.User, fp, map[bool]string{true: "", false: "｜提示：还在用初始密码，建议尽快在面板里改密码"}[a.Changed])
			}
			return
		}
	}
	panelAuthStore.User = "admin"
	panelAuthStore.Salt = randHex(16)
	panelAuthStore.Secret = randHex(32)
	panelAuthStore.Hash = hashPass(panelAuthStore.Salt, defaultAuthPwd)
	panelAuthStore.Changed = false
	// 建号即分配注册序号：这是面板的第一个账号 → 1
	panelAuthStore.SerialSeq++
	panelAuthStore.Serial = panelAuthStore.SerialSeq
	saveAuthLocked()
	log.Printf("面板登录已启用（首次生成）：账号 admin  初始密码 %s", defaultAuthPwd)
	log.Printf("  凭据文件：%s   登录地址：http://<服务器IP>:8368/login", fp)
	log.Printf("  请尽快登录后点右上角「改密码」——初始密码是公开的")
	applyAuthEnableOverride()
}

// applyAuthEnableOverride 环境变量临时开门/关门：LB2A_PANEL_AUTH=off 关、=on 开。
// 用户 2026-09-22 说"把这个面板登录去掉"，但我不想把代码删了 —— 留一个开关，
// 想开回来随时开（改文件或设环境变量都行，不用重新编译）。
func applyAuthEnableOverride() {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("LB2A_PANEL_AUTH")))
	switch v {
	case "off", "0", "false", "no", "disabled":
		panelAuthStore.Disabled = true
	case "on", "1", "true", "yes", "enabled":
		panelAuthStore.Disabled = false
	default:
		return
	}
	log.Printf("面板登录：环境变量 LB2A_PANEL_AUTH=%s → %s", v,
		map[bool]string{true: "已关闭（免登录，谁都能开面板）", false: "已开启"}[panelAuthStore.Disabled])
}

// isDisabled 登录闸门是否被关掉
func (a *panelAuth) isDisabled() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Disabled
}

func saveAuthLocked() {
	if panelAuthStore.fp == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(panelAuthStore.fp), 0755)
	b, _ := json.MarshalIndent(panelAuthStore, "", "  ")
	_ = os.WriteFile(panelAuthStore.fp, b, 0600)
}

// checkPass 校验账号密码（常数时间比较）
func (a *panelAuth) checkPass(user, pass string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.checkPassLocked(user, pass)
}

// checkPassLocked 同上，但调用方**已经持有锁**（后台管理页改账号/密码时用）
func (a *panelAuth) checkPassLocked(user, pass string) bool {
	if subtle.ConstantTimeCompare([]byte(user), []byte(a.User)) != 1 {
		return false
	}
	want := a.Hash
	got := hashPass(a.Salt, pass)
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

// newSession 生成会话串：<过期unix>.<hmac>
func (a *panelAuth) newSession() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	exp := strconv.FormatInt(time.Now().Add(authTTL).Unix(), 10)
	mac := hmac.New(sha256.New, []byte(a.Secret))
	_, _ = mac.Write([]byte(exp + "|" + a.User))
	return exp + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (a *panelAuth) validSession(v string) bool {
	if v == "" {
		return false
	}
	i := strings.IndexByte(v, '.')
	if i <= 0 {
		return false
	}
	expStr, sig := v[:i], v[i+1:]
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	a.mu.Lock()
	secret, user := a.Secret, a.User
	a.mu.Unlock()
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(expStr + "|" + user))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(sig))
}

// loggedIn 这次请求带了有效会话吗
func (a *panelAuth) loggedIn(r *http.Request) bool {
	c, err := r.Cookie(authCookieName)
	if err != nil {
		return false
	}
	return a.validSession(c.Value)
}

func (a *panelAuth) setCookie(w http.ResponseWriter, v string) {
	http.SetCookie(w, &http.Cookie{
		Name: authCookieName, Value: v, Path: "/",
		MaxAge: int(authTTL / time.Second), HttpOnly: true, SameSite: http.SameSiteLaxMode,
		// 面板是 http://IP:port 访问的，不能加 Secure，否则浏览器根本不回传
	})
}

func (a *panelAuth) clearCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: authCookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
}

// panelRecover 兜住面板任何 handler 的 panic。
//
// 主服务 2026-09-22 就加了（§89 发现过 24 次 panic 把连接裸断），面板一直没有 ——
// 面板要是 panic，客户端看到的就是"连接直接断"，而且面板任务的动作没有日志重定向，
// 连那一行线索都看不到。这里至少做到：完整栈进日志 + 回一个干净的 500。
func panelRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("[panel-panic] %s %s %s -> %v\n%s", r.Method, r.URL.Path, r.RemoteAddr, v, debug.Stack())
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, `{"ok":false,"error":"internal_panic","message":"面板内部错误（已记日志）"}`)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// authGate 把没登录的挡在门外
func authGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// 登录被关掉了（disabled=true 或 LB2A_PANEL_AUTH=off）→ 全放行
		if panelAuthStore.isDisabled() {
			next.ServeHTTP(w, r)
			return
		}
		// 例外：登录页/登录接口/健康检查/favicon —— 这些挡了就没法登录了
		// /api/authstate 也要放行：登录页要靠它知道"验证码开没开"，没登录时必须能问
		// （它只泄露"登录开没开 / 验证码开没开"这两个布尔，不是敏感信息）
		if p == "/login" || p == "/api/login" || p == "/api/captcha" || p == "/api/authstate" ||
			p == "/healthz" || p == "/favicon.ico" {
			next.ServeHTTP(w, r)
			return
		}
		// /v1/* 也放行：那是给客户端（Cherry Studio / SDK）调的 API，
		// 鉴权在核心那边按 sk- key 校验，面板登录管不着也不该管它。
		// 不放行的话，挂云隧道后 /v1 会被重定向到 /login，客户端直接调不通（2026-09-26 加）。
		if strings.HasPrefix(p, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		if panelAuthStore.loggedIn(r) {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(p, "/api/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in","message":"请先登录面板"}`)
			return
		}
		http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
	})
}

// handleLoginPage GET /login
func handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if panelAuthStore.isDisabled() {
		http.Redirect(w, r, "/", http.StatusFound) // 登录关了就没什么可登的
		return
	}
	if panelAuthStore.loggedIn(r) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, loginPage)
}

// handleLogin POST /api/login  {user,pass} → 下发会话 Cookie
func handleLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, `{"ok":false,"error":"method_not_allowed"}`)
		return
	}
	var req struct {
		User      string `json:"user"`
		Pass      string `json:"pass"`
		Captcha   string `json:"captcha"`
		CaptchaID string `json:"captchaID"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<14))
	_ = json.Unmarshal(body, &req)
	ip := clientIP(r)
	ua := r.Header.Get("User-Agent")
	// ① 限速/锁定（最先做，脚本连"读题"的机会都被卡住）
	if ok, reason, wait := loginGuard.allowLogin(ip); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())))
		w.WriteHeader(http.StatusTooManyRequests)
		if reason == "locked" {
			_, _ = io.WriteString(w, `{"ok":false,"error":"locked","message":"这个 IP 连续登录失败过多，已锁定 `+
				strconv.Itoa(int(wait.Minutes()))+` 分钟，请稍后再试"}`)
		} else {
			_, _ = io.WriteString(w, `{"ok":false,"error":"too_many_attempts","message":"登录尝试太频繁，请等一分钟再试"}`)
		}
		log.Printf("[panel-login] 拒绝（%s）：ip=%s wait=%s", reason, ip, wait)
		noteLoginRec(ip, ua, reason)
		return
	}
	// 先验验证码（便宜、且能把"拿一个码反复撞密码"这条路直接堵死）
	// 后台管理页可以把验证码关掉（captcha_disabled=true）→ 这一步直接跳过
	if !panelAuthStore.captchaOff() && !checkCaptcha(strings.TrimSpace(req.CaptchaID), req.Captcha) {
		locked, lockFor := loginGuard.noteLoginFail(ip)
		log.Printf("[panel-login] 验证码不通过：ip=%s 连错=%v", ip, locked)
		noteLoginRec(ip, ua, "bad_captcha")
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"bad_captcha","message":"验证码不对或已过期，点一下数字换一个"}`)
		if locked {
			log.Printf("[panel-login] IP 已锁定 %v：%s", lockFor, ip)
		}
		return
	}
	if !panelAuthStore.checkPass(strings.TrimSpace(req.User), req.Pass) {
		locked, lockFor := loginGuard.noteLoginFail(ip)
		log.Printf("[panel-login] 失败：user=%q ip=%s", req.User, clientIP(r))
		noteLoginRec(ip, ua, "bad_pass")
		time.Sleep(600 * time.Millisecond) // 拖一下，防暴力猜
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"bad_credentials","message":"账号或密码不对"}`)
		if locked {
			log.Printf("[panel-login] IP 已锁定 %v：%s", lockFor, ip)
		}
		return
	}
	panelAuthStore.setCookie(w, panelAuthStore.newSession())
	loginGuard.noteLoginOK(ip)
	noteLoginRec(ip, ua, "ok")
	log.Printf("[panel-login] 成功：user=%s ip=%s", req.User, clientIP(r))
	if panelAuthStore.isDefaultPwd() {
		_, _ = io.WriteString(w, `{"ok":true,"warn":"还在用初始密码，建议点右上角「改密码」"}`)
		return
	}
	_, _ = io.WriteString(w, `{"ok":true}`)
}

// handleLogout POST /api/logout
func handleLogout(w http.ResponseWriter, r *http.Request) {
	panelAuthStore.clearCookie(w)
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true}`)
}

// handleAuthState GET /api/authstate —— 面板前端用它决定要不要显示「改密码/退出」。
// 登录被关掉时（disabled=true），前端就把这两个按钮藏起来，免得点了没反应。
func handleAuthState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true,"enabled":`+map[bool]string{true: "true", false: "false"}[!panelAuthStore.isDisabled()]+
		`,"captcha_enabled":`+map[bool]string{true: "true", false: "false"}[!panelAuthStore.captchaOff()]+`}`)
}

// ---- 数字验证码（2026-09-22 用户要求：把登录页红圈那段说明文字换成数字验证码）----
//
// 口径：
//   - 4 位纯数字，服务端内存保存，5 分钟有效
//   - **一次性**：不管输对输错，验过一次就作废（防止拿一个码反复撞密码）
//   - 答案只在服务端，客户端只拿一个随机 id（不把答案塞进 cookie）
//   - 登录时**先验验证码再验密码**（这样暴力猜密码的请求在第一步就被挡掉）
var (
	captchaMu  sync.Mutex
	captchaMap = map[string]captchaEntry{}
)

type captchaEntry struct {
	code string
	exp  time.Time
}

// randInt 取 [0,n) 的随机整数（用 crypto/rand，失败就退回时间戳）
func randInt(n int) int {
	if n <= 0 {
		return 0
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return int(time.Now().UnixNano() % int64(n))
	}
	v := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	if v < 0 {
		v = -v
	}
	return v % n
}

func newCaptcha() (id, code string) {
	code = fmt.Sprintf("%04d", randInt(10000))
	id = randHex(8)
	captchaMu.Lock()
	now := time.Now()
	for k, v := range captchaMap { // 顺手清过期的，别让 map 无限长
		if now.After(v.exp) {
			delete(captchaMap, k)
		}
	}
	captchaMap[id] = captchaEntry{code: code, exp: now.Add(5 * time.Minute)}
	captchaMu.Unlock()
	return id, code
}

// checkCaptcha 校验并作废（一次性）
func checkCaptcha(id, in string) bool {
	in = strings.TrimSpace(in)
	if id == "" || in == "" {
		return false
	}
	captchaMu.Lock()
	e, ok := captchaMap[id]
	delete(captchaMap, id)
	captchaMu.Unlock()
	if !ok || time.Now().After(e.exp) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(e.code), []byte(in)) == 1
}

// handleCaptcha GET /api/captcha —— 发一个数字验证码（登录页显示用）
func handleCaptcha(w http.ResponseWriter, r *http.Request) {
	if !loginGuard.allowCaptcha(clientIP(r)) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"ok":false,"error":"too_many_captcha","message":"要验证码太频繁了，等一分钟再试"}`)
		return
	}
	id, code := newCaptcha()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, `{"ok":true,"id":"`+id+`","code":"`+code+`"}`)
}

// handlePasswd POST /api/passwd {old,new} —— 改密码（改完所有会话失效，要重新登录）
func handlePasswd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !panelAuthStore.loggedIn(r) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in"}`)
		return
	}
	var req struct {
		Old string `json:"old"`
		New string `json:"new"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<14))
	_ = json.Unmarshal(body, &req)
	if len(req.New) < 6 {
		_, _ = io.WriteString(w, `{"ok":false,"error":"short","message":"新密码至少 6 位"}`)
		return
	}
	if !panelAuthStore.checkPass(panelAuthStore.userName(), req.Old) {
		_, _ = io.WriteString(w, `{"ok":false,"error":"bad_old","message":"旧密码不对"}`)
		return
	}
	panelAuthStore.mu.Lock()
	panelAuthStore.Salt = randHex(16)
	panelAuthStore.Hash = hashPass(panelAuthStore.Salt, req.New)
	panelAuthStore.Secret = randHex(32) // 换密钥 = 所有旧会话立刻失效
	panelAuthStore.Changed = true
	saveAuthLocked()
	panelAuthStore.mu.Unlock()
	panelAuthStore.clearCookie(w)
	log.Printf("[panel-login] 密码已修改（旧会话全部失效）")
	_, _ = io.WriteString(w, `{"ok":true,"message":"密码已改，请用新密码重新登录"}`)
}

func (a *panelAuth) userName() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.User
}

func (a *panelAuth) isDefaultPwd() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.Changed && hashPass(a.Salt, defaultAuthPwd) == a.Hash
}

// 注：clientIP 已有实现（main.go 里那个，给访问记录用的），这里直接复用、不重复定义。
// ---- 按 IP 限速 + 连错锁定（2026-09-22 用户要求）----
//
// 为什么需要：验证码答案要显示在页面上，所以它是**明文返回**的 ——
// 脚本能"自己读题再作答"，验证码对脚本没防护作用。真正能挡住脚本的是**次数**：
// 发码有上限、密码连错就锁这个 IP。
//
// 默认阈值（都能用环境变量覆盖，不用改代码）：
//
//	LB2A_LOGIN_RPM    每 IP 每分钟最多几次「登录尝试」   默认 10
//	LB2A_CAPTCHA_RPM  每 IP 每分钟最多几次「发验证码」   默认 30
//	LB2A_LOGIN_FAILS  连续错几次就锁 IP                 默认 5
//	LB2A_LOCK_MIN     锁多久（分钟）                    默认 10
//
// 被锁的 IP：登录和发码都直接 429 + Retry-After；成功登录会把计数清零。
// 状态只在内存里，重启面板即清空（不落盘，免得误锁把自己关在门外）。
type ipGuard struct {
	mu  sync.Mutex
	ips map[string]*ipState
}

type ipState struct {
	loginHits  []time.Time
	capHits    []time.Time
	fails      int
	lockedTill time.Time
	lastSeen   time.Time
}

var loginGuard = &ipGuard{ips: map[string]*ipState{}}

func envInt(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func (g *ipGuard) stateLocked(ip string, now time.Time) *ipState {
	st := g.ips[ip]
	if st == nil {
		st = &ipState{}
		g.ips[ip] = st
	}
	st.lastSeen = now
	if len(g.ips) > 200 { // 顺手清掉一小时没动静的 IP，别让 map 越滚越大
		for k, v := range g.ips {
			if now.Sub(v.lastSeen) > time.Hour {
				delete(g.ips, k)
			}
		}
	}
	return st
}

// pruneHits 只留最近一分钟内的记录（调用方需持有锁）
func pruneHits(h []time.Time, now time.Time) []time.Time {
	out := h[:0]
	for _, t := range h {
		if now.Sub(t) < time.Minute {
			out = append(out, t)
		}
	}
	return out
}

// allowLogin 这次登录尝试放不放行；不放行给出原因和要等多久
func (g *ipGuard) allowLogin(ip string) (bool, string, time.Duration) {
	now := time.Now()
	rpm := envInt("LB2A_LOGIN_RPM", 10)
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.stateLocked(ip, now)
	if now.Before(st.lockedTill) {
		return false, "locked", time.Until(st.lockedTill).Truncate(time.Second)
	}
	st.loginHits = pruneHits(st.loginHits, now)
	if len(st.loginHits) >= rpm {
		return false, "rate", time.Minute
	}
	st.loginHits = append(st.loginHits, now)
	return true, "", 0
}

// noteLoginFail 记一次失败；连错到阈值就把这个 IP 锁起来
func (g *ipGuard) noteLoginFail(ip string) (bool, time.Duration) {
	now := time.Now()
	maxFails := envInt("LB2A_LOGIN_FAILS", 5)
	lockMin := envInt("LB2A_LOCK_MIN", 10)
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.stateLocked(ip, now)
	st.fails++
	if st.fails >= maxFails {
		st.lockedTill = now.Add(time.Duration(lockMin) * time.Minute)
		st.fails = 0 // 锁到期后重新给几次机会
		return true, time.Duration(lockMin) * time.Minute
	}
	return false, 0
}

// noteLoginOK 登录成功：清掉失败计数和锁定
func (g *ipGuard) noteLoginOK(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if st := g.ips[ip]; st != nil {
		st.fails = 0
		st.lockedTill = time.Time{}
	}
}

// allowCaptcha 发码限速（被锁的 IP 连码都不给）
func (g *ipGuard) allowCaptcha(ip string) bool {
	now := time.Now()
	rpm := envInt("LB2A_CAPTCHA_RPM", 30)
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.stateLocked(ip, now)
	if now.Before(st.lockedTill) {
		return false
	}
	st.capHits = pruneHits(st.capHits, now)
	if len(st.capHits) >= rpm {
		return false
	}
	st.capHits = append(st.capHits, now)
	return true
}

// ---- 登录记录（后台管理页要能看"谁在试、锁了谁"）----
//
// 落盘到 data/panel-logins.json，只留最近 200 条（小文件，直接覆盖写）。
type loginRec struct {
	At     time.Time `json:"at"`
	IP     string    `json:"ip"`
	UA     string    `json:"ua,omitempty"`
	Result string    `json:"result"` // ok / bad_pass / bad_captcha / locked / rate
}

var (
	loginRecMu sync.Mutex
	loginRecs  []loginRec
	loginRecFp string
)

func initLoginRecs(fp string) {
	loginRecMu.Lock()
	defer loginRecMu.Unlock()
	loginRecFp = fp
	if raw, err := os.ReadFile(fp); err == nil {
		var d struct {
			Recs []loginRec `json:"recs"`
		}
		if json.Unmarshal(raw, &d) == nil {
			loginRecs = d.Recs
		}
	}
}

func noteLoginRec(ip, ua, result string) {
	loginRecMu.Lock()
	defer loginRecMu.Unlock()
	if ua != "" && len(ua) > 80 {
		ua = ua[:80]
	}
	loginRecs = append(loginRecs, loginRec{At: time.Now(), IP: ip, UA: ua, Result: result})
	if len(loginRecs) > 200 {
		loginRecs = loginRecs[len(loginRecs)-200:]
	}
	if loginRecFp == "" {
		return
	}
	b, _ := json.MarshalIndent(map[string]any{"recs": loginRecs, "saved_at": time.Now()}, "", "  ")
	_ = os.MkdirAll(filepath.Dir(loginRecFp), 0755)
	_ = os.WriteFile(loginRecFp, b, 0600)
}

func loginRecSnapshot(n int) []loginRec {
	loginRecMu.Lock()
	defer loginRecMu.Unlock()
	if n <= 0 || n > len(loginRecs) {
		n = len(loginRecs)
	}
	out := make([]loginRec, 0, n)
	for i := len(loginRecs) - 1; i >= 0 && len(out) < n; i-- { // 新的在前
		out = append(out, loginRecs[i])
	}
	return out
}

// clearAll 清掉所有 IP 的限速/锁定（后台管理页「解锁所有 IP」）；返回清掉几个被锁的
func (g *ipGuard) clearAll() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	n := 0
	for k, v := range g.ips {
		if now.Before(v.lockedTill) {
			n++
		}
		delete(g.ips, k)
	}
	return n
}

// lockedIPs 当前还被锁着的 IP（后台管理页显示用）
func (g *ipGuard) lockedIPs() []map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	out := []map[string]any{}
	for ip, v := range g.ips {
		if now.Before(v.lockedTill) {
			out = append(out, map[string]any{
				"ip": ip, "until": v.lockedTill, "remain_sec": int(time.Until(v.lockedTill).Seconds()),
			})
		}
	}
	return out
}

// handleMe GET /api/me —— 面板右上角那个"用户牌"用：当前账号 + 展示编号
// （要登录；不给没登录的人看账号名）
func handleMe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !panelAuthStore.loggedIn(r) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in"}`)
		return
	}
	panelAuthStore.mu.Lock()
	user, serial, changed := panelAuthStore.User, panelAuthStore.Serial, panelAuthStore.Changed
	panelAuthStore.mu.Unlock()
	if serial <= 0 {
		serial = 1
	}
	out, _ := json.Marshal(map[string]any{"ok": true, "user": user, "serial": serial, "changed": changed})
	_, _ = w.Write(out)
}

// handleAdminState GET /api/admin/state —— 后台管理页要的全部状态
func handleAdminState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// 注意：登录**被关掉**时也放行 —— 否则后台管理页自己就进不去，
	// 也就没法在页面上把登录重新打开（2026-09-22 自检发现的死锁）。
	// 这不降低安全性：登录关着的时候，整个面板本来就是谁都能用的。
	if !panelAuthStore.loggedIn(r) && !panelAuthStore.isDisabled() {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in"}`)
		return
	}
	panelAuthStore.mu.Lock()
	user, changed, disabled, capOff, serial := panelAuthStore.User, panelAuthStore.Changed,
		panelAuthStore.Disabled, panelAuthStore.CaptchaDisabled, panelAuthStore.Serial
	panelAuthStore.mu.Unlock()
	if serial <= 0 {
		serial = 1
	}
	resp := map[string]any{
		"ok":              true,
		"user":            user,
		"serial":          serial,
		"changed":         changed,
		"login_enabled":   !disabled,
		"captcha_enabled": !capOff,
		"limits": map[string]any{
			"login_rpm":   envInt("LB2A_LOGIN_RPM", 10),
			"captcha_rpm": envInt("LB2A_CAPTCHA_RPM", 30),
			"fails":       envInt("LB2A_LOGIN_FAILS", 5),
			"lock_min":    envInt("LB2A_LOCK_MIN", 10),
		},
		"locked": loginGuard.lockedIPs(),
		"logins": loginRecSnapshot(50),
		"now":    time.Now(),
	}
	b, _ := json.Marshal(resp)
	_, _ = w.Write(b)
}

// handleAdminSet POST /api/admin/set —— 后台管理页的保存
//
// body（都能单独给）：
//
//	{"login_enabled":true|false}     面板登录开/关
//	{"captcha_enabled":true|false}   数字验证码开/关
//	{"user":"新账号","old_pass":"..."}   改账号（要旧密码）
//	{"old_pass":"...","new_pass":"..."}  改密码（要旧密码；改完所有会话失效）
func handleAdminSet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// 同 handleAdminState：登录关着的时候也放行，否则没法"开回来"
	if !panelAuthStore.loggedIn(r) && !panelAuthStore.isDisabled() {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in"}`)
		return
	}
	var req struct {
		LoginEnabled   *bool  `json:"login_enabled"`
		CaptchaEnabled *bool  `json:"captcha_enabled"`
		User           string `json:"user"`
		OldPass        string `json:"old_pass"`
		NewPass        string `json:"new_pass"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<14))
	_ = json.Unmarshal(body, &req)

	msgs := []string{}
	needRelogin := false
	// 2026-09-22 §111：以前不管有没有改动都会写一次凭据文件（传 {} 也写），
	// 把手工编辑过的文件（比如带 BOM 的）冲掉。现在只在真改了才落盘。
	changed := false
	panelAuthStore.mu.Lock()
	if req.LoginEnabled != nil {
		if panelAuthStore.Disabled != !*req.LoginEnabled {
			changed = true
		}
		panelAuthStore.Disabled = !*req.LoginEnabled
		if panelAuthStore.Disabled {
			msgs = append(msgs, "面板登录已关闭")
		} else {
			msgs = append(msgs, "面板登录已开启")
		}
	}
	if req.CaptchaEnabled != nil {
		if panelAuthStore.CaptchaDisabled != !*req.CaptchaEnabled {
			changed = true
		}
		panelAuthStore.CaptchaDisabled = !*req.CaptchaEnabled
		if panelAuthStore.CaptchaDisabled {
			msgs = append(msgs, "验证码已关闭")
		} else {
			msgs = append(msgs, "验证码已开启")
		}
	}
	newUser := strings.TrimSpace(req.User)
	wantUser := newUser != "" && newUser != panelAuthStore.User
	wantPass := strings.TrimSpace(req.NewPass) != ""
	if wantUser || wantPass {
		// 改账号/密码都必须先验旧密码（防止有人蹭了会话就改门锁）
		if !panelAuthStore.checkPassLocked(panelAuthStore.User, req.OldPass) {
			panelAuthStore.mu.Unlock()
			_, _ = io.WriteString(w, `{"ok":false,"error":"bad_old","message":"旧密码不对"}`)
			return
		}
	}
	if wantPass {
		if len(req.NewPass) < 6 {
			panelAuthStore.mu.Unlock()
			_, _ = io.WriteString(w, `{"ok":false,"error":"short","message":"新密码至少 6 位"}`)
			return
		}
		panelAuthStore.Salt = randHex(16)
		panelAuthStore.Hash = hashPass(panelAuthStore.Salt, req.NewPass)
		panelAuthStore.Secret = randHex(32)
		panelAuthStore.Changed = true
		changed = true
		msgs = append(msgs, "密码已改（所有登录状态失效，请重新登录）")
		needRelogin = true
	}
	if wantUser {
		panelAuthStore.User = newUser
		panelAuthStore.Secret = randHex(32) // 会话签名里带 user，换账号等于旧会话全废
		changed = true
		msgs = append(msgs, "账号已改成 "+newUser+"（请重新登录）")
		needRelogin = true
	}
	if changed {
		saveAuthLocked()
	}
	panelAuthStore.mu.Unlock()
	if needRelogin {
		panelAuthStore.clearCookie(w)
	}
	if len(msgs) == 0 {
		msgs = append(msgs, "没有需要保存的改动")
	}
	out, _ := json.Marshal(map[string]any{"ok": true, "relogin": needRelogin, "message": strings.Join(msgs, "；")})
	_, _ = w.Write(out)
}

// handleAdminUnlock POST /api/admin/unlock —— 清掉所有 IP 的限速/锁定
func handleAdminUnlock(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !panelAuthStore.loggedIn(r) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"ok":false,"error":"not_logged_in"}`)
		return
	}
	n := loginGuard.clearAll()
	_, _ = io.WriteString(w, `{"ok":true,"message":"已清掉 `+strconv.Itoa(n)+` 个被锁的 IP（限速计数也一起清零）"}`)
}

// (a *panelAuth) 便捷取用
func (a *panelAuth) captchaOff() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.CaptchaDisabled
}

// loginPage 登录页（自包含，不依赖面板那份大 HTML）
const loginPage = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<link rel="icon" href="data:,">
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BlueAPI 面板 · 登录</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif; background:#f5f7fa; color:#1f2937;
       min-height:100vh; display:flex; align-items:center; justify-content:center; padding:20px; }
.box { width:100%; max-width:360px; background:#ffffff; border:1px solid #e5e7eb; border-radius:14px;
       box-shadow:0 6px 24px rgba(15,23,42,.06); padding:26px 24px; }
.brand { font-size:17px; font-weight:800; margin-bottom:4px; }
.sub { font-size:12px; color:#6b7280; margin-bottom:18px; }
label { display:block; font-size:12px; color:#6b7280; margin:12px 0 4px; }
input { width:100%; padding:10px 12px; font-size:14px; font-family:inherit; color:#111827;
        background:#ffffff; border:1px solid #d1d5db; border-radius:8px; }
input:focus { outline:none; border-color:#2563eb; }
button { width:100%; margin-top:18px; padding:11px 14px; font-size:14px; font-weight:600; font-family:inherit;
         color:#ffffff; background:#2563eb; border:1px solid #2563eb; border-radius:8px; cursor:pointer; }
button:hover { background:#1d4ed8; }
button[disabled] { opacity:.6; cursor:not-allowed; }
.msg { margin-top:12px; font-size:12px; min-height:16px; }
.err { color:#dc2626; }
.warn { color:#b45309; }
/* 数字验证码那一行 */
.cap-row { display:flex; gap:8px; align-items:center; margin-top:12px; }
.cap-code { flex:0 0 auto; min-width:96px; text-align:center; font-family:Consolas,monospace;
            font-size:20px; font-weight:700; letter-spacing:5px; color:#1d4ed8;
            background:#eef4ff; border:1px dashed #93c5fd; border-radius:8px; padding:7px 10px;
            cursor:pointer; user-select:none; }
.cap-row input { flex:1 1 auto; }
.cap-ref { flex:0 0 auto; white-space:nowrap; width:auto; margin:0; padding:0 10px; height:38px;
           font-size:12px; font-weight:600; color:#374151; background:#f3f4f6; border:1px solid #d1d5db; }
.cap-ref:hover { background:#e5e7eb; color:#111827; }
.foot { margin-top:16px; font-size:11px; color:#9ca3af; line-height:1.7; }
</style>
</head>
<body>
<form class="box" id="f">
  <div class="brand">&#128311; BlueAPI</div>
  <label>账号</label>
  <input id="u" autocomplete="username" autofocus>
  <label>密码</label>
  <input id="p" type="password" autocomplete="current-password">
  <div id="caprow">
    <div class="cap-row">
      <div class="cap-code" id="cap" title="点一下换一个验证码">----</div>
      <input id="c" inputmode="numeric" autocomplete="off" maxlength="4" placeholder="验证码">
      <button type="button" id="capref" class="cap-ref">换一个</button>
    </div>
  </div>
  <button id="b" type="submit">登录</button>
  <div class="msg" id="m"></div>
</form>
<script>
function $(id) { return document.getElementById(id); }
function next() {
  try {
    var q = new URLSearchParams(location.search);
    var n = q.get('next') || '/';
    // 2026-09-22 §111：防"登录后跳外站"的钓鱼（open redirect）。
    // 以前只挡 「//evil.com」 和 「http...」，但浏览器把 「/\evil.com」 也当
    // 「//evil.com」处理，所以反斜杠必须一律拒掉，且必须是站内单斜杠开头。
    if (n.charAt(0) !== '/') return '/';
    if (n.charAt(1) === '/' || n.charAt(1) === '\\') return '/';
    if (n.indexOf('\\') >= 0) return '/';
    if (n.indexOf('://') >= 0) return '/';
    return n;
  } catch (e) { return '/'; }
}
// 数字验证码：点数字本身或「换一个」都能刷新
var capId = '';
async function loadCap() {
  if ($('caprow') && $('caprow').style.display === 'none') return; // 验证码被后台关掉了
  try {
    var r = await fetch('/api/captcha', {cache:'no-store'});
    var d = await r.json();
    capId = (d && d.id) || '';
    $('cap').textContent = (d && d.code) || '----';
    $('c').value = '';
  } catch (e) {
    $('cap').textContent = '----';
  }
}
$('cap').onclick = loadCap;
$('capref').onclick = loadCap;
loadCap();
// 后台管理页可以把验证码关掉 → 这里就不显示那一行（不然填了也没用）
fetch('/api/authstate', {cache:'no-store'}).then(function(r){ return r.json(); }).then(function(d){
  if (d && d.captcha_enabled === false && $('caprow')) $('caprow').style.display = 'none';
}).catch(function(){});
$('f').onsubmit = async function(ev) {
  ev.preventDefault();
  $('b').disabled = true;
  $('m').className = 'msg';
  $('m').textContent = '登录中…';
  try {
    var r = await fetch('/api/login', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        user: $('u').value.trim(),
        pass: $('p').value,
        captcha: $('c').value.trim(),
        captchaID: capId
      })
    });
    var d = null;
    try { d = await r.json(); } catch (e) {}
    if (r.ok && d && d.ok) {
      $('m').className = 'msg';
      $('m').textContent = '登录成功，正在进入…';
      location.href = next();
      return;
    }
    $('m').className = 'msg err';
    $('m').textContent = (d && d.message) || '登录失败';
  } catch (e) {
    $('m').className = 'msg err';
    $('m').textContent = '请求失败：' + e.message;
  }
  $('b').disabled = false;
  loadCap();   // 验证码是一次性的：失败后自动换一个
};
</script>
</body>
</html>
`
