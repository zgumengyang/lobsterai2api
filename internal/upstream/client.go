// Package upstream 封装对 LobsterAI 上游的全部 HTTP 调用。
package upstream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"lobsterai2api/internal/auth"
)

const (
	clientVersion = "0.1.0"
	clientUA      = "LobsterAI/0.1.0"
)

// ServerBase returns the upstream API base URL from LB2A_UPSTREAM_BASE env.
// No hardcoded domain — users must set this in their config or environment.
func ServerBase() string {
	if v := os.Getenv("LB2A_UPSTREAM_BASE"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return ""
}

// apiEnvelope 上游统一信封。
type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// Client 上游 HTTP 客户端。
type Client struct {
	HTTP     *http.Client
	LastBody []byte // 最近一次非 2xx 响应体，供调用方 Classify
}

// New 生产默认值。配置连接池减少 TLS 握手。
func New() *Client {
	tr := &http.Transport{
		// 关闭连接复用：跑一段时间后上游 SLB 会对复用的老 keep-alive 连接回 500，
		// 而每次新建连接（=curl 行为）实测 100% 200。牺牲少量握手开销换取稳定。
		DisableKeepAlives: true,
		// 上游迟迟不吐响应头时快速失败、快速换号（治"180 秒长蛇阵 + 批量同时冷却"）。
		// 只限首字节等待，不影响后续长思考流的读取。
		ResponseHeaderTimeout: 90 * time.Second,
		// HTTP/2 保持默认（实测 h2/h1.1 对上游都 200，不是 500 根因）
	}
	// 拨号旁路：设了 LB2A_DEBUG_NET 就把每次拨号的目标地址/本地地址打进日志，
	// 排查 upstream 500 时直接看到请求走的是哪个 IP（v4/v6、哪个 SLB 节点）。
	if os.Getenv("LB2A_DEBUG_NET") != "" {
		d := &net.Dialer{Timeout: 30 * time.Second}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			// 打印实际解析结果：纯 Go resolver vs 系统 resolver 的差异就在这里暴露
			if ips, err := net.LookupIP(strings.Split(addr, ":")[0]); err == nil {
				s := make([]string, 0, len(ips))
				for _, ip := range ips {
					s = append(s, ip.String())
				}
				log.Printf("[resolve] %s -> %s", addr, strings.Join(s, ","))
			} else {
				log.Printf("[resolve-fail] %s: %v", addr, err)
			}
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				log.Printf("[dial-fail] %s -> %s: %v", network, addr, err)
				return nil, err
			}
			log.Printf("[dial] %s -> %s (local %s)", network, addr, conn.LocalAddr().String())
			return conn, nil
		}
	}
	return &Client{
		HTTP: &http.Client{Timeout: 180 * time.Second, Transport: tr},
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n]
	}
	return s
}

// doJSON 发请求并解信封；HTTP 非 2xx 或业务 code != 0 时返回带 body 片段的 *Error。
func (c *Client) doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	c.LastBody = raw
	if resp.StatusCode >= 400 {
		kind := Classify(resp.StatusCode, string(raw))
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: truncate(string(raw), 200)}
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("parse failed: %w (body: %s)", err, truncate(string(raw), 120))
	}
	if env.Code != 0 {
		kind := Classify(resp.StatusCode, env.Msg)
		if kind == ErrNone {
			kind = ErrClient
		}
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: fmt.Sprintf("code=%d msg=%s", env.Code, truncate(env.Msg, 160))}
	}
	return env.Data, nil
}

// chatHeaders 设置 chat completions 请求头。
func chatHeaders(req *http.Request, a *auth.Auth) {
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream, application/json")
	req.Header.Set("User-Agent", clientUA)
	// 必须单独声明这个 capability，lobsterai_options 的思考级别控制才被上游接受
	// （实测带上旧的 kimi-k3-agentic-v1 或空格分隔多值都会报
	//  "lobsterai_options: thinking-level-control-v1 capability is required"）
	req.Header.Set("X-LobsterAI-Client-Capabilities", "thinking-level-control-v1")
	req.Header.Set("X-LobsterAI-Client-Version", clientVersion)
}

// authHeaders 设置 auth 请求头（exchange/refresh 不需要 Bearer token）。
func authHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
}

// RefreshToken 刷新 access token；成功时更新 a 的字段（缺省值保留旧值），
// 调用方负责 SaveAtomic。
func (c *Client) RefreshToken(a *auth.Auth) error {
	if strings.TrimSpace(a.RefreshToken) == "" {
		return fmt.Errorf("no refreshToken")
	}
	url := ServerBase() + "/api/auth/refresh"
	body := a.KeyfromBody()
	body["refreshToken"] = a.RefreshToken
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	authHeaders(req)
	data, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var tok struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"`
	}
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return fmt.Errorf("refresh_failed: no accessToken in response — re-login required")
	}
	a.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		a.RefreshToken = tok.RefreshToken
	}
	if tok.ExpiresIn > 0 {
		a.ExpiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	} else if exp := jwtExpiry(tok.AccessToken); exp > 0 {
		a.ExpiresAt = exp
	}
	return nil
}

// jwtExpiry 解码 JWT payload 的 exp（Unix 秒）；失败返回 0。
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
	if json.Unmarshal(payload, &claims) != nil || claims.Exp <= 0 {
		return 0
	}
	return claims.Exp
}

// prepareChatBody 预处理请求体：force stream=true（上游只支持流式，实测 stream:false 返回 500），
// 标准化 tool_choice。
// thinkingLevelFromBody 推断思考级别：客户端显式指定优先，否则取 env LB2A_THINKING_LEVEL，
// 都未指定时用 "off"（优先保证响应速度，避免"一直在思考"）。
func thinkingLevelFromBody(body map[string]any) string {
	if t, ok := body["thinking"].(map[string]any); ok {
		if lv, ok := t["level"].(string); ok && strings.TrimSpace(lv) != "" {
			return normalizeThinkingLevel(lv)
		}
	}
	if re, ok := body["reasoning_effort"].(string); ok && strings.TrimSpace(re) != "" {
		return normalizeThinkingLevel(re)
	}
	if def := os.Getenv("LB2A_THINKING_LEVEL"); strings.TrimSpace(def) != "" {
		return normalizeThinkingLevel(def)
	}
	return "off"
}

// normalizeThinkingLevel 把各种写法归一到上游认的三个值：off / high / max。
func normalizeThinkingLevel(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "off", "none", "minimal", "low", "disabled", "disable", "false", "0":
		return "off"
	case "max", "xhigh", "ultra", "highest":
		return "max"
	default:
		return "high"
	}
}

// alwaysThinkMarkers 上游明确表示"这个模型的思考级别不接受"的几类回复。
//
// 2026-09-22 实测（Tier 停用、模型全落账号池后暴露出来的）：
//
//	glm-5.3    -> {"message":"该模型始终思考，不支持关闭思考；请使用 low、high 或 max。","code":50201}
//	MiniMax-M3 -> {"message":"lobsterai_options: model does not have a valid thinkingConfig","code":4000}
//
// 这两种都是被我们**默认注入的 level=off** 顶死的 —— 于是站点列表里明明有这些模型，一请求就报错。
var alwaysThinkMarkers = []string{
	"不支持关闭思考", "始终思考", "does not have a valid thinkingConfig",
	"thinkingConfig", "thinking-level-control", "not support thinking off",
	"unsupported thinking level",
	// 2026-09-22 实测补充：有道对"思考模式不被这个模型接受"有时会回一个**很误导的**
	// `模型不可见或无访问权限` (40301)。实测同一个模型 MiniMax-M3 在"不发 lobsterai_options"
	// 之后就能正常出内容 → 说明这条 40301 里有相当一部分其实是形态问题，不是权限问题，
	// 所以也让它走一遍重试阶梯（最坏情况：本来就没权限的模型多花两次上游调用，最终报错不变）。
	"模型不可见或无访问权限",
}

// accessMarkers "这个号没有这个模型的权限"（权限按账号算）。
//
// 为什么要和形态错误分开（2026-09-22 二次实测）：
//
//	40301 既可能来自"思考级别不被接受"，也可能来自"这个号真没权限"。
//	区分办法很简单：**形态已经被证明过是对的**（该模型已锁定形态）还回 40301，
//	那就只能是权限问题 → 立刻交给调用方换号，别在形态阶梯上白烧 3 次上游调用。
var accessMarkers = []string{"模型不可见或无访问权限", "40301", "没有访问权限", "无权限"}

func isAccessError(body []byte) bool {
	s := string(body)
	for _, m := range accessMarkers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// needsThinkingRetry 这次上游报错是不是"思考级别不对"造成的
func needsThinkingRetry(body []byte) bool {
	s := string(body)
	for _, m := range alwaysThinkMarkers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// prepareChatBody 默认思考级别（跟着请求/环境变量走）
func prepareChatBody(rawBody []byte) []byte {
	return prepareChatBodyLevel(rawBody, "")
}

// prepareChatBodyNoOptions 完全不注入 lobsterai_options（重试用）。
//
// MiniMax-M3 / kimi-k2.7-code 实测回的是
//
//	{"message":"lobsterai_options: model does not have a valid thinkingConfig","code":4000}
//
// —— 连"合法的 thinkingConfig"都不认，那唯一可能是这个模型根本不吃这个字段，只能剥掉再试。
func prepareChatBodyNoOptions(rawBody []byte) []byte {
	var body map[string]any
	if err := json.Unmarshal(rawBody, &body); err != nil {
		return rawBody
	}
	body["stream"] = true
	delete(body, "thinking")
	delete(body, "reasoning_effort")
	delete(body, "lobsterai_options")
	out, err := json.Marshal(body)
	if err != nil {
		return rawBody
	}
	return out
}

// HeadError 上游把错误**塞在 200 流的第一块**里（不是 HTTP 4xx）。
//
// 2026-09-22 实测（这条最坑）：池子里 7 个号，发同一个 qwen3.8-flash，
// 4 次里 2 次正常、2 次回 `event:error {"message":"模型不可见或无访问权限: qwen3.8-flash","code":40301}`
// —— 也就是说**访问权限是按账号算的**，有的号有这个模型、有的没有。
// 旧代码只看 HTTP 状态码（200），于是既没换号也没报错，客户端就吃到一个空/错的回复。
// 现在把它抽成类型化错误，让池子**换号重试**（换到有权限的那个号）。
type HeadError struct {
	Status int
	Body   []byte
}

func (e *HeadError) Error() string {
	return fmt.Sprintf("上游流首报错 (http %d): %s", e.Status, truncate(string(e.Body), 200))
}

// headIsError 流首那块是不是错误块（`event:error` 或带 error/code 的对象）
func headIsError(head []byte) bool {
	s := string(head)
	if strings.Contains(s, "event:error") || strings.Contains(s, "event: error") {
		return true
	}
	low := strings.ToLower(s)
	return strings.Contains(low, "proxy_error") || strings.Contains(low, "upstream_error")
}

// multiReadCloser 把"预读出来的头"和"剩下的流"拼回一个 ReadCloser
type multiReadCloser struct {
	r io.Reader
	c io.Closer
}

func (m *multiReadCloser) Read(p []byte) (int, error) { return m.r.Read(p) }
func (m *multiReadCloser) Close() error               { return m.c.Close() }

// peekChatHead 预读上游 SSE 的开头（最多 4 行 / 4KB，遇到空行就停）。
//
// 为什么需要（2026-09-22 实测）：有道对"思考级别不对"这类错误**不是**回 HTTP 4xx，
// 而是 HTTP 200 + 流里第一件事就是 `event:error` + `data:{...50201/4000}`。
// 只看状态码是看不见的，于是模型列表里那些"始终思考"的模型一请求就报错。
func peekChatHead(rc io.ReadCloser) (io.ReadCloser, []byte) {
	br := bufio.NewReaderSize(rc, 64*1024)
	var sb strings.Builder
	for i := 0; i < 4 && sb.Len() < 4096; i++ {
		line, err := br.ReadString('\n')
		sb.WriteString(line)
		if err != nil {
			break
		}
		if strings.TrimSpace(line) == "" { // 一个 SSE 块结束 → 后面是正文了，不再预读
			break
		}
	}
	return &multiReadCloser{r: io.MultiReader(strings.NewReader(sb.String()), br), c: rc}, []byte(sb.String())
}

// prepareChatBodyLevel forceLevel 非空时强制用这个思考级别（重试用）
func prepareChatBodyLevel(rawBody []byte, forceLevel string) []byte {
	var body map[string]any
	if err := json.Unmarshal(rawBody, &body); err != nil {
		return rawBody // 解析失败原样发送
	}
	// force stream for upstream SSE compat
	body["stream"] = true
	// 思考级别控制（2026-09-20 实测打通）：
	//   - 顶层 `thinking:{level}` 会让上游直接 500「服务器内部错误」，必须剥掉；
	//   - 正确姿势是包进 lobsterai_options，且 capability 头必须单独声明
	//     `thinking-level-control-v1`；
	//   - 实测 {"lobsterai_options":{"version":1,"thinking":{"level":"off"}}} → 无 reasoning，
	//     level=high/max → 有 reasoning。
	// 默认值取 env LB2A_THINKING_LEVEL（未设则 off，优先保证"不卡"）。
	level := forceLevel
	if level == "" {
		level = thinkingLevelFromBody(body)
	}
	delete(body, "thinking")
	delete(body, "reasoning_effort")
	body["lobsterai_options"] = map[string]any{
		"version":  1,
		"thinking": map[string]any{"level": level},
	}
	// normalize tool_choice
	if tc, ok := body["tool_choice"]; ok {
		switch v := tc.(type) {
		case string:
			if v == "" || v == "none" {
				delete(body, "tool_choice")
			}
		case map[string]any:
			// object form, keep as-is
		case nil:
			delete(body, "tool_choice")
		}
	}
	out, err := json.Marshal(body)
	if err != nil {
		return rawBody
	}
	return out
}

// ChatStream 发 chat 请求并返回原始 SSE body 流（调用方负责 Close）。
// 非 2xx 时 rc 为 nil、status 为上游状态码、err 为 nil（body 在 c.LastBody，
// 调用方用 Classify(status, body) 判定）；只有传输层失败才返回 err。
func (c *Client) ChatStream(a *auth.Auth, body []byte) (rc io.ReadCloser, status int, err error) {
	// 逐行追加记录"进入预处理前的原始请求体"，用于分辨不同来源(客户端/服务器)的请求差异
	if dp := os.Getenv("LB2A_DEBUG_RAW"); dp != "" {
		if f, err := os.OpenFile(dp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			_, _ = f.WriteString(time.Now().Format("15:04:05.000") + " raw=" + string(body) + "\r\n")
			_ = f.Close()
		}
	}
	// 请求形态阶梯：默认(thinking=off) → thinking=high → 完全不带 lobsterai_options
	//
	// 为什么第二档是 high 不是 low（2026-09-22 实测数据）：
	//   glm-5.3 先回「该模型始终思考，不支持关闭思考；请使用 low、high 或 max」，
	//   换成 low 之后又回「unsupported thinking level for model glm-5.3」→ 说明 low 它也不认，
	//   只有 high/max 才吃。
	// MiniMax-M3 / kimi-k2.7-code 更极端：整条 thinking 字段都不认，必须不发才行。
	//
	// 形态是**按模型**定的，所以成功一次就记住（shapeCache），
	// 之后同一个模型直接命中，不再白跑那 1~2 次重试（实测：平均每单省 1~2 次上游往返）。
	model := modelOf(body)
	type shapeStep struct {
		name string
		body []byte
	}
	var steps []shapeStep
	switch shapeOf(model) {
	case shapeHigh:
		steps = append(steps, shapeStep{shapeHigh, prepareChatBodyLevel(body, "high")})
	case shapeNone:
		steps = append(steps, shapeStep{shapeNone, prepareChatBodyNoOptions(body)})
	}
	steps = append(steps,
		shapeStep{shapeDefault, prepareChatBody(body)},
		shapeStep{shapeHigh, prepareChatBodyLevel(body, "high")},
		shapeStep{shapeNone, prepareChatBodyNoOptions(body)},
	)

	var lastStatus int
	var lastHead []byte
	for _, step := range steps {
		rc, status, err = c.postChat(a, step.body)
		lastStatus = status
		if err != nil {
			return rc, status, err // 传输层故障：交出去（调用方按"上游故障"处理，不罚账号）
		}
		if status >= 400 {
			lastHead = c.LastBody
			if needsThinkingRetry(c.LastBody) {
				continue // 形态问题 → 升下一档
			}
			return rc, status, nil
		}
		var head []byte
		rc, head = peekChatHead(rc)
		if needsThinkingRetry(head) {
			// 该模型的形态已经验证过是对的，还回 40301 → 只能解释成"这个号没这个模型的权限"，
			// 直接交给调用方换号，别再在形态阶梯上浪费上游调用（实测：省掉 2/3 的无效请求）
			if isAccessError(head) && shapeOf(model) != "" {
				log.Printf("chat_stream uid=%s: 模型 %s 形态已知（%s）仍被拒 → 判定该号无权限，交给调用方换号（%s）",
					a.UID, model, shapeOf(model), truncate(string(head), 120))
				_ = rc.Close()
				return nil, status, &HeadError{Status: status, Body: head}
			}
			log.Printf("chat_stream uid=%s: 模型 %s 不吃「%s」档 → 升一档重试（%s）",
				a.UID, model, step.name, truncate(string(head), 140))
			_ = rc.Close()
			lastHead = head
			continue
		}
		if headIsError(head) {
			// 不是形态问题（例如 40301 模型不可见/无权限）→ 交给调用方换号
			_ = rc.Close()
			return nil, status, &HeadError{Status: status, Body: head}
		}
		if step.name != shapeDefault {
			rememberShape(model, step.name)
			log.Printf("chat_stream uid=%s: 模型 %s 的可用形态锁定为「%s」（下次直接用，不再试）",
				a.UID, model, step.name)
		}
		return rc, status, nil
	}
	// 整条阶梯走完都没通：把最后一次的错误交出去（多半是"这号没这模型权限"→ 换号）
	if len(lastHead) > 0 {
		return nil, lastStatus, &HeadError{Status: lastStatus, Body: lastHead}
	}
	return nil, lastStatus, nil
}

// 请求形态：默认（thinking=off）/ thinking=high / 完全不带 lobsterai_options
const (
	shapeDefault = "default"
	shapeHigh    = "high"
	shapeNone    = "none"
)

// shapeCache 模型 → 上次成功的形态。进程内记忆，重启失效（重启后第一单会再试一次，无妨）。
var shapeCache sync.Map

func shapeOf(model string) string {
	if model == "" {
		return ""
	}
	if v, ok := shapeCache.Load(model); ok {
		s, _ := v.(string)
		return s
	}
	return ""
}

func rememberShape(model, shape string) {
	if model == "" || shape == "" {
		return
	}
	shapeCache.Store(model, shape)
}

// modelOf 从请求体里取 model 名（取不到返回空，形态记忆就退化成"每次从默认档试"）
func modelOf(rawBody []byte) string {
	var m struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(rawBody, &m) != nil {
		return ""
	}
	return strings.TrimSpace(m.Model)
}

// postChat 发一次 chat 请求（ChatStream 的干活的半截，抽出来是为了能重试）
func (c *Client) postChat(a *auth.Auth, prepared []byte) (io.ReadCloser, int, error) {
	url := ServerBase() + "/api/proxy/v1/chat/completions"
	if dp := os.Getenv("LB2A_DEBUG_BODY"); dp != "" {
		_ = os.WriteFile(dp, prepared, 0o600)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(prepared))
	if err != nil {
		return nil, 0, err
	}
	chatHeaders(req, a)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		log.Printf("chat_stream uid=%s: transport error: %v", a.UID, err)
		return nil, 0, err
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		tr := resp.Header.Get("Trace_id")
		resp.Body.Close()
		c.LastBody = raw
		kind := Classify(resp.StatusCode, string(raw))
		log.Printf("chat_stream uid=%s: upstream %d %s trace=%s body=%s",
			a.UID, resp.StatusCode, kind, tr, truncate(string(raw), 200))
		return nil, resp.StatusCode, nil
	}
	log.Printf("chat_stream uid=%s: upstream %d trace=%s", a.UID, resp.StatusCode, resp.Header.Get("Trace_id"))
	return resp.Body, resp.StatusCode, nil
}

// FetchModels 调上游动态模型接口。
// GET {server}/api/models/available，Bearer accessToken。
// 返回模型 ID 列表；失败返回错误（调用方回退静态表）。
func (c *Client) FetchModels(a *auth.Auth) ([]string, error) {
	url := ServerBase() + "/api/models/available"
	body := a.KeyfromBody()
	// build query string from keyfrom
	parts := make([]string, 0)
	for k, v := range body {
		parts = append(parts, fmt.Sprintf("%s=%s", k, fmt.Sprintf("%v", v)))
	}
	if len(parts) > 0 {
		url += "?" + strings.Join(parts, "&")
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("models api status %d: %s", resp.StatusCode, truncate(string(raw), 120))
	}
	var env struct {
		Code int `json:"code"`
		Data []struct {
			ModelID   string `json:"modelId"`
			ModelName string `json:"modelName"`
			Provider  string `json:"provider"`
			ApiFormat string `json:"apiFormat"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("models parse: %w", err)
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("models api code=%d", env.Code)
	}
	ids := make([]string, 0, len(env.Data))
	for _, m := range env.Data {
		if m.ModelID != "" {
			ids = append(ids, m.ModelID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("models api returned empty list")
	}
	return ids, nil
}

// ErrNoCredits 上游**权威地回答**"这个号没有可用积分了"
// （profile-summary 里 `totalCreditsRemaining` 缺失或 ≤ 0 时走这个分支）。
//
// 它不是"查询失败"，是"余额 = 0"。这个区分很关键：
//
//	2026-09-22 实测 —— 98164 / 98790 / 98806 三个号早就没积分了，但这个分支被当成普通 error
//	只打一行日志就 continue，账号的 credits 永远停在最后一次读到的有效值（11 / 5 / 55），
//	选号照样选它们 → 上游在**流中途**回"免费额度已用完" → 客户端只看到"正在重新连接 2/5"。
//	判据就是这行一直在刷：quota-refresh 98790: profile-summary: no credits
var ErrNoCredits = errors.New("profile-summary: no credits")

// QuotaUsage 查询账号当前积分。
// GET {server}/api/user/profile-summary 的 totalCreditsRemaining（含 free + campaign 活动积分）。
// 注意: /api/user/quota 只显示 freeCreditsTotal=300, 不含 5000 活动积分。
func (c *Client) QuotaUsage(a *auth.Auth) (remain int64, total int64, err error) {
	url := ServerBase() + "/api/user/profile-summary"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	data, err := c.doJSON(req)
	if err != nil {
		return 0, 0, err
	}
	var ps struct {
		TotalCreditsRemaining float64 `json:"totalCreditsRemaining"`
	}
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, 0, fmt.Errorf("profile-summary parse: %w", err)
	}
	clamp := func(v float64) int64 {
		if v < 0 {
			return 0
		}
		return int64(v)
	}
	if ps.TotalCreditsRemaining > 0 {
		return clamp(ps.TotalCreditsRemaining), 0, nil
	}
	return 0, 0, ErrNoCredits
}

// InviteProgress 一个账号的「邀请好友」进度。
//
// 接口是从 LobsterAI 客户端 resources/app.asar 里逆出来的（portal 前端 chunk
// 也调同一个）：
//
//	GET {base}/api/invitation/progress   （Bearer 账号 token）
//	data: invitationCode / invitedCount / stage1{Threshold,Completed} /
//	      stage2{Threshold,Completed} / perInviteRewardCredits / totalRewardCredits
//
// 面板「账号池」用它显示每个号已邀请了几人（2026-09-26 用户要求）。
type InviteProgress struct {
	InvitationCode         string
	InvitedCount           int
	Stage1Threshold        int
	Stage1Completed        bool
	Stage2Threshold        int
	Stage2Completed        bool
	PerInviteRewardCredits int
	TotalRewardCredits     float64
}

func (c *Client) InviteProgress(a *auth.Auth) (*InviteProgress, error) {
	req, err := http.NewRequest(http.MethodGet, ServerBase()+"/api/invitation/progress", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	data, err := c.doJSON(req)
	if err != nil {
		return nil, err
	}
	var d struct {
		InvitationCode         string  `json:"invitationCode"`
		InvitedCount           int     `json:"invitedCount"`
		Stage1Threshold        int     `json:"stage1Threshold"`
		Stage1Completed        bool    `json:"stage1Completed"`
		Stage2Threshold        int     `json:"stage2Threshold"`
		Stage2Completed        bool    `json:"stage2Completed"`
		PerInviteRewardCredits int     `json:"perInviteRewardCredits"`
		TotalRewardCredits     float64 `json:"totalRewardCredits"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("invitation/progress parse: %w", err)
	}
	return &InviteProgress{
		InvitationCode:         d.InvitationCode,
		InvitedCount:           d.InvitedCount,
		Stage1Threshold:        d.Stage1Threshold,
		Stage1Completed:        d.Stage1Completed,
		Stage2Threshold:        d.Stage2Threshold,
		Stage2Completed:        d.Stage2Completed,
		PerInviteRewardCredits: d.PerInviteRewardCredits,
		TotalRewardCredits:     d.TotalRewardCredits,
	}, nil
}

// ---------------------------------------------------------------------------
// 每日积分礼（client-activities）—— 2026-09-21 从 LobsterAI 桌面客户端
// resources/app.asar 里逆出来的接口（原来是 no-op 桩，白白浪费了每天 100 分）：
//
//	GET  {base}/api/client-activities/slot?placement=desktop_sidebar&clientVersion=..&containerApiVersion=2&platform=win32
//	GET  {base}/api/client-activities/{code}/context?configRevision=N
//	POST {base}/api/client-activities/{code}/actions/check_in
//	     body {"configRevision":N,"idempotencyKey":"daily-check-in-<uuid>","payload":{}}
//
// 实测（uid 98164，2026-09-21）：context 报 rewardCredits=100、claimedToday=false →
// 领取返回 creditsGranted=100、periodKey=当天、expiresAt=+30 天，账号积分 110.1 → 210.1，
// creditItems 多出一条 campaign:每日登录奖励。活动是常驻的
// （daily-check-in-evergreen-prod-20260814），slot/context 可匿名访问，领取必须带 token。
// 同一天重复领会返回 claimedToday / replayed，不会重复加分。
// ---------------------------------------------------------------------------

const (
	activityPlacement  = "desktop_sidebar" // ActivityPlacement.DesktopSidebar
	activityAPIVersion = 2                 // ActivityContainerApiVersion.NativeDailyCheckInV1
	activityClientVer  = "2026.9.4"        // 客户端版本号，上游只做兼容判断
	activityActionID   = "check_in"        // DailyCheckInAction.CheckIn
)

// CheckinResult 一次「每日积分礼」的结果。
type CheckinResult struct {
	ActivityCode string
	Credits      int64 // 本次实际到账积分
	AlreadyToday bool  // 今天已经领过
	NoActivity   bool  // 上游当前没有可领的活动
}

// DailyCheckin 领今天的「每日积分礼」。可重复调用：今天领过就返回 AlreadyToday。
func (c *Client) DailyCheckin(a *auth.Auth) (CheckinResult, error) {
	var res CheckinResult
	base := ServerBase()
	if base == "" {
		return res, fmt.Errorf("上游地址为空")
	}
	// ① 活动位
	q := url.Values{}
	q.Set("placement", activityPlacement)
	q.Set("clientVersion", activityClientVer)
	q.Set("containerApiVersion", strconv.Itoa(activityAPIVersion))
	q.Set("platform", "win32")
	slotRaw, err := c.activityReq(a, http.MethodGet, base+"/api/client-activities/slot?"+q.Encode(), nil)
	if err != nil {
		return res, fmt.Errorf("取活动位失败: %w", err)
	}
	var slot struct {
		SlotState string `json:"slotState"`
		Activity  *struct {
			ActivityCode   string `json:"activityCode"`
			ConfigRevision int    `json:"configRevision"`
		} `json:"activity"`
	}
	if err := json.Unmarshal(slotRaw, &slot); err != nil {
		return res, fmt.Errorf("活动位解析失败: %w", err)
	}
	if slot.SlotState != "available" || slot.Activity == nil || slot.Activity.ActivityCode == "" {
		res.NoActivity = true
		return res, nil
	}
	res.ActivityCode = slot.Activity.ActivityCode
	rev := slot.Activity.ConfigRevision

	// ② 上下文：今天领没领
	ctxRaw, err := c.activityReq(a, http.MethodGet, fmt.Sprintf(
		"%s/api/client-activities/%s/context?configRevision=%d",
		base, url.PathEscape(res.ActivityCode), rev), nil)
	if err != nil {
		return res, fmt.Errorf("取活动上下文失败: %w", err)
	}
	var ctx struct {
		State struct {
			ClaimedToday bool `json:"claimedToday"`
		} `json:"state"`
	}
	_ = json.Unmarshal(ctxRaw, &ctx)
	if ctx.State.ClaimedToday {
		res.AlreadyToday = true
		return res, nil
	}

	// ③ 领取
	body, _ := json.Marshal(map[string]any{
		"configRevision": rev,
		"idempotencyKey": "daily-check-in-" + newUUID(),
		"payload":        map[string]any{},
	})
	actRaw, err := c.activityReq(a, http.MethodPost, fmt.Sprintf(
		"%s/api/client-activities/%s/actions/%s",
		base, url.PathEscape(res.ActivityCode), activityActionID), body)
	if err != nil {
		return res, fmt.Errorf("领取失败: %w", err)
	}
	var act struct {
		Replayed bool `json:"replayed"`
		Result   struct {
			CreditsGranted int64 `json:"creditsGranted"`
		} `json:"result"`
	}
	if err := json.Unmarshal(actRaw, &act); err != nil {
		return res, fmt.Errorf("领取响应解析失败: %w", err)
	}
	res.Credits = act.Result.CreditsGranted
	if act.Replayed || res.Credits == 0 {
		res.AlreadyToday = true
	}
	return res, nil
}

// activityReq 发一个 activity 请求（带账号 token；body 为 nil 表示 GET）。
func (c *Client) activityReq(a *auth.Auth, method, fullURL string, body []byte) (json.RawMessage, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, fullURL, rdr)
	if err != nil {
		return nil, err
	}
	if a != nil && a.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.doJSON(req)
}

// newUUID 给 idempotencyKey 用的随机串（v4 形状，只为唯一性）。
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
