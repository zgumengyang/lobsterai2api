// Package server 暴露 OpenAI 兼容 HTTP 接口，内部驱动 pool 挑号 + upstream 转发。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"lobsterai2api/internal/pool"
	"lobsterai2api/internal/relay"
	"lobsterai2api/internal/upstream"
)

// Config handler 依赖。
type Config struct {
	Pool         *pool.Pool
	Upstream     *upstream.Client
	APIKey       string        // 空 = 不鉴权
	APIKeys      []string      // 多 key 列表（与 APIKey 合并生效，任一匹配即放行）
	CallLogFile  string        // 调用流水落盘路径（data/keycalls.jsonl；空 = 不记）
	MaxRotate    int           // 单请求最多换号次数，默认 3
	HardCooldown time.Duration // 余额不足冷却，默认 12h
	SoftCooldown time.Duration // 429 冷却，默认 60s
	ErrThreshold int           // 连续其他错误冷却阈值，默认 3
	ErrCooldown  time.Duration // 错误冷却时长，默认 10m
	RefreshSkew  time.Duration // token 提前刷新窗口，默认 10m
	UsageFile    string        // token 统计持久化文件（空 = 不持久化）
	RouteFile    string        // 上游路由统计持久化文件（空 = 不持久化）
	NotesFile    string        // 账号备注持久化文件（空 = 不持久化）
	PriceFile    string        // 单价设置持久化文件（空 = 不持久化）
	LimitsFile   string        // Key 限速设置持久化文件（空 = 不持久化）
	KeyFile      string        // Key 使用统计持久化文件（空 = 不持久化）
	Relay        *relay.Store  // 外部反代渠道（空 = 不启用多渠道）
}

// Handler 主路由。
type Handler struct {
	cfg         Config
	mux         *http.ServeMux
	keyMu       sync.Mutex
	keyStats    map[string]*KeyUsage
	keyFile     string
	keyDirty    bool
	keyLastSave time.Time
	calls       *callLog // 调用流水（JSONL，面板「记录」那张表）
	keysMu      sync.RWMutex
	keysHot     map[string]bool                         // 热重载后的生效 key 集合（nil = 用启动时的 cfg）
	reloadFn    func() (keys int, accts int, err error) // 由 main 注入：重读 config + 重扫 auths
	keepaliveFn func() (ok int, failed int, banned int) // 由 main 注入：立刻刷新所有账号 token
	checkinFn   func() map[string]any                   // 由 main 注入：立刻领一轮「每日积分礼」
	usageMu     sync.Mutex
	usage       UsageStats
	usageDaily  map[string]*UsageStats
	// 按上游来源记账（"lobster" = 账号池，其余 = 渠道 ID）。
	// 用途：面板要能看出"反代的消耗也算进今日/累计总数了"。
	// 不变量：usageSrc 各项相加 == usage；usageSrcDaily[day] 各项相加 == usageDaily[day]。
	usageSrc      map[string]*UsageStats
	usageSrcDaily map[string]map[string]*UsageStats
	usageFile     string
	routeMu       sync.Mutex
	route         RouteStats
	fallbacks     []FallbackEvent
	routeFile     string
	routeSince    time.Time
	notesMu       sync.Mutex
	notes         map[string]string
	notesFile     string
	priceMu       sync.Mutex
	priceIn       float64 // 每百万输入 token 单价
	priceCache    float64 // 每百万缓存命中输入 token 单价
	priceOut      float64 // 每百万输出 token 单价
	cnyRate       float64 // 汇率（同时显示人民币）
	priceFile     string
	limitMu       sync.Mutex
	limits        map[string]int         // key -> 每分钟请求上限（0 = 不限速）
	limitHits     map[string][]time.Time // key -> 最近一分钟的调用时间
	limitsFile    string
	limitWait     map[string]int // key -> 正在排队等待的请求数
	// 每 Key 的「每日额度」（按请求单数）：超了直接 429，不排队 —— 这才是套餐该用的闸门
	limitDaily    map[string]int64 // key -> 每日上限（0 = 不限）
	limitStrict   map[string]bool  // key -> true = 每分钟限速超了直接 429（默认排队）
	limitUsed     map[string]int64 // key -> 今日已用单数
	limitUsedDay  string           // limitUsed 对应的日期（跨天清零）
	faultMu       sync.Mutex
	upstreamFault []time.Time // 上游自身故障（5xx / 超时 / 连接失败）时间戳
	upstreamLast  string
	requestFault  []time.Time // 请求形态错误（4xx 非账号级）时间戳
	requestLast   string
}

// ---------------------------------------------------------------------------
// 错误归因：上游故障 / 请求形态错误 一律不惩罚账号。
// 这是 2026-09-20 治「频繁冷却」的核心改动：此前任何 5xx、超时、4xx 都会
// 记到被选中账号的 errCount 上，3 次就冷却 10 分钟，上游一抖全池阵亡。
// ---------------------------------------------------------------------------

// noteUpstreamFault 记录一次「上游自身故障」，只进全局滑动窗口，不惩罚账号。
func (h *Handler) noteUpstreamFault(kind, why string) {
	h.faultMu.Lock()
	defer h.faultMu.Unlock()
	h.upstreamFault = append(h.upstreamFault, time.Now())
	if len(h.upstreamFault) > 1024 {
		h.upstreamFault = h.upstreamFault[len(h.upstreamFault)-1024:]
	}
	h.upstreamLast = kind + ": " + brief(why)
}

// noteRequestFault 记录一次「请求形态错误」（同一 body 打哪个账号都会被拒）。
func (h *Handler) noteRequestFault(kind, why string) {
	h.faultMu.Lock()
	defer h.faultMu.Unlock()
	h.requestFault = append(h.requestFault, time.Now())
	if len(h.requestFault) > 1024 {
		h.requestFault = h.requestFault[len(h.requestFault)-1024:]
	}
	h.requestLast = kind + ": " + brief(why)
}

// faultsSnapshot 返回最近 60 秒的上游/请求故障计数与最近一条说明。
func (h *Handler) faultsSnapshot() (int, string, int, string) {
	h.faultMu.Lock()
	defer h.faultMu.Unlock()
	cut := time.Now().Add(-60 * time.Second)
	count := func(ts []time.Time) int {
		n := 0
		for i := len(ts) - 1; i >= 0; i-- {
			if ts[i].After(cut) {
				n++
			} else {
				break
			}
		}
		return n
	}
	return count(h.upstreamFault), h.upstreamLast, count(h.requestFault), h.requestLast
}

// loadLimits 启动时恢复每个 Key 的限速设置。
func (h *Handler) loadLimits() {
	h.limits = map[string]int{}
	h.limitHits = map[string][]time.Time{}
	h.limitWait = map[string]int{}
	h.limitDaily = map[string]int64{}
	h.limitStrict = map[string]bool{}
	h.limitUsed = map[string]int64{}
	h.limitUsedDay = limitDayKey()
	if h.limitsFile == "" {
		return
	}
	raw, err := os.ReadFile(h.limitsFile)
	if err != nil {
		return
	}
	var d struct {
		Limits  map[string]int   `json:"limits"`
		Daily   map[string]int64 `json:"daily"`
		Strict  map[string]bool  `json:"strict"`
		Used    map[string]int64 `json:"used"`
		UsedDay string           `json:"used_day"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	if d.Limits != nil {
		h.limits = d.Limits
	}
	if d.Daily != nil {
		h.limitDaily = d.Daily
	}
	if d.Strict != nil {
		h.limitStrict = d.Strict
	}
	if d.Used != nil && d.UsedDay == h.limitUsedDay {
		h.limitUsed = d.Used
	}
}

func (h *Handler) saveLimitsLocked() {
	if h.limitsFile == "" {
		return
	}
	b, err := json.MarshalIndent(map[string]any{
		"limits":   h.limits,
		"daily":    h.limitDaily,
		"strict":   h.limitStrict,
		"used":     h.limitUsed,
		"used_day": h.limitUsedDay,
		"saved_at": time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.limitsFile, b, 0644)
}

// 限速处理方式：超限时排队等待（客户端变慢但不出错），等太久才 429。
const (
	limitMaxWait    = 90 * time.Second // 单个请求最多排队多久
	limitMaxWaiters = 50               // 同一 Key 同时排队的请求上限（超出直接 429）
)

// allowWindow 滑动窗口限速。超限时阻塞等待空位（最多 limitMaxWait），返回 false 表示放弃；
// 该 Key 开了 strict 就改成直接拒（不排队）。
func (h *Handler) allowWindow(key string) (bool, int) {
	deadline := time.Now().Add(limitMaxWait)
	queued := false
	for {
		h.limitMu.Lock()
		rpm := h.limits[key]
		if rpm <= 0 {
			if queued {
				h.limitWait[key]--
			}
			h.limitMu.Unlock()
			return true, 0
		}
		now := time.Now()
		cut := now.Add(-time.Minute)
		hits := h.limitHits[key][:0]
		for _, t := range h.limitHits[key] {
			if t.After(cut) {
				hits = append(hits, t)
			}
		}
		if len(hits) < rpm {
			hits = append(hits, now)
			h.limitHits[key] = hits
			if queued {
				h.limitWait[key]--
			}
			h.limitMu.Unlock()
			return true, rpm
		}
		if !queued {
			// strict：这个 Key 不许排队，超了立刻 429
			if h.limitStrict[key] {
				h.limitMu.Unlock()
				return false, rpm
			}
			if h.limitWait[key] >= limitMaxWaiters {
				h.limitMu.Unlock()
				return false, rpm
			}
			h.limitWait[key]++
			queued = true
		}
		wait := time.Until(hits[0].Add(time.Minute + 50*time.Millisecond))
		h.limitHits[key] = hits
		h.limitMu.Unlock()
		if wait < 0 {
			wait = 50 * time.Millisecond
		}
		if time.Now().Add(wait).After(deadline) {
			h.limitMu.Lock()
			if queued {
				h.limitWait[key]--
			}
			h.limitMu.Unlock()
			return false, rpm
		}
		time.Sleep(wait)
	}
}

func limitDayKey() string { return time.Now().Format("2006-01-02") }

// allowKey 两道闸门：
//
//	① 每 Key 每日额度（按请求单数）—— **硬闸门**，超了直接 429，不排队。卖套餐该用这个。
//	② 每分钟滑动窗口 —— 默认排队等空位（客户端变慢但不出错），开了 strict 就直接 429。
//
// 返回 (是否放行, 拒绝类型, 触发值)：类型 ""=放行 / "daily_quota" / "rate_limited"。
func (h *Handler) allowKey(key string) (bool, string, int) {
	// ① 每日额度：跨天自动清零
	h.limitMu.Lock()
	if h.limitUsedDay != limitDayKey() {
		h.limitUsed = map[string]int64{}
		h.limitUsedDay = limitDayKey()
	}
	daily := h.limitDaily[key]
	if daily > 0 && h.limitUsed[key] >= daily {
		h.limitMu.Unlock()
		return false, "daily_quota", int(daily)
	}
	h.limitMu.Unlock()

	// ② 每分钟窗口
	ok, rpm := h.allowWindow(key)
	if !ok {
		return false, "rate_limited", rpm
	}

	// 两道都过了 → 记一单（只有配了额度的 Key 需要记）
	if daily > 0 {
		h.limitMu.Lock()
		h.limitUsed[key]++
		h.saveLimitsLocked()
		h.limitMu.Unlock()
	}
	return true, "", rpm
}

func (h *Handler) limitsSnapshot() map[string]int {
	h.limitMu.Lock()
	defer h.limitMu.Unlock()
	out := make(map[string]int, len(h.limits))
	for k, v := range h.limits {
		out[k] = v
	}
	return out
}

// keyLimitsHandler Key 限速管理：GET 查、POST 改（{key, rpm}）。
func (h *Handler) keyLimitsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		var req struct {
			Key    string `json:"key"`
			RPM    *int   `json:"rpm"`
			Daily  *int64 `json:"daily"`  // 每日单数上限（0 = 不限）
			Strict *bool  `json:"strict"` // true = 每分钟限速超了直接 429，不排队
		}
		if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.Key) == "" {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "key required")
			return
		}
		h.limitMu.Lock()
		if h.limits == nil {
			h.limits = map[string]int{}
		}
		if h.limitDaily == nil {
			h.limitDaily = map[string]int64{}
		}
		if h.limitStrict == nil {
			h.limitStrict = map[string]bool{}
		}
		out := map[string]any{"ok": true, "key": req.Key}
		if req.RPM != nil {
			rpm := *req.RPM
			if rpm < 0 {
				rpm = 0
			}
			if rpm == 0 {
				delete(h.limits, req.Key)
			} else {
				h.limits[req.Key] = rpm
			}
			out["rpm"] = rpm
		}
		if req.Daily != nil {
			d := *req.Daily
			if d < 0 {
				d = 0
			}
			if d == 0 {
				delete(h.limitDaily, req.Key)
				delete(h.limitUsed, req.Key)
			} else {
				h.limitDaily[req.Key] = d
			}
			out["daily"] = d
		}
		if req.Strict != nil {
			if *req.Strict {
				h.limitStrict[req.Key] = true
			} else {
				delete(h.limitStrict, req.Key)
			}
			out["strict"] = *req.Strict
		}
		h.saveLimitsLocked()
		h.limitMu.Unlock()
		writeJSON(w, http.StatusOK, out)
		return
	}
	h.limitMu.Lock()
	used := map[string]int64{}
	for k, v := range h.limitUsed {
		used[k] = v
	}
	daily := map[string]int64{}
	for k, v := range h.limitDaily {
		daily[k] = v
	}
	strict := map[string]bool{}
	for k, v := range h.limitStrict {
		strict[k] = v
	}
	h.limitMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"limits": h.limitsSnapshot(), "daily": daily, "strict": strict,
		"used": used, "used_day": limitDayKey(),
	})
}

// poolReset 一键解除账号冷却/禁用（body 带 uid = 单解；不带 = 全解）。
func (h *Handler) poolReset(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		UID string `json:"uid"`
	}
	_ = json.Unmarshal(body, &req)
	if strings.TrimSpace(req.UID) != "" {
		ok := h.cfg.Pool.ClearOne(strings.TrimSpace(req.UID))
		writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "uid": req.UID})
		return
	}
	n := h.cfg.Pool.ClearCooldown()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "cleared": n})
}

// poolFreeze 手工冻结 / 解冻一个池子账号（用户要求 2026-09-22：龙虾池每个号都要有这按钮）。
//
//	{"uid":"94981","on":true}   冻结 —— 从选号里排除；账号配置、登录态全留着，随时解冻
//	{"uid":"94981","on":false}  解冻 —— 清冷却 + 解冻 + 清错误计数
//
// 冻结走的是既有的 Disable（跟"被封禁"同一套状态机），所以选号那边天然不会再碰它。
func (h *Handler) poolFreeze(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		UID string `json:"uid"`
		On  *bool  `json:"on"`
	}
	_ = json.Unmarshal(body, &req)
	uid := strings.TrimSpace(req.UID)
	if uid == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "uid required")
		return
	}
	if req.On == nil || *req.On {
		if h.cfg.Pool.AuthByUID(uid) == nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "这个 uid 已经不在池子里了（刷新页面再试）"})
			return
		}
		h.cfg.Pool.Disable(uid, "手动冻结")
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "uid": uid, "frozen": true,
			"detail": "已冻结：不再参与选号（账号和登录态都留着，随时能解冻）",
		})
		return
	}
	ok := h.cfg.Pool.ClearOne(uid)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": ok, "uid": uid, "frozen": false,
		"detail": "已解冻：重新参与选号",
	})
}

// loadPrice 启动时恢复单价设置（默认按 OpenAI GPT-4o 官方价：$2.5 输入 / $1.25 缓存输入 / $10 输出 每百万 token）。
func (h *Handler) loadPrice() {
	h.priceIn, h.priceCache, h.priceOut, h.cnyRate = 2.5, 1.25, 10, 7.2
	if h.priceFile == "" {
		return
	}
	raw, err := os.ReadFile(h.priceFile)
	if err != nil {
		return
	}
	var d struct {
		Input  *float64 `json:"input_per_m"`
		Cached *float64 `json:"cached_per_m"`
		Output *float64 `json:"output_per_m"`
		CNY    *float64 `json:"cny_rate"`
	}
	if json.Unmarshal(raw, &d) == nil {
		if d.Input != nil {
			h.priceIn = *d.Input
		}
		if d.Cached != nil {
			h.priceCache = *d.Cached
		}
		if d.Output != nil {
			h.priceOut = *d.Output
		}
		if d.CNY != nil {
			h.cnyRate = *d.CNY
		}
	}
}

func (h *Handler) savePriceLocked() {
	if h.priceFile == "" {
		return
	}
	b, err := json.MarshalIndent(map[string]any{
		"input_per_m":  h.priceIn,
		"cached_per_m": h.priceCache,
		"output_per_m": h.priceOut,
		"cny_rate":     h.cnyRate,
		"saved_at":     time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.priceFile, b, 0644)
}

// priceHandler 单价设置：GET 查、POST 改。
func (h *Handler) priceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		var req struct {
			Input  *float64 `json:"input_per_m"`
			Cached *float64 `json:"cached_per_m"`
			Output *float64 `json:"output_per_m"`
			CNY    *float64 `json:"cny_rate"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse body: "+err.Error())
			return
		}
		h.priceMu.Lock()
		if req.Input != nil && *req.Input >= 0 {
			h.priceIn = *req.Input
		}
		if req.Cached != nil && *req.Cached >= 0 {
			h.priceCache = *req.Cached
		}
		if req.Output != nil && *req.Output >= 0 {
			h.priceOut = *req.Output
		}
		if req.CNY != nil && *req.CNY >= 0 {
			h.cnyRate = *req.CNY
		}
		h.savePriceLocked()
		in, cac, out, cny := h.priceIn, h.priceCache, h.priceOut, h.cnyRate
		h.priceMu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "input_per_m": in, "cached_per_m": cac, "output_per_m": out, "cny_rate": cny})
		return
	}
	h.priceMu.Lock()
	in, cac, out, cny := h.priceIn, h.priceCache, h.priceOut, h.cnyRate
	h.priceMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"input_per_m": in, "cached_per_m": cac, "output_per_m": out, "cny_rate": cny})
}

func (h *Handler) priceSnapshot() (float64, float64, float64, float64) {
	h.priceMu.Lock()
	defer h.priceMu.Unlock()
	return h.priceIn, h.priceCache, h.priceOut, h.cnyRate
}

// loadNotes 启动时恢复账号备注。
func (h *Handler) loadNotes() {
	h.notes = map[string]string{}
	if h.notesFile == "" {
		return
	}
	raw, err := os.ReadFile(h.notesFile)
	if err != nil {
		return
	}
	var d struct {
		Notes map[string]string `json:"notes"`
	}
	if json.Unmarshal(raw, &d) == nil && d.Notes != nil {
		h.notes = d.Notes
	}
}

// saveNotesLocked 落盘（调用方需持有 notesMu）。
func (h *Handler) saveNotesLocked() {
	if h.notesFile == "" {
		return
	}
	b, err := json.MarshalIndent(map[string]any{"notes": h.notes, "saved_at": time.Now()}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.notesFile, b, 0644)
}

func (h *Handler) notesSnapshot() map[string]string {
	h.notesMu.Lock()
	defer h.notesMu.Unlock()
	out := make(map[string]string, len(h.notes))
	for k, v := range h.notes {
		out[k] = v
	}
	return out
}

// RouteStats 上游路由统计（内存态，服务重启清零）。
type RouteStats struct {
	RelayOK      int64            `json:"relay_ok"`      // 成功走反代的次数
	RelayFail    int64            `json:"relay_fail"`    // 反代失败的次数
	PoolFallback int64            `json:"pool_fallback"` // 反代失败后落到龙虾池的次数（会消耗积分）
	PoolDirect   int64            `json:"pool_direct"`   // 正常走龙虾池的次数
	FallbackDest map[string]int64 `json:"fallback_dest"` // 回退去向计数：渠道 ID 或 "lobster"
	LastErr      string           `json:"last_err"`
	LastErrAt    time.Time        `json:"last_err_at"`
}

// loadRoute 启动时恢复路由统计（重启不丢）。
func (h *Handler) loadRoute() {
	if h.routeFile == "" {
		return
	}
	raw, err := os.ReadFile(h.routeFile)
	if err != nil {
		return
	}
	var d struct {
		Route     RouteStats      `json:"route"`
		Fallbacks []FallbackEvent `json:"fallbacks"`
		Since     time.Time       `json:"since"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	h.route = d.Route
	if h.route.FallbackDest == nil {
		h.route.FallbackDest = map[string]int64{}
	}
	// 老数据迁移：以前只统计了「回退龙虾池」，补进去向表
	if len(h.route.FallbackDest) == 0 && h.route.PoolFallback > 0 {
		h.route.FallbackDest["lobster"] = h.route.PoolFallback
	}
	h.fallbacks = d.Fallbacks
	h.routeSince = d.Since
}

// saveRouteLocked 落盘（调用方需持有 routeMu）。
func (h *Handler) saveRouteLocked() {
	if h.routeFile == "" {
		return
	}
	if h.routeSince.IsZero() {
		h.routeSince = time.Now()
	}
	b, err := json.MarshalIndent(map[string]any{
		"route":     h.route,
		"fallbacks": h.fallbacks,
		"since":     h.routeSince,
		"saved_at":  time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.routeFile, b, 0644)
}

// FallbackEvent 一次「反代失败 → 回退龙虾池」的记录。
type FallbackEvent struct {
	At    time.Time `json:"at"`
	Model string    `json:"model"`
	Cause string    `json:"cause"`
}

func (h *Handler) noteRelayOK() {
	h.routeMu.Lock()
	h.route.RelayOK++
	h.saveRouteLocked()
	h.routeMu.Unlock()
}

func (h *Handler) noteRelayFail(model string, err error) {
	h.routeMu.Lock()
	defer h.routeMu.Unlock()
	h.route.RelayFail++
	h.route.LastErr = err.Error()
	h.route.LastErrAt = time.Now()
	h.fallbacks = append(h.fallbacks, FallbackEvent{At: time.Now(), Model: model, Cause: err.Error()})
	if len(h.fallbacks) > 20 {
		h.fallbacks = h.fallbacks[len(h.fallbacks)-20:]
	}
	h.saveRouteLocked()
}

func (h *Handler) notePool(fallback bool) {
	h.routeMu.Lock()
	if fallback {
		h.route.PoolFallback++
		h.bumpDestLocked("lobster")
	} else {
		h.route.PoolDirect++
	}
	h.saveRouteLocked()
	h.routeMu.Unlock()
}

// bumpDestLocked 回退去向计数（调用方需持有 routeMu）。
func (h *Handler) bumpDestLocked(dest string) {
	if h.route.FallbackDest == nil {
		h.route.FallbackDest = map[string]int64{}
	}
	h.route.FallbackDest[dest]++
}

// noteFallbackTo 记录一次「回退到某个渠道」。
func (h *Handler) noteFallbackTo(channelID string) {
	h.routeMu.Lock()
	h.bumpDestLocked(channelID)
	h.saveRouteLocked()
	h.routeMu.Unlock()
}

func (h *Handler) routeSnapshot() (RouteStats, []FallbackEvent) {
	h.routeMu.Lock()
	defer h.routeMu.Unlock()
	ev := append([]FallbackEvent(nil), h.fallbacks...)
	return h.route, ev
}

func (h *Handler) routeLastErr() string {
	h.routeMu.Lock()
	defer h.routeMu.Unlock()
	return h.route.LastErr
}

// UsageStats 累计 token 消耗统计。
type UsageStats struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	CacheHitTokens   int64 `json:"cache_hit_tokens"`
}

// noteAccountUsage 把一次成功请求记到"服务它的那个账号"头上。
// 面板账号卡片上的「今日 N 单 · X token」就是这儿攒的（积分消耗在 pool 里按余额差累加）。
func (h *Handler) noteAccountUsage(uid string, usage map[string]any) {
	if h.cfg.Pool == nil || uid == "" || usage == nil {
		return
	}
	var tokens int64
	if v, ok := usage["total_tokens"].(float64); ok {
		tokens = int64(v)
	}
	h.cfg.Pool.NoteRequest(uid, tokens)
}

// noteUsageFrom 累计一次调用的 usage。
//
// src = 这次请求走的是哪个上游："lobster" = 龙虾账号池，其余 = 渠道 ID。
// 除了总量/按天，还额外按来源记一份（usageSrc / usageSrcDaily）——
// 这样面板能直接看出"反代的消耗也计入了总数"，而不是只能猜。
// **不变量：所有来源相加 == 总量**（两边用同一个 apply 累加，不会漏也不会重复）。
func (h *Handler) noteUsageFrom(src string, u map[string]any) {
	if u == nil {
		return
	}
	if strings.TrimSpace(src) == "" {
		src = "lobster"
	}
	num := func(m map[string]any, k string) int64 {
		if v, ok := m[k].(float64); ok {
			return int64(v)
		}
		return 0
	}
	// 缓存命中：龙虾用 prompt_cache_hit_tokens，标准 OpenAI 用 prompt_tokens_details.cached_tokens
	hitTokens := num(u, "prompt_cache_hit_tokens")
	if hitTokens == 0 {
		if d, ok := u["prompt_tokens_details"].(map[string]any); ok {
			hitTokens = num(d, "cached_tokens")
		}
	}
	var reasoning int64
	if d, ok := u["completion_tokens_details"].(map[string]any); ok {
		reasoning = num(d, "reasoning_tokens")
	}
	apply := func(s *UsageStats) {
		s.Requests++
		s.PromptTokens += num(u, "prompt_tokens")
		s.CompletionTokens += num(u, "completion_tokens")
		s.TotalTokens += num(u, "total_tokens")
		s.CacheHitTokens += hitTokens
		s.ReasoningTokens += reasoning
	}
	h.usageMu.Lock()
	defer h.usageMu.Unlock()
	apply(&h.usage)
	day := time.Now().Format("2006-01-02")
	if h.usageDaily == nil {
		h.usageDaily = map[string]*UsageStats{}
	}
	d := h.usageDaily[day]
	if d == nil {
		d = &UsageStats{}
		h.usageDaily[day] = d
	}
	apply(d)
	// 按上游来源
	if h.usageSrc == nil {
		h.usageSrc = map[string]*UsageStats{}
	}
	s := h.usageSrc[src]
	if s == nil {
		s = &UsageStats{}
		h.usageSrc[src] = s
	}
	apply(s)
	if h.usageSrcDaily == nil {
		h.usageSrcDaily = map[string]map[string]*UsageStats{}
	}
	if h.usageSrcDaily[day] == nil {
		h.usageSrcDaily[day] = map[string]*UsageStats{}
	}
	sd := h.usageSrcDaily[day][src]
	if sd == nil {
		sd = &UsageStats{}
		h.usageSrcDaily[day][src] = sd
	}
	apply(sd)
	h.saveUsageLocked()
}

// saveUsageLocked 持久化统计到文件（调用方需持有 usageMu）。
func (h *Handler) saveUsageLocked() {
	if h.usageFile == "" {
		return
	}
	data := map[string]any{
		"total":         h.usage,
		"daily":         h.usageDaily,
		"sources":       h.usageSrc,
		"sources_daily": h.usageSrcDaily,
		"saved_at":      time.Now(),
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.usageFile, b, 0644)
}

// loadUsage 启动时恢复统计（需在并发开始前调用）。
func (h *Handler) loadUsage() {
	if h.usageFile == "" {
		return
	}
	raw, err := os.ReadFile(h.usageFile)
	if err != nil {
		return
	}
	var d struct {
		Total        UsageStats                        `json:"total"`
		Daily        map[string]*UsageStats            `json:"daily"`
		Sources      map[string]*UsageStats            `json:"sources"`
		SourcesDaily map[string]map[string]*UsageStats `json:"sources_daily"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	h.usage = d.Total
	h.usageDaily = d.Daily
	if h.usageDaily == nil {
		h.usageDaily = map[string]*UsageStats{}
	}
	h.usageSrc = d.Sources
	if h.usageSrc == nil {
		h.usageSrc = map[string]*UsageStats{}
	}
	h.usageSrcDaily = d.SourcesDaily
	if h.usageSrcDaily == nil {
		h.usageSrcDaily = map[string]map[string]*UsageStats{}
	}
	// 老文件（本次改动之前存的）没有按来源的数据 —— 就地把历史全部归到「账号池」，
	// 保证不变量继续成立：各来源相加 == 总计。之后的新流量按真实来源记。
	// 代价：改动之前那部分走反代的流量会被算进池子（量很小，只在面板提示里说明）。
	if len(h.usageSrc) == 0 && h.usage.TotalTokens > 0 {
		cp := h.usage
		h.usageSrc = map[string]*UsageStats{"lobster": &cp}
	}
	if len(h.usageSrcDaily) == 0 && len(h.usageDaily) > 0 {
		h.usageSrcDaily = map[string]map[string]*UsageStats{}
		for day, v := range h.usageDaily {
			if v == nil {
				continue
			}
			cp := *v
			h.usageSrcDaily[day] = map[string]*UsageStats{"lobster": &cp}
		}
	}
}

// KeyUsage 单个 API Key 的使用统计。
type KeyUsage struct {
	LastUsed time.Time   `json:"last_used"`
	Count    int64       `json:"count"`
	Recent   []time.Time `json:"recent"` // 最近 20 次调用时间（用于判断是否真的在用）

	// ---- 2026-09-22 新增：面板要回答"这个 token 到底在干什么" ----
	// 总量 / 今日（按天自动清零）/ 模型分布 / 最近 30 条明细
	TotalTokens int64            `json:"total_tokens,omitempty"`
	TodayDay    string           `json:"today,omitempty"` // 今日计数属于哪一天
	TodayCalls  int64            `json:"today_calls,omitempty"`
	TodayTokens int64            `json:"today_tokens,omitempty"`
	Models      map[string]int64 `json:"models,omitempty"` // 模型名 → 累计调用次数
	Calls       []KeyCall        `json:"calls,omitempty"`  // 最近 30 条（新的在后）
}

// KeyCall 一条 key 调用明细。字段刻意少而准：面板上要能一眼看出
// "哪个 key、什么时候、调了什么模型、走了哪条上游、花了多少 token、成没成"。
type KeyCall struct {
	At     time.Time `json:"at"`
	Model  string    `json:"model,omitempty"`
	Source string    `json:"source,omitempty"` // lobster = 账号池，ch_xxx = 某条反代
	Tokens int64     `json:"tokens,omitempty"`
	Stream bool      `json:"stream,omitempty"`
	OK     bool      `json:"ok"`
	Note   string    `json:"note,omitempty"` // 失败原因（已截断）
}

// NewHandler 构建 handler。
func NewHandler(cfg Config) *Handler {
	if cfg.MaxRotate <= 0 {
		cfg.MaxRotate = 3
	}
	if cfg.HardCooldown <= 0 {
		cfg.HardCooldown = 12 * time.Hour
	}
	if cfg.SoftCooldown <= 0 {
		cfg.SoftCooldown = 60 * time.Second
	}
	if cfg.ErrThreshold <= 0 {
		cfg.ErrThreshold = 5
	}
	if cfg.ErrCooldown <= 0 {
		cfg.ErrCooldown = 3 * time.Minute
	}
	if cfg.RefreshSkew <= 0 {
		cfg.RefreshSkew = 10 * time.Minute
	}
	h := &Handler{cfg: cfg, mux: http.NewServeMux(), usageFile: cfg.UsageFile, routeFile: cfg.RouteFile}
	h.loadUsage()
	h.loadRoute()
	h.notesFile = cfg.NotesFile
	h.loadNotes()
	h.priceFile = cfg.PriceFile
	h.loadPrice()
	h.limitsFile = cfg.LimitsFile
	h.loadLimits()
	h.keyFile = cfg.KeyFile
	h.loadKeyStats()
	h.calls = newCallLog(cfg.CallLogFile)
	h.mux.HandleFunc("POST /v1/chat/completions", h.withAuth(h.chatCompletions))
	h.mux.HandleFunc("GET /v1/models", h.withAuth(h.models))
	// 只给面板用：龙虾账号池自己的模型表（不含任何反代接管的）。
	// 面板「模型测试」用它把「龙虾账号池」那一组跟各反代分组严格分开，
	// 保证一个模型名不会在两个组里重复出现（见 panel buildModelOptionsHtml）。
	h.mux.HandleFunc("GET /models/pool", h.withAuth(h.poolModels))
	// 只给面板用：反代账户余额 / 用量（尽力而为；DeepSeek 官方能拿真余额，new-api 类只能拿"已用"）
	h.mux.HandleFunc("GET /channels/balance", h.guard(h.channelsBalance))
	// 给反代配「后台登录态」（读账户余额用）
	h.mux.HandleFunc("POST /channels/login", h.guard(h.channelsLogin))
	// 2026-09-22 §111 安全修复：这几个是"读管理数据"的接口，之前漏了守卫，
	// 8367 暴露在公网时任何人都能直接读走 key 明细 / 账号池 / 用量。
	// 面板走本机回环（127.0.0.1）代理，加守卫后照旧能用。
	h.mux.HandleFunc("GET /status", h.guard(h.status))
	h.mux.HandleFunc("GET /events", h.guard(h.eventsHandler))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	h.mux.HandleFunc("GET /keys/stats", h.guard(h.keyStatsHandler))
	h.mux.HandleFunc("GET /calls/log", h.guard(h.callsLogHandler))
	h.mux.HandleFunc("GET /usage/stats", h.guard(h.usageStatsHandler))
	h.mux.HandleFunc("GET /notes", h.guard(h.notesList))
	h.mux.HandleFunc("POST /notes/set", h.guard(h.notesSet))
	h.mux.HandleFunc("GET /price", h.guard(h.priceHandler))
	h.mux.HandleFunc("POST /price/set", h.guard(h.priceHandler))
	h.mux.HandleFunc("GET /keylimits", h.guard(h.keyLimitsHandler))
	h.mux.HandleFunc("POST /keylimits/set", h.guard(h.keyLimitsHandler))
	h.mux.HandleFunc("POST /pool/reset", h.guard(h.poolReset))
	h.mux.HandleFunc("POST /pool/freeze", h.guard(h.poolFreeze))
	h.mux.HandleFunc("POST /reload", h.guard(h.reloadHandler))
	h.mux.HandleFunc("POST /keepalive", h.guard(h.keepaliveHandler))
	h.mux.HandleFunc("POST /checkin", h.guard(h.checkinHandler))
	h.mux.HandleFunc("GET /channels", h.guard(h.channelsList))
	h.mux.HandleFunc("POST /channels/add", h.guard(h.channelsAdd))
	h.mux.HandleFunc("POST /channels/delete", h.guard(h.channelsDelete))
	h.mux.HandleFunc("POST /channels/update", h.guard(h.channelsUpdate))
	h.mux.HandleFunc("POST /channels/toggle", h.guard(h.channelsToggle))
	h.mux.HandleFunc("POST /channels/reorder", h.guard(h.channelsReorder))
	h.mux.HandleFunc("POST /channels/test", h.guard(h.channelsTest))
	h.mux.HandleFunc("POST /channels/default", h.guard(h.channelsDefault))
	h.mux.HandleFunc("POST /channels/fallback", h.guard(h.channelsFallback))
	return h
}

// channelsFallback 反代失败时是否回退龙虾账号池（关掉 = 不消耗龙虾积分）。
func (h *Handler) channelsFallback(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		Enabled *bool  `json:"enabled"`
		Target  string `json:"target"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse body: "+err.Error())
		return
	}
	target := strings.TrimSpace(req.Target)
	if target == "" && req.Enabled != nil {
		// 兼容旧参数：true = 回退龙虾池，false = 关闭
		if *req.Enabled {
			target = "lobster"
		}
	}
	if target != "" && target != "auto" {
		t := h.resolveTarget(target)
		if t == "" {
			writeOpenAIError(w, http.StatusNotFound, "not_found", "unknown fallback target: "+target)
			return
		}
		target = t
	}
	if !h.cfg.Relay.SetFallback(target) {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "unknown fallback target: "+target)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"fallback": h.cfg.Relay.Fallback() != "",
		"target":   h.cfg.Relay.Fallback(),
	})
}

// channelsDefault 设置默认上游："" = 自动，"lobster" = 账号池，其他 = 渠道 ID。
func (h *Handler) channelsDefault(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		ID      string `json:"id"`
		Default string `json:"default"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse body: "+err.Error())
		return
	}
	if req.ID != "" {
		req.Default = req.ID
	}
	// 允许直接写渠道名 / 别名（龙虾、lobster、pool）
	if req.Default != "" {
		t := h.resolveTarget(req.Default)
		if t == "" {
			writeOpenAIError(w, http.StatusNotFound, "not_found", "unknown channel: "+req.Default)
			return
		}
		req.Default = t
	}
	if !h.cfg.Relay.SetDefault(req.Default) {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "unknown channel: "+req.Default)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "default": h.cfg.Relay.Default()})
}

// guard 渠道管理接口守卫：只允许本机回环或带正确 API Key 的请求。
func (h *Handler) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
				next(w, r)
				return
			}
		}
		valid := h.validKeys()
		token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if len(valid) > 0 && valid[token] {
			next(w, r)
			return
		}
		writeOpenAIError(w, http.StatusForbidden, "forbidden", "loopback or valid api key required")
	}
}

// ---------------------------------------------------------------------------
// 反代渠道（多渠道）管理
// ---------------------------------------------------------------------------

type channelReq struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	BaseURL string   `json:"base_url"`
	APIKey  string   `json:"api_key"`
	Models  []string `json:"models"`
	// ModelMap 模型名映射：客户端发来的名字 → 该渠道上游真正认的名字。
	// 用途：默认上游设成某反代时，把"池子那边叫惯了"的模型名也交给它（如 deepseek-flash=DeepSeek-V4.1-Flash）
	ModelMap map[string]string `json:"model_map"`
	Enabled  *bool             `json:"enabled"`
	Note     *string           `json:"note"`
	// Priority 优先级：数字小的先试；0 或不传 = 排到末尾。
	Priority *int `json:"priority"`
}

// channelsList 列出全部反代渠道（token 打码）。
func (h *Handler) channelsList(w http.ResponseWriter, r *http.Request) {
	out := []map[string]any{}
	if h.cfg.Relay != nil {
		for _, c := range h.cfg.Relay.List() {
			out = append(out, channelView(c))
		}
	}
	enabled := 0
	def := ""
	fbTarget := ""
	order := []string{"lobster"}
	if h.cfg.Relay != nil {
		enabled = h.cfg.Relay.MatchCount()
		def = h.cfg.Relay.Default()
		fbTarget = h.cfg.Relay.Fallback()
		order = h.cfg.Relay.Order()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels":        out,
		"count":           len(out),
		"enabled":         enabled,
		"default":         def,
		"fallback":        fbTarget != "",
		"fallback_target": fbTarget,
		// order = 路由顺序（含 "lobster" = 账号池的位置）；面板拖动改的就是它
		"order": order,
		"now":   time.Now(),
	})
}

// channelsAdd 新增渠道并做一次连通测试。
func (h *Handler) channelsAdd(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var req channelReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.BaseURL) == "" || strings.TrimSpace(req.APIKey) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "base_url 与 api_key 必填")
		return
	}
	ch := &relay.Channel{
		Name:     strings.TrimSpace(req.Name),
		BaseURL:  req.BaseURL,
		APIKey:   req.APIKey,
		Models:   req.Models,
		ModelMap: req.ModelMap,
		Enabled:  true,
	}
	if req.Priority != nil {
		ch.Priority = *req.Priority
	}
	if req.Note != nil {
		ch.Note = strings.TrimSpace(*req.Note)
	}
	if ch.Name == "" {
		ch.Name = "反代-" + ch.BaseURL
	}
	if req.Enabled != nil {
		ch.Enabled = *req.Enabled
	}
	v := probeChannel(ch)
	ch.OK = v.OK
	ch.LastTest = v.Desc
	ch.TestedAt = time.Now()
	if v.OK {
		ch.SeenModels = v.Sample
	}
	h.cfg.Relay.Add(ch)

	resp := map[string]any{"ok": v.OK, "channel": channelView(h.cfg.Relay.Get(ch.ID)), "detail": v.Desc}
	if v.Model != "" {
		resp["probe_model"] = v.Model
	}
	if v.Err != nil {
		resp["error"] = v.Err.Error()
	}
	if len(v.Sample) > 0 {
		resp["models_sample"] = firstN(v.Sample, 30)
	}
	writeJSON(w, http.StatusOK, resp)
}

// channelsUpdate 修改渠道（字段留空 = 不改；地址或 Token 变了自动重测）。
func (h *Handler) channelsUpdate(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var req channelReq
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id required")
		return
	}
	cur := h.cfg.Relay.Get(req.ID)
	if cur == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "channel not found")
		return
	}
	needTest := false
	if v := strings.TrimSpace(req.Name); v != "" {
		cur.Name = v
	}
	if v := strings.TrimSpace(req.BaseURL); v != "" && v != cur.BaseURL {
		cur.BaseURL = v
		needTest = true
	}
	if v := strings.TrimSpace(req.APIKey); v != "" && v != cur.APIKey {
		cur.APIKey = v
		needTest = true
	}
	if req.Models != nil {
		cur.Models = req.Models
	}
	if req.ModelMap != nil {
		cur.ModelMap = req.ModelMap
	}
	if req.Note != nil {
		cur.Note = strings.TrimSpace(*req.Note)
	}
	if req.Priority != nil {
		cur.Priority = *req.Priority
	}
	if req.Enabled != nil {
		cur.Enabled = *req.Enabled
	}
	if cur.Name == "" {
		cur.Name = "反代-" + cur.BaseURL
	}
	h.cfg.Relay.Replace(cur)
	resp := map[string]any{"ok": true}
	if needTest {
		v := probeChannel(cur)
		h.cfg.Relay.SetTest(cur.ID, v.OK, v.Desc)
		if v.OK {
			h.cfg.Relay.SetSeen(cur.ID, v.Sample)
		}
		resp["ok"] = v.OK
		resp["detail"] = v.Desc
		resp["models"] = v.Models
		resp["models_sample"] = firstN(v.Sample, 30)
		if v.Model != "" {
			resp["probe_model"] = v.Model
		}
		if v.Err != nil {
			resp["error"] = v.Err.Error()
		}
	}
	resp["channel"] = channelView(h.cfg.Relay.Get(cur.ID))
	writeJSON(w, http.StatusOK, resp)
}

// channelsDelete 删除渠道。
func (h *Handler) channelsDelete(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req channelReq
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id required")
		return
	}
	// 只删渠道配置与相关引用（default / fallback 由 relay.Delete 内部重置）。
	// 所有历史记录一律保留：回退事件明细(fallbacks)、回退去向计数(fallback_dest)、
	// Token 使用统计(usage.json)、累计路由计数 —— 删渠道不该抹掉历史。
	writeJSON(w, http.StatusOK, map[string]any{"ok": h.cfg.Relay.Delete(req.ID)})
}

// channelsToggle 启用/停用渠道。
func (h *Handler) channelsToggle(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req channelReq
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" || req.Enabled == nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id + enabled required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": h.cfg.Relay.SetEnabled(req.ID, *req.Enabled)})
}

// channelsReorder 调整渠道优先级（= 路由顺序）。
// body: {"ids":["ch_a","ch_b",...]} —— 第 1 个优先级最高，依次 +10。
// 用在两处：① 多个渠道都接管同一个模型时先试谁；② 反代失败后兜底扫渠道的顺序。
func (h *Handler) channelsReorder(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.IDs) == 0 {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "ids required")
		return
	}
	n, ok := h.cfg.Relay.Reorder(req.IDs)
	if !ok {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "unknown channel id in ids（允许的 ID：各渠道 ID 与 \"lobster\"）")
		return
	}
	out := []map[string]any{}
	for _, c := range h.cfg.Relay.List() {
		out = append(out, channelView(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "reordered": n, "channels": out, "order": h.cfg.Relay.Order()})
}

// channelsTest 重新测试渠道连通性。
func (h *Handler) channelsTest(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req channelReq
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id required")
		return
	}
	ch := h.cfg.Relay.Get(req.ID)
	if ch == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "channel not found")
		return
	}
	v := probeChannel(ch)
	h.cfg.Relay.SetTest(ch.ID, v.OK, v.Desc)
	// 测试发现的模型落盘：这样即使「接管模型」留空，面板「模型测试」里也看得到这个反代
	if v.OK {
		h.cfg.Relay.SetSeen(ch.ID, v.Sample)
	}
	resp := map[string]any{
		"ok":            v.OK,
		"models":        v.Models,
		"models_sample": firstN(v.Sample, 30),
		"detail":        v.Desc,
	}
	resp["seen_models"] = firstN(v.Sample, 200)
	if v.Model != "" {
		resp["probe_model"] = v.Model
	}
	if v.Err != nil {
		resp["error"] = v.Err.Error()
		resp["probe_error"] = v.Err.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

// channelModelList 渠道「对外可见」的模型名 = 接管声明 ∪ 最近一次实测到的模型。
//
// 为什么要有这个：面板加反代时「接管模型」可以不填（只存不接管）。以前这种情况
// 该渠道在 /v1/models 和面板「模型测试」里就彻底消失了 —— 用户看到的现象就是
// 「我加的反代在模型测试里没有它」。实测到的模型是真实证据，拿它兜底最准。
func channelModelList(c *relay.Channel) []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.Models)+len(c.SeenModels))
	seen := map[string]bool{}
	for _, m := range append(append([]string(nil), c.Models...), c.SeenModels...) {
		m = strings.TrimSpace(m)
		if m == "" || seen[strings.ToLower(m)] {
			continue
		}
		seen[strings.ToLower(m)] = true
		out = append(out, m)
	}
	return out
}

// channelView 面板视图：token 打码。
func channelView(c *relay.Channel) map[string]any {
	if c == nil {
		return nil
	}
	return map[string]any{
		"id":           c.ID,
		"name":         c.Name,
		"base_url":     c.BaseURL,
		"api_key":      maskKey(c.APIKey),
		"key_len":      len(c.APIKey),
		"models":       c.Models,
		"model_map":    c.ModelMap,
		"seen_models":  c.SeenModels,
		"has_login":    len(channelLogins(c)) > 0,
		"logins_count": len(channelLogins(c)),
		"keys_count":   len(c.AccountKeys()),
		"login_names": func() []string {
			ls := channelLogins(c)
			out := make([]string, 0, len(ls))
			for _, l := range ls {
				n := l.Phone
				if n == "" {
					n = l.Username
				}
				if n == "" {
					n = "未命名"
				}
				out = append(out, n)
			}
			return out
		}(),
		"note":       c.Note,
		"priority":   c.Priority,
		"enabled":    c.Enabled,
		"ok":         c.OK,
		"last_test":  c.LastTest,
		"tested_at":  c.TestedAt,
		"created_at": c.CreatedAt,
		"endpoint":   c.Endpoint(),
	}
}

func maskKey(k string) string {
	k = strings.TrimSpace(k)
	if k == "" {
		return ""
	}
	if len(k) <= 10 {
		return "****"
	}
	return k[:6] + "..." + k[len(k)-4:]
}

func firstN(in []string, n int) []string {
	if len(in) > n {
		return in[:n]
	}
	return in
}

// probeModel 挑一个"能真发一单"的模型名：优先渠道自己声明的，其次 /models 列表里
// 第一个像 chat 的（跳过 embedding / rerank / tts / 图像 这些不能当 chat 用的）。
func probeModel(declared, listed []string) string {
	bad := func(m string) bool {
		m = strings.ToLower(m)
		for _, k := range []string{"embed", "rerank", "tts", "whisper", "audio", "image", "video", "moderation", "dall", "stable-diffusion", "-vision", "vision-"} {
			if strings.Contains(m, k) {
				return true
			}
		}
		return false
	}
	for _, m := range declared {
		m = strings.TrimSpace(m)
		if m != "" && m != "*" && !bad(m) {
			return m
		}
	}
	for _, m := range listed {
		m = strings.TrimSpace(m)
		if m != "" && !bad(m) {
			return m
		}
	}
	return ""
}

// probeResult 一次"真测"的结论。光 /models 通不算数，必须再真发一单。
type probeResult struct {
	OK     bool
	Models int
	Sample []string
	Model  string // 真实调用用的模型名
	Err    error
	Desc   string // 写进渠道 last_test 的描述
}

// probeChannel 两段式探活：① /models 验地址+Token；② 真发一单 max_tokens=1 验额度/限流。
func probeChannel(ch *relay.Channel) probeResult {
	n, ids, err := relay.Test(ch, 15*time.Second)
	if err != nil {
		return probeResult{OK: false, Err: err, Desc: "测试失败: " + err.Error()}
	}
	pm := probeModel(ch.Models, ids)
	if pm == "" {
		return probeResult{OK: true, Models: n, Sample: ids,
			Desc: fmt.Sprintf("测试通过（%d 个模型；没找到可真发的 chat 模型名，只验了接口）", n)}
	}
	if perr := relay.ChatProbe(ch, pm, 25*time.Second); perr != nil {
		return probeResult{OK: false, Models: n, Sample: ids, Model: pm, Err: perr,
			Desc: fmt.Sprintf("接口通(%d 个模型)但真发一单失败，模型 %s：%s", n, pm, perr.Error())}
	}
	return probeResult{OK: true, Models: n, Sample: ids, Model: pm,
		Desc: fmt.Sprintf("测试通过：%d 个模型，真实调用 %s 正常", n, pm)}
}

// usageStatsHandler 返回累计 token 消耗。
func (h *Handler) usageStatsHandler(w http.ResponseWriter, r *http.Request) {
	h.usageMu.Lock()
	u := h.usage
	daily := map[string]UsageStats{}
	for k, v := range h.usageDaily {
		daily[k] = *v
	}
	today := time.Now().Format("2006-01-02")
	// 按上游来源快照（总和恒等于上面的 u / daily[today]）
	srcTotal := map[string]UsageStats{}
	for k, v := range h.usageSrc {
		srcTotal[k] = *v
	}
	srcToday := map[string]UsageStats{}
	for k, v := range h.usageSrcDaily[today] {
		srcToday[k] = *v
	}
	h.usageMu.Unlock()
	route, fallbacks := h.routeSnapshot()
	pin, pcac, pout, pcny := h.priceSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"usage":         u,
		"daily":         daily,
		"today":         daily[today],
		"sources":       srcTotal,
		"sources_today": srcToday,
		"route":         route,
		"route_since":   h.routeSinceTime(),
		"price":         map[string]any{"input_per_m": pin, "cached_per_m": pcac, "output_per_m": pout, "cny_rate": pcny},
		"fallbacks":     fallbacks,
		"now":           time.Now(),
	})
}

func (h *Handler) routeSinceTime() time.Time {
	h.routeMu.Lock()
	defer h.routeMu.Unlock()
	return h.routeSince
}

// keyStatsHandler 返回各 API Key 的最后使用时间与调用次数（用于面板判断哪个 key 在用）。
func (h *Handler) keyStatsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"keys": h.keyStatsSnapshot(),
		"now":  time.Now(),
	})
}

// callsLogHandler 调用流水查询（面板「记录」那张表）。
//
//	GET /calls/log?key=sk-xxx&from=2026-09-22&to=2026-09-22&limit=200
//
// 返回 {"ok":true,"total":N,"rows":[...]}；rows 新的在前。
// 不传 key = 所有 key 合在一起（面板目前按 key 查，留着这个能力）。
func (h *Handler) callsLogHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	total, rows := h.calls.Query(strings.TrimSpace(q.Get("key")),
		strings.TrimSpace(q.Get("from")), strings.TrimSpace(q.Get("to")), limit, offset)
	if rows == nil {
		rows = []CallLogEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "total": total, "rows": rows, "now": time.Now(),
	})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		valid := h.validKeys()
		if len(valid) > 0 {
			authz := r.Header.Get("Authorization")
			token := ""
			if strings.HasPrefix(authz, "Bearer ") {
				token = strings.TrimPrefix(authz, "Bearer ")
			}
			if token == "" || !valid[token] {
				writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
				return
			}
			// 把这次用的 key 挂进请求上下文：chatCompletions 要拿它记
			// "这个 token 到底在干什么"（模型 / 上游 / token 数 / 成败）
			r = r.WithContext(context.WithValue(r.Context(), keyCtx{}, token))
			// 面板自身的管理/探活请求带内部标记，不计入使用统计
			if r.Header.Get("X-Lobster-Internal") != "1" {
				ok, kind, limit := h.allowKey(token)
				if !ok {
					if kind == "daily_quota" {
						writeOpenAIError(w, http.StatusTooManyRequests, "daily_quota_exceeded",
							fmt.Sprintf("该 Key 今日额度已用完（%d 单/天），北京时间明天自动重置", limit))
					} else {
						writeOpenAIError(w, http.StatusTooManyRequests, kind,
							fmt.Sprintf("该 Key 限速 %d 次/分钟，已排队等待仍未轮到，请降低并发或稍后重试", limit))
					}
					return
				}
				h.noteKeyUse(token)
			}
		}
		next(w, r)
	}
}

// keyCtx 请求上下文里放"这次用的是哪个 API Key"，给"这个 token 在干什么"用。
// 用自定义空结构体当 key，避免和别的包塞进 context 的值撞名。
type keyCtx struct{}

func keyFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(keyCtx{}).(string); ok {
		return v
	}
	return ""
}

// noteKeyUse 记录某 key 的一次使用（最后一次时间 + 累计次数）。
func (h *Handler) noteKeyUse(k string) {
	h.keyMu.Lock()
	defer h.keyMu.Unlock()
	if h.keyStats == nil {
		h.keyStats = map[string]*KeyUsage{}
	}
	st := h.keyStats[k]
	if st == nil {
		st = &KeyUsage{}
		h.keyStats[k] = st
	}
	st.LastUsed = time.Now()
	st.Count++
	st.Recent = append(st.Recent, st.LastUsed)
	if len(st.Recent) > 20 {
		st.Recent = st.Recent[len(st.Recent)-20:]
	}
	h.saveKeyStatsLocked()
}

// keySaveInterval 统计落盘节流间隔：请求热路径上每次都写盘太亏，
// 距上次落盘 <5s 时先标记脏，由后台协程延迟补写（调用方需持有 keyMu）。
const keySaveInterval = 5 * time.Second

func (h *Handler) saveKeyStatsLocked() {
	if h.keyFile == "" {
		return
	}
	if time.Since(h.keyLastSave) < keySaveInterval {
		if !h.keyDirty {
			h.keyDirty = true
			go func() {
				time.Sleep(keySaveInterval)
				h.keyMu.Lock()
				h.keyDirty = false
				h.flushKeyStatsLocked()
				h.keyMu.Unlock()
			}()
		}
		return
	}
	h.keyLastSave = time.Now()
	h.flushKeyStatsLocked()
}

// flushKeyStatsLocked 立即把统计写盘（调用方需持有 keyMu）。
func (h *Handler) flushKeyStatsLocked() {
	if h.keyFile == "" {
		return
	}
	b, err := json.MarshalIndent(map[string]any{
		"keys":     h.keyStats,
		"saved_at": time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(h.keyFile, b, 0644)
}

// loadKeyStats 启动时恢复 key 使用统计（否则每次重启面板都显示"未使用"）。
func (h *Handler) loadKeyStats() {
	h.keyStats = map[string]*KeyUsage{}
	if h.keyFile == "" {
		return
	}
	raw, err := os.ReadFile(h.keyFile)
	if err != nil {
		return
	}
	var d struct {
		Keys map[string]*KeyUsage `json:"keys"`
	}
	if json.Unmarshal(raw, &d) == nil && d.Keys != nil {
		h.keyStats = d.Keys
	}
}

// keyStatsSnapshot 返回 key 使用统计快照。
func (h *Handler) keyStatsSnapshot() map[string]KeyUsage {
	h.keyMu.Lock()
	defer h.keyMu.Unlock()
	out := map[string]KeyUsage{}
	for k, v := range h.keyStats {
		// 深拷贝：面板是另一个 goroutine 在读，切片/map 不能和请求热路径共享底层数组
		// （否则 append 与遍历并发就是 data race；这也是老代码 out[k] = *v 的隐患）
		cp := *v
		if len(v.Recent) > 0 {
			cp.Recent = append([]time.Time(nil), v.Recent...)
		}
		if len(v.Calls) > 0 {
			cp.Calls = append([]KeyCall(nil), v.Calls...)
		}
		if len(v.Models) > 0 {
			cp.Models = make(map[string]int64, len(v.Models))
			for mk, mv := range v.Models {
				cp.Models[mk] = mv
			}
		}
		out[k] = cp
	}
	return out
}

// noteKeyCall 记一条 key 调用明细（面板「这个 token 在干什么」的数据源）。
//
// 只在 key 已经有统计条目时才记（noteKeyUse 会先建），
// 这样面板自己的内部探活请求（带 X-Lobster-Internal）不会凭空造出一行。
func (h *Handler) noteKeyCall(k string, c KeyCall) {
	if strings.TrimSpace(k) == "" {
		return
	}
	h.keyMu.Lock()
	defer h.keyMu.Unlock()
	st := h.keyStats[k]
	if st == nil {
		return
	}
	day := time.Now().Format("2006-01-02")
	if st.TodayDay != day {
		st.TodayDay, st.TodayCalls, st.TodayTokens = day, 0, 0
	}
	if strings.TrimSpace(c.Model) != "" {
		if st.Models == nil {
			st.Models = map[string]int64{}
		}
		st.Models[c.Model]++
	}
	st.TodayCalls++
	st.TodayTokens += c.Tokens
	st.TotalTokens += c.Tokens
	st.Calls = append(st.Calls, c)
	if len(st.Calls) > 30 {
		st.Calls = st.Calls[len(st.Calls)-30:]
	}
	h.saveKeyStatsLocked()
}

// validKeys 汇总生效 key 集合（APIKey + APIKeys 去重）。
func (h *Handler) validKeys() map[string]bool {
	// 热重载后的集合优先（面板增删 Key 后无需重启主服务）
	h.keysMu.RLock()
	hot := h.keysHot
	h.keysMu.RUnlock()
	if hot != nil {
		out := make(map[string]bool, len(hot))
		for k := range hot {
			out[k] = true
		}
		return out
	}
	out := map[string]bool{}
	if strings.TrimSpace(h.cfg.APIKey) != "" {
		out[strings.TrimSpace(h.cfg.APIKey)] = true
	}
	for _, k := range h.cfg.APIKeys {
		k = strings.TrimSpace(k)
		if k != "" {
			out[k] = true
		}
	}
	return out
}

// SetValidKeys 热更新生效 key 集合（面板增删 Key 后调用，不需要重启主服务）。
// list 为空时忽略，避免误清空成"无鉴权"模式。
func (h *Handler) SetValidKeys(list []string) int {
	next := map[string]bool{}
	for _, k := range list {
		k = strings.TrimSpace(k)
		if k != "" {
			next[k] = true
		}
	}
	if len(next) == 0 {
		return 0
	}
	h.keysMu.Lock()
	h.keysHot = next
	h.keysMu.Unlock()
	return len(next)
}

// SetReload 注入重载逻辑（由 main 提供：重读 config.json + 重扫 auths 目录）。
func (h *Handler) SetReload(fn func() (int, int, error)) {
	h.keysMu.Lock()
	h.reloadFn = fn
	h.keysMu.Unlock()
}

// reloadHandler POST /reload —— 热重载 key 列表与账号目录，不重启进程。
// 为什么需要它：APIKey/APIKeys 与 auths 目录都是**启动时读一次**，
// 面板增删 Key / 增删账号后若不重启，运行中的进程看不到变更。
// 有了这个接口，面板改完直接调一次即可，请求不中断、统计不丢失。
func (h *Handler) reloadHandler(w http.ResponseWriter, r *http.Request) {
	h.keysMu.RLock()
	fn := h.reloadFn
	h.keysMu.RUnlock()
	if fn == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "reload not wired"})
		return
	}
	keys, accts, err := fn()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	log.Printf("[reload] 热重载完成: keys=%d accounts=%d", keys, accts)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "keys": keys, "accounts": accts})
}

// SetKeepalive 注入 keepalive 逻辑（由 main 提供：立刻刷新所有账号 token）。
func (h *Handler) SetKeepalive(fn func() (int, int, int)) {
	h.keysMu.Lock()
	h.keepaliveFn = fn
	h.keysMu.Unlock()
}

// keepaliveHandler POST /keepalive —— 立刻跑一轮 token 续期，不等 22:00 定时。
//
// 为什么需要：token 有效期 30 天、refreshToken 180 天，定时任务在 22:00 才跑。
// 但服务器重启/改配置后，或者想确认「续期这条路还通不通」时，没法手动触发。
// 有了这个接口就能随时点名续一次，返回成功/失败/封禁数量。
//
// 注意：会逐个打上游 /api/auth/refresh，40 个账号约 5-10 秒，属于慢接口（别放前端自动轮询）。
func (h *Handler) keepaliveHandler(w http.ResponseWriter, r *http.Request) {
	h.keysMu.RLock()
	fn := h.keepaliveFn
	h.keysMu.RUnlock()
	if fn == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "keepalive not wired"})
		return
	}
	ok, failed, banned := fn()
	log.Printf("[keepalive] 手动续期完成: ok=%d failed=%d banned=%d", ok, failed, banned)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "refreshed": ok, "failed": failed, "banned": banned,
	})
}

// SetCheckin 注入「每日积分礼」逻辑（由 main 提供：立刻给所有账号领一轮）。
func (h *Handler) SetCheckin(fn func() map[string]any) {
	h.keysMu.Lock()
	h.checkinFn = fn
	h.keysMu.Unlock()
}

// checkinHandler POST /checkin —— 立刻领一轮「每日积分礼」，不等 09:00 / 21:00 定时。
//
// 为什么需要：每个账号每天能领 100 积分，定时任务只在 9 点 / 21 点跑；
// 重启、补号、或者想立刻确认「这条路还通不通」时得有个手动入口。
// 同一天重复领是安全的：上游按天记账，第二次会返回「今天已领」。
func (h *Handler) checkinHandler(w http.ResponseWriter, r *http.Request) {
	h.keysMu.RLock()
	fn := h.checkinFn
	h.keysMu.RUnlock()
	if fn == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "checkin not wired"})
		return
	}
	res := fn()
	log.Printf("[checkin] 手动领取完成: %v", res)
	res["ok"] = true
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	notes := h.notesSnapshot()
	list := h.cfg.Pool.List()
	out := make([]map[string]any, 0, len(list))
	for _, a := range list {
		m := map[string]any{
			"uid":       a.UID,
			"nickname":  a.Nickname,
			"credits":   a.Credits,
			"cooling":   a.Cooling,
			"until":     a.Until,
			"reason":    a.Reason,
			"disabled":  a.Disabled,
			"err_count": a.ErrCount,
			"note":      notes[a.UID],
			"inflight":  a.Inflight,
			// 今日用量（按账号记账，见 pool.NoteRequest / accBurnLocked）
			"today_requests": a.TodayRequests,
			"today_tokens":   a.TodayTokens,
			"today_burn":     a.TodayBurn,
		}
		// 邀请进度（2026-09-26 用户要求：面板要显示每个号已邀请几人）
		if a.InvitedCount > 0 || a.InviteCode != "" {
			m["invited_count"] = a.InvitedCount
			m["invite_code"] = a.InviteCode
			m["invite_stage1"] = a.InviteStage1
			m["invite_stage1_done"] = a.InviteStage1Done
			m["invite_reward"] = a.InviteReward
		}
		// 添加时间（第一次进池子的时刻，落盘 data/pool-added.json）：
		// 面板「账号池」按它排账号先后（2026-09-25 用户要求）。
		// 零值不发 —— 让前端走「没时间 → 按 uid 兜底」，别把 0001-01-01 当成真时间。
		if !a.AddedAt.IsZero() {
			m["added_at"] = a.AddedAt
		}
		out = append(out, m)
	}
	upN, upLast, reqN, reqLast := h.faultsSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts": out,
		"faults": map[string]any{
			"upstream_60s":  upN,
			"upstream_last": upLast,
			"request_60s":   reqN,
			"request_last":  reqLast,
			"err_threshold": h.cfg.ErrThreshold,
			"err_cooldown":  h.cfg.ErrCooldown.String(),
			"note":          "5xx/超时/4xx 归因到上游或请求本身，不再冷却账号",
		},
	})
}

// eventsHandler 返回账号池最近的状态变更事件（冷却/解冻/禁用）与错误归因。
func (h *Handler) eventsHandler(w http.ResponseWriter, r *http.Request) {
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 && k <= 2000 {
			n = k
		}
	}
	ev := h.cfg.Pool.RecentEvents(n)
	if ev == nil {
		ev = []pool.Event{}
	}
	upN, upLast, reqN, reqLast := h.faultsSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"events": ev,
		"faults": map[string]any{
			"upstream_60s":  upN,
			"upstream_last": upLast,
			"request_60s":   reqN,
			"request_last":  reqLast,
		},
	})
}

// notesList 账号备注列表。
func (h *Handler) notesList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"notes": h.notesSnapshot()})
}

// notesSet 设置/清除某个账号的备注。
func (h *Handler) notesSet(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		UID  string `json:"uid"`
		Note string `json:"note"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.UID) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "uid required")
		return
	}
	note := strings.TrimSpace(req.Note)
	if len(note) > 200 {
		note = note[:200]
	}
	h.notesMu.Lock()
	if h.notes == nil {
		h.notes = map[string]string{}
	}
	if note == "" {
		delete(h.notes, req.UID)
	} else {
		h.notes[req.UID] = note
	}
	h.saveNotesLocked()
	h.notesMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "uid": req.UID, "note": note})
}

// 静态模型表（动态接口失败时的回退）。
// 2026-08-06 从 GET /api/models/available 实测拉取，共 19 个。
var staticModels = []map[string]any{
	{"id": "deepseek-v4-flash", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "deepseek-v4-pro", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "MiniMax-M3", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "MiniMax-M2.7", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "qwen3.7-max", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "qwen3.7-plus", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "qwen3.6-plus", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "qwen3.5-plus-2026-04-20", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "kimi-k2.7-code", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "kimi-k2.7-code-highspeed", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "kimi-k2.6", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "kimi-k2.5", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "doubao-seed-2-1-pro-260628", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "doubao-seed-2-1-turbo-260628", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "doubao-seed-2-0-code-preview-260215", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "glm-5.2", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "glm-5.1", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "glm-5v-turbo", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
	{"id": "glm-5", "object": "model", "created": 1753600000, "owned_by": "lobsterai", "context_length": 131072},
}

// dynamicModelsCache 动态模型缓存。
var dynamicModelsCache struct {
	sync.RWMutex
	ids     []string
	fetched time.Time
}

const dynamicModelsTTL = time.Hour

// models 返回模型列表：优先动态（缓存 1h），失败回退静态表。
func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   h.modelList(),
	})
}

// quotaWords 「上游额度用完」类的字样（含上游的推广口吻，如"请升级套餐"）
var quotaWords = []string{
	"免费额度", "额度已用完", "额度用完", "额度用尽", "额度不足", "余额不足", "积分不足", "积分用完",
	"请升级套餐", "升级套餐", "quota exceeded", "insufficient balance", "insufficient credit",
	"insufficient quota", "out of credit", "payment required", "free quota",
}

// isQuotaErr 这次失败是不是"上游额度用完"（是的话值得换一把 key / 换一条线路重试）
func isQuotaErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	low := strings.ToLower(s)
	for _, w := range quotaWords {
		if strings.Contains(low, strings.ToLower(w)) || strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// isModelMissingErr 上游明确表示"这个模型不支持 / 不存在"。
//
// 用于两处（2026-09-22 体检实测：客户端发 no-such-model-xyz 时，
// 我们换了 5 个号、白打 5 次上游，最后还报 503 "all accounts unavailable"）：
//  1. 换号循环里 → 这类错误换号也没用，立刻停止换号
//  2. 最终归因 → 报 404 model_not_found，而不是"线路全挂了"
//
// 注意：40301「模型不可见或无访问权限」**不在**这一列 —— 那是按账号算的，必须继续换号。
func isModelMissingErr(b []byte) bool {
	s := string(b)
	for _, m := range []string{"不支持的模型", "模型不存在", "模型未找到", "model not found", "unsupported model", "no such model", "unknown model"} {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// isRateLimitErr 这次失败是不是"上游限速"（HTTP 429 / rate_limit_exceeded / tpm）。
//
// 为什么要和 isQuotaErr 并列（2026-09-22 实测）：
// 用户 Tier 反代配了 3 把令牌，但连打 6 单全挂，日志清一色
//
//	渠道 Tier http 429: {"error":{"message":"rate_limit_exceeded: tpm ..."}}
//
// 原因是限速走的是"额度用完"以外的那条路 —— 循环 `break` 了，**根本不换号**。
// 而 tpm/rpm 限额是**按账号算的**，换一把就是另一份额度。
func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	low := strings.ToLower(err.Error())
	if strings.Contains(low, "http 429") || strings.Contains(low, "status 429") {
		return true
	}
	for _, w := range []string{"rate_limit", "rate limit", "too many requests", "tpm limit", "rpm limit", "rate-limit"} {
		if strings.Contains(low, w) {
			return true
		}
	}
	return false
}

// sanitizeUpstreamMsg 别把上游的推广文案（"免费额度已用完，请升级套餐"）原样甩给客户。
// 用户明确要求：不要显示这个 —— 换成中立话术，同时告诉客户网关已经自动换线了。
func sanitizeUpstreamMsg(s string) string {
	if s == "" {
		return s
	}
	low := strings.ToLower(s)
	for _, w := range quotaWords {
		if strings.Contains(low, strings.ToLower(w)) || strings.Contains(s, w) {
			return "上游额度暂时不可用，网关已自动切换其它线路，请稍后重试"
		}
	}
	return s
}

// httpPostJSONH 带自定义请求头的 POST JSON，返回解析结果 + 原始 body + 状态码。
func httpPostJSONH(u string, hdr map[string]string, body any) (map[string]any, string, int) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err.Error(), 0
	}
	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(string(b)))
	if err != nil {
		return nil, err.Error(), 0
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		if strings.TrimSpace(v) != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := balanceClient.Do(req)
	if err != nil {
		return nil, err.Error(), 0
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m, string(raw), resp.StatusCode
}

// newAPITokenItem new-api 的令牌条目（列表接口里的 key 是**打码**的）
type newAPITokenItem struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Key  string `json:"key"`
}

// listNewAPITokens 用登录态列出该账号的令牌
func listNewAPITokens(root string, cred loginCred) ([]newAPITokenItem, error) {
	for _, hdr := range cred.attempts() {
		m, raw, code := httpGetJSONH(root+"/api/token/?p=0&page_size=100", hdr)
		if code != 200 || m == nil {
			continue
		}
		var d struct {
			Success bool `json:"success"`
			Data    struct {
				Items []newAPITokenItem `json:"items"`
			} `json:"data"`
		}
		if json.Unmarshal([]byte(raw), &d) != nil || !d.Success {
			continue
		}
		return d.Data.Items, nil
	}
	return nil, fmt.Errorf("令牌列表拿不到（登录态可能已失效）")
}

// createNewAPIToken 用登录态给该账号新建一把令牌，返回完整的 sk-（列表接口拿不到明文，只有这里能拿到）
func createNewAPIToken(root string, cred loginCred, name string) (string, error) {
	body := map[string]any{
		"name": name, "remain_quota": 0, "expired_time": -1,
		"unlimited_quota": true, "model_limits_enabled": false,
		"model_limits": "", "allow_ips": "", "group": "",
	}
	var lastErr string
	for _, hdr := range cred.attempts() {
		m, raw, code := httpPostJSONH(root+"/api/token/", hdr, body)
		if code != 200 || m == nil {
			lastErr = fmt.Sprintf("http %d", code)
			continue
		}
		var d struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
			Data    struct {
				Key string `json:"key"`
			} `json:"data"`
		}
		if json.Unmarshal([]byte(raw), &d) != nil {
			lastErr = "返回解析失败"
			continue
		}
		if !d.Success || strings.TrimSpace(d.Data.Key) == "" {
			lastErr = strings.TrimSpace(d.Message)
			if lastErr == "" {
				lastErr = "上游没返回 key"
			}
			continue
		}
		k := strings.TrimSpace(d.Data.Key)
		if !strings.HasPrefix(k, "sk-") {
			k = "sk-" + k
		}
		return k, nil
	}
	return "", fmt.Errorf("建令牌失败：%s", lastErr)
}

// maskPrefixSuffix 把打码的 key（sk-abcd**********wxyz）拆成前缀/后缀，用来跟明文比对
func maskPrefixSuffix(masked string) (string, string) {
	if i := strings.Index(masked, "*"); i >= 0 {
		pre := masked[:i]
		rest := strings.TrimLeft(masked[i:], "*")
		return pre, rest
	}
	return "", ""
}

// ---------------------------------------------------------------------------
// 反代账户余额 / 用量查询（尽力而为，拿不到就如实说拿不到）

// relayBalance 归一化后的余额/用量。Total / Used / Remaining 是"拿不到就是 null"。
type relayBalance struct {
	OK        bool     `json:"ok"`
	Kind      string   `json:"kind,omitempty"`     // deepseek-official / new-api / unknown
	Currency  string   `json:"currency,omitempty"` // CNY / USD
	Total     *float64 `json:"total,omitempty"`
	Used      *float64 `json:"used,omitempty"`
	Remaining *float64 `json:"remaining,omitempty"`
	Unlimited bool     `json:"unlimited,omitempty"` // 这个 key 没有额度上限（余额得登录上游后台看）
	Auth      string   `json:"auth,omitempty"`      // 这次是用什么登录态读到的：cookie / bearer
	// 后台账号信息（new-api 的 /api/user/self）—— 面板反代卡片要像龙虾账号那样显示这些
	Username string `json:"username,omitempty"`
	// Phone tierflow 这类站把手机号当"账号名"用（形如 187****5260），面板优先显示它
	Phone    string `json:"phone,omitempty"`
	UID      string `json:"uid,omitempty"`
	Email    string `json:"email,omitempty"`
	Group    string `json:"group,omitempty"`
	Requests int64  `json:"requests,omitempty"`
	Role     int64  `json:"role,omitempty"`
	Status   int64  `json:"status,omitempty"`
	Created  int64  `json:"created_time,omitempty"`
	TwoFA    bool   `json:"twofa_enabled,omitempty"`
	WeChat   bool   `json:"wechat_bound,omitempty"`
	HasPwd   bool   `json:"has_password,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

var (
	balanceMu    sync.Mutex
	balanceCache = map[string]relayBalance{}
	balanceAt    = map[string]time.Time{}
)

const balanceTTL = 60 * time.Second

var balanceClient = &http.Client{Timeout: 15 * time.Second}

// fetchNewAPIAccountBalance 用「后台登录态」读 new-api / one-api 的**账户**额度。
//
// 为什么需要它：new-api 用 API key 只能拿到"这个 key 已用多少"（/v1/dashboard/billing/usage），
// 账户真正的剩余额度只在 /api/user/self 里，且必须带**登录后**的 access_token（不是 sk- key）。
// 返回 (余额, 是否成功)。失败就让上层继续走别的探测路径。
func fetchNewAPIAccountBalance(root, token string) (relayBalance, bool) {
	for _, hdr := range parseLoginCred(token).attempts() {
		if b, ok := tryNewAPIAccountBalance(root, hdr); ok {
			if _, isCookie := hdr["Cookie"]; isCookie {
				b.Auth = "cookie"
			} else {
				b.Auth = "bearer"
			}
			return b, true
		}
	}
	return relayBalance{}, false
}

// tryNewAPIAccountBalance 用一组请求头去问 /api/user/self，成功就解析出额度。
func tryNewAPIAccountBalance(root string, hdr map[string]string) (relayBalance, bool) {
	m, raw, code := httpGetJSONH(root+"/api/user/self", hdr)
	if code != 200 || m == nil {
		return relayBalance{}, false
	}
	var d struct {
		Success bool `json:"success"`
		Data    struct {
			Quota        float64 `json:"quota"`
			UsedQuota    float64 `json:"used_quota"`
			Username     string  `json:"username"`
			DisplayName  string  `json:"display_name"`
			Phone        string  `json:"phone"`
			UID          string  `json:"uid"`
			Email        string  `json:"email"`
			Group        string  `json:"group"`
			RequestCount float64 `json:"request_count"`
			Role         float64 `json:"role"`
			Status       float64 `json:"status"`
			CreatedTime  float64 `json:"created_time"`
			TwoFA        bool    `json:"twofa_enabled"`
			WeChatBound  bool    `json:"wechat_bound"`
			HasPassword  bool    `json:"has_password"`
			ID           float64 `json:"id"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(raw), &d) != nil || !d.Success {
		return relayBalance{}, false
	}
	// quota_per_unit：new-api 的"1 美元 = 多少 quota"，默认 500000
	per := 500000.0
	if m, _, code := httpGetJSON(root+"/api/status", ""); code == 200 && m != nil {
		if dm, ok := m["data"].(map[string]any); ok {
			if v, ok := jsonFloat(dm["quota_per_unit"]); ok && v > 0 {
				per = v
			}
		}
	}
	bal := d.Data.Quota / per
	used := d.Data.UsedQuota / per
	out := relayBalance{
		OK: true, Kind: "new-api-account", Currency: "USD",
		Total: &bal, Used: &used,
		Detail: fmt.Sprintf("账户余额 $%.2f（累计已用 $%.2f）", bal, used),
	}
	// 账号信息：面板要像龙虾账号卡片那样把这些显示出来
	out.Username = strings.TrimSpace(d.Data.DisplayName)
	if out.Username == "" {
		out.Username = strings.TrimSpace(d.Data.Username)
	}
	out.Email = strings.TrimSpace(d.Data.Email)
	out.Group = strings.TrimSpace(d.Data.Group)
	out.Phone = strings.TrimSpace(d.Data.Phone)
	out.UID = strings.TrimSpace(d.Data.UID)
	if out.UID == "" && d.Data.ID != 0 {
		out.UID = strconv.FormatInt(int64(d.Data.ID), 10)
	}
	if d.Data.RequestCount > 0 {
		out.Requests = int64(d.Data.RequestCount)
	}
	out.Role = int64(d.Data.Role)
	out.Status = int64(d.Data.Status)
	out.Created = int64(d.Data.CreatedTime)
	out.TwoFA = d.Data.TwoFA
	out.WeChat = d.Data.WeChatBound
	out.HasPwd = d.Data.HasPassword
	// 账号名优先用手机号（跟龙虾账号池那种 187****5260 的观感一致）
	name := out.Phone
	if name == "" {
		name = out.Username
	}
	if name != "" {
		out.Detail = "账号 " + name + " · " + out.Detail
	}
	return out, true
}

// httpGetJSONH 带自定义请求头的 GET，返回解析结果 + 原始 body + 状态码。
func httpGetJSONH(u string, hdr map[string]string) (map[string]any, string, int) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err.Error(), 0
	}
	for k, v := range hdr {
		if strings.TrimSpace(v) != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := balanceClient.Do(req)
	if err != nil {
		return nil, err.Error(), 0
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m, string(b), resp.StatusCode
}

// httpGetJSON 带 Bearer key 的 GET。
func httpGetJSON(u, key string) (map[string]any, string, int) {
	hdr := map[string]string{}
	if strings.TrimSpace(key) != "" {
		hdr["Authorization"] = "Bearer " + strings.TrimSpace(key)
	}
	return httpGetJSONH(u, hdr)
}

// loginCred 上游后台的登录态。面板里粘进来的东西各站形态不一样，这里统一解析：
//
//	① 控制台给的一整串 JSON：{"user":"<localStorage.user>","uid":"<localStorage.uid>","cookie":"<document.cookie>"}
//	   （自定义前端的站，如 tierflow.cn，就是这种：axios withCredentials + TF-User 头，没有 access_token）
//	② 直接的 token（eyJ... / sk-...）—— 标准 new-api 后台的 access_token
//	③ 直接的 Cookie 串（session=xxx;/yyy）—— 有些站只认 cookie
type loginCred struct {
	Token  string // 走 Authorization: Bearer
	Cookie string // 走 Cookie:
	UID    string // 走 TF-User / New-Api-User
}

func parseLoginCred(raw string) loginCred {
	raw = strings.TrimSpace(raw)
	out := loginCred{}
	if raw == "" {
		return out
	}
	if strings.HasPrefix(raw, "{") {
		var d struct {
			User   string `json:"user"`
			UID    string `json:"uid"`
			Token  string `json:"token"`
			Cookie string `json:"cookie"`
		}
		if json.Unmarshal([]byte(raw), &d) == nil {
			out.UID = strings.TrimSpace(d.UID)
			out.Cookie = strings.TrimSpace(d.Cookie)
			out.Token = strings.TrimSpace(d.Token)
			if strings.TrimSpace(d.User) != "" {
				var u map[string]any
				if json.Unmarshal([]byte(d.User), &u) == nil {
					for _, k := range []string{"access_token", "accessToken", "token"} {
						if v, ok := u[k].(string); ok && strings.TrimSpace(v) != "" {
							out.Token = strings.TrimSpace(v)
							break
						}
					}
					if out.UID == "" {
						if f, ok := jsonFloat(u["id"]); ok {
							out.UID = strconv.FormatInt(int64(f), 10)
						}
					}
				}
			}
			if out.Token != "" || out.Cookie != "" {
				return out
			}
		}
	}
	low := strings.ToLower(raw)
	// eyJ... / sk-... 一律当 token
	if strings.HasPrefix(raw, "eyJ") || strings.HasPrefix(low, "sk-") {
		out.Token = raw
		return out
	}
	// 带 = 且像 cookie 串的
	if strings.Contains(raw, "=") {
		out.Cookie = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(raw, "Cookie:"), "cookie:"))
		return out
	}
	out.Token = raw
	return out
}

// attempts 返回要依次尝试的请求头组合（cookie 优先——自定义前端的站多是这种）。
func (l loginCred) attempts() []map[string]string {
	var out []map[string]string
	if strings.TrimSpace(l.Cookie) != "" {
		h := map[string]string{"Cookie": strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(l.Cookie, "Cookie:"), "cookie:"))}
		if l.UID != "" {
			h["TF-User"] = l.UID
			h["New-Api-User"] = l.UID
		}
		out = append(out, h)
	}
	if strings.TrimSpace(l.Token) != "" {
		h := map[string]string{"Authorization": "Bearer " + strings.TrimSpace(l.Token)}
		if l.UID != "" {
			h["TF-User"] = l.UID
			h["New-Api-User"] = l.UID
		}
		out = append(out, h)
	}
	return out
}

func jsonFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func jsonStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func curSym(cur string) string {
	if strings.EqualFold(cur, "USD") {
		return "$"
	}
	return "¥"
}

// fetchChannelBalance 尽量从上游问出余额。顺序：
//
//	① DeepSeek 官方风格：GET {root}/user/balance → balance_infos[0].total_balance
//	② new-api / one-api 风格：GET {root}/v1/dashboard/billing/usage（已用，美分）
//	   ＋ GET {root}/v1/dashboard/billing/subscription（额度；>=1e6 视为该 key 无上限）
//	③ 都不行 → ok=false，detail 如实说明"这个上游不公开余额接口"
func fetchChannelBalance(ch *relay.Channel) relayBalance {
	out := relayBalance{}
	base := strings.TrimRight(strings.TrimSpace(ch.BaseURL), "/")
	root := base
	if strings.HasSuffix(root, "/v1") {
		root = strings.TrimSuffix(root, "/v1")
	}

	// ⓪ 有后台登录态就先走它 —— 能拿到真正的"账户余额"（其它路径只能估）
	if strings.TrimSpace(ch.LoginToken) != "" {
		if b, ok := fetchNewAPIAccountBalance(root, ch.LoginToken); ok {
			return b
		}
	}

	// ① DeepSeek 官方
	if m, raw, code := httpGetJSON(root+"/user/balance", ch.APIKey); code == 200 && m != nil {
		if infos, ok := m["balance_infos"].([]any); ok && len(infos) > 0 {
			if info, ok := infos[0].(map[string]any); ok {
				if tot, ok := jsonFloat(info["total_balance"]); ok {
					cur := strings.ToUpper(jsonStr(info["currency"]))
					if cur == "" {
						cur = "CNY"
					}
					out.OK = true
					out.Kind = "deepseek-official"
					out.Currency = cur
					out.Total = &tot
					top, _ := jsonFloat(info["topped_up_balance"])
					grant, _ := jsonFloat(info["granted_balance"])
					out.Detail = fmt.Sprintf("余额 %s%.2f（充值 %.2f / 赠送 %.2f）", curSym(cur), tot, top, grant)
					return out
				}
			}
		}
		_ = raw
	}

	// ② new-api / one-api
	usedM, rawUsed, codeUsed := httpGetJSON(root+"/v1/dashboard/billing/usage", ch.APIKey)
	if codeUsed == 200 && usedM != nil {
		if cents, ok := jsonFloat(usedM["total_usage"]); ok {
			usd := cents / 100
			out.OK = true
			out.Kind = "new-api"
			out.Currency = "USD"
			out.Used = &usd
			if subM, _, codeSub := httpGetJSON(root+"/v1/dashboard/billing/subscription", ch.APIKey); codeSub == 200 && subM != nil {
				if hard, ok := jsonFloat(subM["hard_limit_usd"]); ok {
					if hard >= 1e6 {
						out.Unlimited = true
						out.Detail = fmt.Sprintf("该 key 无额度上限 · 累计已用 $%.4f（账户余额要登录上游后台看）", usd)
					} else {
						rem := hard - usd
						out.Remaining = &rem
						out.Detail = fmt.Sprintf("额度 $%.2f · 已用 $%.4f · 剩余 $%.4f", hard, usd, rem)
					}
					return out
				}
			}
			out.Detail = fmt.Sprintf("累计已用 $%.4f（拿不到余额上限）", usd)
			return out
		}
		_ = rawUsed
	}

	out.Detail = "这个上游没有公开的余额接口（要登录它的后台才能看）"
	return out
}

// channelsLogin 给反代配「后台登录态」，用来读它的账户余额。
//
// body 三种用法：
//
//	{"id":"ch_x","token":"<access_token>"}          直接贴浏览器里拿的登录态（最稳）
//	{"id":"ch_x","username":"..","password":".."}  服务器替你登 new-api（有验证码的站会失败）
//	{"id":"ch_x","clear":true}                     清掉登录态
//
// **只有验证通过（/api/user/self 返回 success）才会保存**，避免存进去一个假 token 让余额卡一直报错。
func (h *Handler) channelsLogin(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var req struct {
		ID       string `json:"id"`
		Token    string `json:"token"`
		Username string `json:"username"`
		Password string `json:"password"`
		Clear    bool   `json:"clear"`
		Op       string `json:"op"`    // "" / add / remove / clear
		Index    int    `json:"index"` // remove 用
		Label    string `json:"label"` // 可选备注
		APIKey   string `json:"api_key"`
		On       *bool  `json:"on"` // freeze 用：true = 冻结，false = 解冻
	}
	if json.Unmarshal(body, &req) != nil || strings.TrimSpace(req.ID) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id required")
		return
	}
	ch := h.cfg.Relay.Get(req.ID)
	if ch == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "channel not found")
		return
	}
	dropBalanceCache := func() {
		balanceMu.Lock()
		// 一个渠道有很多个账号的缓存键（ch.ID#0、ch.ID#1、ch.ID#single）→ 前缀全清
		prefix := ch.ID + "#"
		for k := range balanceCache {
			if strings.HasPrefix(k, prefix) {
				delete(balanceCache, k)
				delete(balanceAt, k)
			}
		}
		delete(balanceCache, ch.ID)
		delete(balanceAt, ch.ID)
		balanceMu.Unlock()
	}
	// op: "" / "add" 加号（同一个号会覆盖旧的）；"remove" 删一个；"clear" 全清
	switch strings.ToLower(strings.TrimSpace(req.Op)) {
	case "remove":
		if req.Index < 0 || req.Index >= len(ch.Logins) {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "下标越界（这个号可能已经被删了）"})
			return
		}
		removed := ch.Logins[req.Index]
		ch.Logins = append(append([]relay.ChannelLogin(nil), ch.Logins[:req.Index]...), ch.Logins[req.Index+1:]...)
		ch.LoginToken = ""
		h.cfg.Relay.Replace(ch)
		dropBalanceCache()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "removed": removed.Phone + removed.Username, "left": len(ch.Logins),
			"detail": "已删除该后台账号（还剩 " + strconv.Itoa(len(ch.Logins)) + " 个）",
		})
		return
	case "clear":
		ch.Logins = nil
		ch.LoginToken = ""
		h.cfg.Relay.Replace(ch)
		dropBalanceCache()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "detail": "已清除该反代的后台登录态", "left": 0})
		return
	case "setkey":
		// 给某个后台账号配它自己的 sk- 令牌（配了就参与 A 模式轮询）
		if req.Index < 0 || req.Index >= len(ch.Logins) {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "下标越界（刷新页面再试）"})
			return
		}
		ch.Logins[req.Index].APIKey = strings.TrimSpace(req.APIKey)
		h.cfg.Relay.Replace(ch)
		dropBalanceCache()
		n := 0
		for _, l := range ch.Logins {
			if strings.TrimSpace(l.APIKey) != "" {
				n++
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "keys": n,
			"detail": func() string {
				if n == 0 {
					return "已清空该账号的令牌（转发回落渠道自己的 key）"
				}
				return "已配好 · 当前有 " + strconv.Itoa(n) + " 个令牌参与轮询（A 模式：请求轮流用）"
			}(),
		})
		return
	case "freeze":
		// 手工冻结/解冻某个后台账号（2026-09-22 用户要求：Tier 这类反代的**每个账号**都要能冻结/解冻）
		// 冻结的效果：这条不参与转发轮询（见 relay.Channel.AccountKeys），面板上标「已冻结」。
		// 跟「删」的区别：登录态、令牌、备注全留着，解冻就回来。
		if req.Index < 0 || req.Index >= len(ch.Logins) {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "下标越界（这个号可能已经被删了，刷新页面再试）"})
			return
		}
		on := req.On == nil || *req.On
		ch.Logins[req.Index].Frozen = on
		h.cfg.Relay.Replace(ch)
		n := 0
		for _, l := range ch.Logins {
			if strings.TrimSpace(l.APIKey) != "" && !l.Frozen {
				n++
			}
		}
		who := ch.Logins[req.Index].Phone
		if who == "" {
			who = ch.Logins[req.Index].Username
		}
		detail := "已冻结 " + who + "：不再参与转发（配置都留着，随时解冻）"
		if !on {
			detail = "已解冻 " + who + "：重新参与转发"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "frozen": on, "keys": n, "index": req.Index, "detail": detail,
		})
		return
	case "synckeys":
		// 一键同步：用每个后台账号的登录态，给它建/认出一把 sk-，自动填进那一行。
		// 规则：① 一个令牌都没有 → 替它新建一把（名字 blueapi-auto）；
		//      ② 已有令牌 → 列表是**打码**的，只有能认出"渠道自己那把 key"时才敢挂上；否则如实说让你手动贴。
		root := channelRoot(ch)
		created, matched, skipped := 0, 0, 0
		var notes []string
		for i := range ch.Logins {
			cred := parseLoginCred(ch.Logins[i].Token)
			if len(cred.attempts()) == 0 {
				skipped++
				continue
			}
			// 已经配好 key 的号不再动它（避免每点一次同步就往人家账号里多建一把令牌）
			if strings.TrimSpace(ch.Logins[i].APIKey) != "" {
				matched++
				continue
			}
			items, err := listNewAPITokens(root, cred)
			if err != nil {
				notes = append(notes, "第 "+strconv.Itoa(i+1)+" 个号：列表拿不到（"+err.Error()+"）")
				skipped++
				continue
			}
			// 先看看已有的令牌里能不能认出"渠道自己那把 key"（列表是打码的，只能靠前后缀认）
			plain := strings.TrimSpace(ch.APIKey)
			hit := ""
			if plain != "" {
				// 注意：new-api 列表里的 key 是**不带 sk- 前缀**的（打码后形如 `eUo1**********crNu`），
				// 而渠道里存的是带前缀的 `sk-eUo1...crNu` —— 两边都去掉前缀再比，否则永远认不出。
				norm := func(s string) string { return strings.TrimPrefix(strings.TrimSpace(s), "sk-") }
				for _, it := range items {
					k := strings.TrimSpace(it.Key)
					if k == plain || norm(k) == norm(plain) {
						hit = plain
						break
					}
					pre, suf := maskPrefixSuffix(k)
					if pre != "" && strings.HasPrefix(norm(plain), norm(pre)) && strings.HasSuffix(norm(plain), suf) {
						hit = plain
						break
					}
				}
			}
			if hit != "" {
				ch.Logins[i].APIKey = hit
				matched++
				continue
			}
			// 认不出（或者压根没有）→ 替它新建一把，这样才拿得到明文
			k, err := createNewAPIToken(root, cred, "blueapi-auto")
			if err != nil {
				notes = append(notes, "第 "+strconv.Itoa(i+1)+" 个号："+err.Error())
				skipped++
				continue
			}
			ch.Logins[i].APIKey = k
			created++
			if len(items) > 0 {
				notes = append(notes, "第 "+strconv.Itoa(i+1)+" 个号：原有 "+strconv.Itoa(len(items))+" 把令牌但列表打码读不到明文，已替它新建一把 blueapi-auto")
			}
		}
		h.cfg.Relay.Replace(ch)
		dropBalanceCache()
		n := len(ch.AccountKeys())
		detail := "自动同步完成：新建 " + strconv.Itoa(created) + " 把 · 认出已有 " + strconv.Itoa(matched) + " 把"
		if skipped > 0 {
			detail += " · 跳过 " + strconv.Itoa(skipped) + " 个"
		}
		detail += "　→ 当前有 " + strconv.Itoa(n) + " 个令牌参与轮询"
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "created": created, "matched": matched, "skipped": skipped,
			"keys": n, "notes": notes, "detail": detail,
		})
		return
	}
	base := strings.TrimRight(strings.TrimSpace(ch.BaseURL), "/")
	root := base
	if strings.HasSuffix(root, "/v1") {
		root = strings.TrimSuffix(root, "/v1")
	}
	token := strings.TrimSpace(req.Token)
	if token == "" {
		if strings.TrimSpace(req.Username) == "" || req.Password == "" {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "token 或 username+password 必填")
			return
		}
		lbody, _ := json.Marshal(map[string]string{"username": req.Username, "password": req.Password})
		resp, err := balanceClient.Post(root+"/api/user/login", "application/json", strings.NewReader(string(lbody)))
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "连不上上游登录接口: " + err.Error()})
			return
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var lr struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
			Data    struct {
				AccessToken string `json:"access_token"`
			} `json:"data"`
		}
		_ = json.Unmarshal(raw, &lr)
		if !lr.Success || strings.TrimSpace(lr.Data.AccessToken) == "" {
			msg := strings.TrimSpace(lr.Message)
			if msg == "" {
				msg = "上游没返回 access_token"
			}
			writeJSON(w, http.StatusOK, map[string]any{"ok": false,
				"error": "用户名/密码登录被拒：" + msg + "（这个站开了验证码的话直连登不进去，改用粘贴 access_token）"})
			return
		}
		token = strings.TrimSpace(lr.Data.AccessToken)
	}
	// 先验证再保存
	b, ok := fetchNewAPIAccountBalance(root, token)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false,
			"error": "登录态验证失败（/api/user/self 不通过）：token 可能不对/过期，或这个上游不是 new-api 系、没有这个接口"})
		return
	}
	// 验证通过 → 存成这个反代下的一个「后台账号」。
	// 判重：同一个号（uid / 手机号 / 用户名任一相同）再登一次就**覆盖**旧凭据，不堆重复行；
	// 不同号则**追加** —— 这就是"一个反代能同时挂很多账号"的关键。
	entry := relay.ChannelLogin{
		Token: strings.TrimSpace(token), Label: strings.TrimSpace(req.Label),
		Phone: b.Phone, UID: b.UID, Username: b.Username, AddedAt: time.Now(),
	}
	replaced := false
	for i := range ch.Logins {
		old := ch.Logins[i]
		same := (b.UID != "" && old.UID == b.UID) ||
			(b.Phone != "" && old.Phone == b.Phone) ||
			(b.Username != "" && old.Username == b.Username)
		if same {
			ch.Logins[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		ch.Logins = append(ch.Logins, entry)
	}
	ch.LoginToken = "" // 老字段不再使用（已迁移进 Logins）
	h.cfg.Relay.Replace(ch)
	dropBalanceCache()
	balanceMu.Lock()
	delete(balanceCache, ch.ID)
	delete(balanceAt, ch.ID)
	balanceMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "balance": b, "id": ch.ID, "name": ch.Name,
		"replaced": replaced, "count": len(ch.Logins),
		"detail": func() string {
			if replaced {
				return "已更新该后台账号（共 " + strconv.Itoa(len(ch.Logins)) + " 个）"
			}
			return "已新增一个后台账号（共 " + strconv.Itoa(len(ch.Logins)) + " 个）"
		}(),
	})
}

// channelsBalance 面板用：GET /channels/balance?id=xxx
func (h *Handler) channelsBalance(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Relay == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "relay_disabled", "relay store not configured")
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "id required")
		return
	}
	ch := h.cfg.Relay.Get(id)
	if ch == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "channel not found")
		return
	}
	force := r.URL.Query().Get("force") == "1"
	// 一个反代下可能挂了很多个后台账号 → 逐个读，一个账号一块信息
	logins := channelLogins(ch)
	if len(logins) == 0 {
		// 没配后台登录态 → 回落原来的探测顺序（能不能读到余额看上游）
		key := ch.ID + "#single"
		balanceMu.Lock()
		b, hit := balanceCache[key]
		stale := !hit || time.Since(balanceAt[key]) >= balanceTTL
		balanceMu.Unlock()
		if force || stale {
			b = fetchChannelBalance(ch)
			balanceMu.Lock()
			balanceCache[key] = b
			balanceAt[key] = time.Now()
			balanceMu.Unlock()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id": id, "name": ch.Name, "accounts": []any{}, "balance": b, "count": 0,
		})
		return
	}
	root := channelRoot(ch)
	accounts := make([]map[string]any, 0, len(logins))
	cachedAny := false
	var first relayBalance
	needSave := false
	for i, l := range logins {
		key := ch.ID + "#" + strconv.Itoa(i)
		b, c := cachedChannelBalanceCached(ch.ID, root, key, l, force)
		if !b.OK {
			b.Detail = "登录态可能已失效或被踢（重新用 relay-login 抓一次登录态即可）"
		}
		// 回填身份快照：老版本迁移过来的 entry 没有 uid/phone，
		// 回填之后"同一个号再登一次"才会被识别成覆盖，而不是又新增一条。
		if b.OK && i < len(ch.Logins) &&
			(ch.Logins[i].UID != b.UID || ch.Logins[i].Phone != b.Phone || ch.Logins[i].Username != b.Username) &&
			(b.UID != "" || b.Phone != "" || b.Username != "") {
			ch.Logins[i].UID, ch.Logins[i].Phone, ch.Logins[i].Username = b.UID, b.Phone, b.Username
			needSave = true
		}
		cachedAny = cachedAny || c
		if i == 0 {
			first = b
		}
		accounts = append(accounts, map[string]any{
			"index": i, "label": l.Label, "ok": b.OK, "balance": b,
			"phone": b.Phone, "username": b.Username, "uid": b.UID,
			"has_key": strings.TrimSpace(l.APIKey) != "", "key_mask": maskKey(l.APIKey),
			"frozen": l.Frozen,
		})
	}
	if needSave {
		h.cfg.Relay.Replace(ch)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "name": ch.Name, "accounts": accounts, "balance": first,
		"count": len(accounts), "cached": cachedAny,
	})
}

// channelRoot 渠道 BaseURL 去掉结尾 /v1（不同家的后台路径不一样，这里统一拼根）
func channelRoot(ch *relay.Channel) string {
	root := strings.TrimRight(strings.TrimSpace(ch.BaseURL), "/")
	if strings.HasSuffix(root, "/v1") {
		root = strings.TrimSuffix(root, "/v1")
	}
	return root
}

// channelLogins 取这个渠道的后台登录态列表（含老字段 login_token 的兼容）
func channelLogins(ch *relay.Channel) []relay.ChannelLogin {
	if ch == nil {
		return nil
	}
	if len(ch.Logins) > 0 {
		return ch.Logins
	}
	if strings.TrimSpace(ch.LoginToken) != "" {
		return []relay.ChannelLogin{{Token: ch.LoginToken}}
	}
	return nil
}

// cachedChannelBalanceCached 单个后台账号的余额读取（60 秒缓存），额外告诉调用方这次是不是命中缓存
func cachedChannelBalanceCached(chID, root, key string, l relay.ChannelLogin, force bool) (relayBalance, bool) {
	balanceMu.Lock()
	if !force {
		if b, ok := balanceCache[chID+"#"+key]; ok && time.Since(balanceAt[chID+"#"+key]) < balanceTTL {
			balanceMu.Unlock()
			return b, true
		}
	}
	balanceMu.Unlock()
	b, ok := fetchNewAPIAccountBalance(root, l.Token)
	if !ok {
		b = relayBalance{OK: false, Detail: "读不到账号信息"}
	}
	balanceMu.Lock()
	balanceCache[chID+"#"+key] = b
	balanceAt[chID+"#"+key] = time.Now()
	balanceMu.Unlock()
	return b, false
}

// poolKnowsModel 龙虾账号池的模型表里有没有这个名字。
//
// **大小写敏感**（2026-09-22 实测踩坑）：龙虾上游对模型名大小写是敏感的，
// 池子表里是 `glm-5.3-flash`，客户端发 `GLM-5.3-Flash`（Tier 声明的写法）时，
// 以前用 EqualFold 判定"池子认识" → 请求被池子抢走 → 上游认不出 → 回一个**空 200**，
// 用户看到的就是"有回复但是空的"。改成精确匹配后，这种名字会直接交给声明它的反代。
// 表拉不到（动态接口失败/无健康账号）时返回 true —— 判不了就别乱改路由行为。
func (h *Handler) poolKnowsModel(model string) bool {
	m := strings.TrimSpace(model)
	if m == "" {
		return true
	}
	ids := h.fetchDynamicModels()
	if len(ids) == 0 {
		return true
	}
	for _, id := range ids {
		if strings.TrimSpace(id) == m {
			return true
		}
	}
	return false
}

// poolModels 只返回龙虾账号池自己的模型（不带任何反代接管的）。
// 静态表兜底：动态拉取失败时用 staticModels，保证面板「模型测试」里
// 「龙虾账号池」那一组永远不至于空掉。
func (h *Handler) poolModels(w http.ResponseWriter, r *http.Request) {
	ids := h.fetchDynamicModels()
	out := make([]map[string]any, 0, len(ids))
	if len(ids) > 0 {
		for _, id := range ids {
			out = append(out, map[string]any{
				"id": id, "object": "model", "created": 1753600000,
				"owned_by": "lobsterai", "context_length": 131072,
			})
		}
	} else {
		out = append(out, staticModels...)
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": out})
}

// modelList 动态获取模型 ID 列表并包装成 OpenAI 格式。
func (h *Handler) modelList() []map[string]any {
	var out []map[string]any
	if ids := h.fetchDynamicModels(); len(ids) > 0 {
		out = make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			out = append(out, map[string]any{
				"id":             id,
				"object":         "model",
				"created":        1753600000,
				"owned_by":       "lobsterai",
				"context_length": 131072,
			})
		}
	} else {
		out = append(out, staticModels...)
	}
	// 追加反代渠道接管的模型（owned_by = 渠道名），方便客户端下拉直选
	seen := map[string]bool{}
	poolIDs := map[string]bool{}
	for _, m := range out {
		if id, ok := m["id"].(string); ok {
			seen[id] = true
			poolIDs[id] = true
		}
	}
	if h.cfg.Relay != nil {
		for _, c := range h.cfg.Relay.List() {
			if !c.Enabled {
				continue
			}
			for _, mid := range channelModelList(c) {
				mid = strings.TrimSpace(mid)
				if mid == "" || mid == "*" || seen[mid] {
					continue
				}
				seen[mid] = true
				out = append(out, map[string]any{
					"id":             mid,
					"object":         "model",
					"created":        1753600000,
					"owned_by":       c.Name,
					"context_length": 131072,
				})
			}
		}
		// 前缀变体：`渠道名/模型` = 强制走该渠道
		for _, c := range h.cfg.Relay.List() {
			if !c.Enabled {
				continue
			}
			for _, mid := range channelModelList(c) {
				mid = strings.TrimSpace(mid)
				if mid == "" || mid == "*" {
					continue
				}
				full := c.Name + "/" + mid
				if seen[full] {
					continue
				}
				seen[full] = true
				out = append(out, map[string]any{
					"id":             full,
					"object":         "model",
					"created":        1753600000,
					"owned_by":       c.Name + "(指定渠道)",
					"context_length": 131072,
				})
			}
		}
		// 被反代接管的模型再补一个 `lobster/模型`，用来强制走龙虾账号池
		base := make([]string, 0, len(out))
		for _, m := range out {
			if id, ok := m["id"].(string); ok && id != "" && !strings.Contains(id, "/") {
				base = append(base, id)
			}
		}
		for _, id := range base {
			// 只有「龙虾池本来就有、但被渠道接管了」的模型才需要 lobster/ 逃生口
			if !poolIDs[id] || h.cfg.Relay.Match(id) == nil {
				continue
			}
			alt := "lobster/" + id
			if seen[alt] {
				continue
			}
			seen[alt] = true
			out = append(out, map[string]any{
				"id":             alt,
				"object":         "model",
				"created":        1753600000,
				"owned_by":       "lobsterai(指定龙虾池)",
				"context_length": 131072,
			})
		}
	}
	return out
}

// fetchDynamicModels 从池中任一健康账号拉模型列表，缓存 1h。
func (h *Handler) fetchDynamicModels() []string {
	dynamicModelsCache.RLock()
	if len(dynamicModelsCache.ids) > 0 && time.Since(dynamicModelsCache.fetched) < dynamicModelsTTL {
		ids := dynamicModelsCache.ids
		dynamicModelsCache.RUnlock()
		return ids
	}
	dynamicModelsCache.RUnlock()

	// 缓存过期：挑一个健康账号拉
	acct := h.cfg.Pool.Pick()
	if acct == nil {
		return nil
	}
	ids, err := h.cfg.Upstream.FetchModels(acct)
	if err != nil || len(ids) == 0 {
		return nil
	}
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = ids
	dynamicModelsCache.fetched = time.Now()
	dynamicModelsCache.Unlock()
	return ids
}

// ---------------------------------------------------------------------------
// 上游指定（模型名前缀 / 请求头 X-Lobster-Channel）
// ---------------------------------------------------------------------------

// isPoolAlias 是否是「龙虾账号池」的别名。
func isPoolAlias(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "lobster", "lobsterai", "pool", "龙虾", "账号池", "龙虾池", "默认":
		return true
	}
	return false
}

// resolveTarget 解析目标："" = 未指定，"lobster" = 账号池，其他 = 渠道 ID。
func (h *Handler) resolveTarget(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if isPoolAlias(name) {
		return "lobster"
	}
	if h.cfg.Relay != nil {
		if ch := h.cfg.Relay.Find(name); ch != nil {
			return ch.ID
		}
	}
	return ""
}

// splitRoute 解析模型名里的上游前缀，如 `优质/kimi-k3`、`lobster/deepseek-v4-pro`。
// 前缀必须是已知渠道名或池别名，否则原样返回（保住 meta-llama/Llama-3 这类带斜杠的模型名）。
func (h *Handler) splitRoute(model string) (target string, realModel string) {
	i := strings.Index(model, "/")
	if i <= 0 || i == len(model)-1 {
		return "", model
	}
	if t := h.resolveTarget(model[:i]); t != "" {
		return t, model[i+1:]
	}
	return "", model
}

// rewriteModel 把请求体里的 model 字段换成真实模型名。
func rewriteModel(body []byte, model string) []byte {
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return body
	}
	m["model"] = model
	b, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return b
}

func (h *Handler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	var peek struct {
		Stream bool   `json:"stream"`
		Model  string `json:"model"`
	}
	_ = json.Unmarshal(body, &peek)
	// 先建记账对象再校验：参数错的请求也要能在面板上看到（那是"这个 key 在干什么"的一部分）
	// 「这个 token 在干什么」：整条请求的记账（模型 / 走了哪条上游 / token 数 / 成败）。
	// 用 defer 收尾，保证每条 return 路径都会落一条记录（成功、失败、参数错都算）。
	keyRec := KeyCall{At: time.Now(), Stream: peek.Stream, OK: true}
	keyUsed := keyFromCtx(r.Context())
	// 调用流水用的几个局部量（在 noteUse / 账号池循环里填）
	var (
		tokIn, tokCached, tokOut, tokReason int64
		poolAcct                            string
	)
	// 第一条 user 消息预览 → 面板「记录」表的"消息"列
	firstMsg := ""
	{
		var m struct {
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if json.Unmarshal(body, &m) == nil {
			for _, one := range m.Messages {
				if !strings.EqualFold(strings.TrimSpace(one.Role), "user") {
					continue
				}
				var s string
				if json.Unmarshal(one.Content, &s) == nil {
					firstMsg = s
				} else {
					firstMsg = string(one.Content) // 多模态数组：原样留一点，够辨认就行
				}
				break
			}
		}
		firstMsg = briefN(firstMsg, 80)
	}
	defer func() {
		if keyUsed == "" {
			return
		}
		if keyRec.Model == "" {
			keyRec.Model = peek.Model
		}
		h.noteKeyCall(keyUsed, keyRec)
		// 落一条调用流水（面板「记录」那张表的数据源）
		h.calls.Append(CallLogEntry{
			At: keyRec.At, Key: keyUsed, Model: keyRec.Model, Src: keyRec.Source,
			Acct: poolAcct, Msg: firstMsg, In: tokIn, Cached: tokCached, Out: tokOut,
			Reason: tokReason, Total: keyRec.Tokens, Stream: keyRec.Stream,
			OK: keyRec.OK, Note: keyRec.Note, MS: time.Since(keyRec.At).Milliseconds(),
		})
	}()
	// 失败就记一笔（note 只留第一条原因，免得被后续覆盖成"最终话术"）
	markFail := func(note string) {
		if keyRec.OK {
			keyRec.OK = false
			keyRec.Note = brief(note)
		}
	}
	// 成功路径都会调它：既记全局用量，也把"哪条上游 / 多少 token"记进这条 key 的明细
	noteUse := func(src string, usage map[string]any) {
		if src != "" {
			keyRec.Source = src
		}
		if usage != nil {
			num := func(k string) int64 {
				if v, ok := usage[k].(float64); ok {
					return int64(v)
				}
				return 0
			}
			tokIn, tokOut = num("prompt_tokens"), num("completion_tokens")
			// 缓存命中：龙虾用 prompt_cache_hit_tokens，标准 OpenAI 用 prompt_tokens_details.cached_tokens
			tokCached = num("prompt_cache_hit_tokens")
			if d, ok := usage["prompt_tokens_details"].(map[string]any); ok {
				if v, ok := d["cached_tokens"].(float64); ok {
					tokCached = int64(v)
				}
			}
			if d, ok := usage["completion_tokens_details"].(map[string]any); ok {
				if v, ok := d["reasoning_tokens"].(float64); ok {
					tokReason = int64(v)
				}
			}
			keyRec.Tokens = num("total_tokens")
			if keyRec.Tokens == 0 {
				keyRec.Tokens = tokIn + tokOut
			}
		}
		h.noteUsageFrom(src, usage)
	}
	// 入口形态校验（2026-09-22 整体体检发现）：
	// 以前 body 不是 JSON / messages 为空时，我们照原样转给上游 → 上游 4xx/5xx →
	// 最后被报成 503 "all accounts unavailable (cooling/disabled)"，
	// 客户端以为"线路全挂了"，其实是**请求本身有问题**被甩锅给了账号。
	if !json.Valid(body) {
		markFail("请求体不是合法 JSON")
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "请求体不是合法 JSON")
		return
	}
	var shape struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &shape); err == nil && len(shape.Messages) == 0 {
		markFail("messages 不能为空")
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "messages 不能为空")
		return
	}
	// 入站记录：把来源 IP / UA / 模型 / body 长度打进日志，用于分辨"同一服务不同来源"的行为差异
	if os.Getenv("LB2A_DEBUG_RAW") != "" {
		log.Printf("[inbound] remote=%s ua=%q model=%s stream=%v len=%d", r.RemoteAddr, r.Header.Get("User-Agent"), peek.Model, peek.Stream, len(body))
	}

	var timeout time.Duration
	if h.cfg.Upstream != nil && h.cfg.Upstream.HTTP != nil {
		timeout = h.cfg.Upstream.HTTP.Timeout
	}

	// 上游指定：模型名前缀（优质/xxx、lobster/xxx）优先，其次请求头 X-Lobster-Channel
	// explicit = 客户端自己点名的上游，必须照办（哪怕它停用/不认这个模型，也让它自己去报错）
	explicit := false
	target, realModel := h.splitRoute(peek.Model)
	if target != "" {
		explicit = true
	}
	if hdr := strings.TrimSpace(r.Header.Get("X-Lobster-Channel")); hdr != "" {
		if t := h.resolveTarget(hdr); t != "" {
			target = t
			explicit = true
		}
	}
	// 都没指定 → 用面板里选的默认上游（空 = 按接管规则自动选）
	if target == "" && h.cfg.Relay != nil {
		target = h.cfg.Relay.Default()
	}
	if realModel != peek.Model {
		body = rewriteModel(body, realModel)
	}
	if strings.TrimSpace(realModel) != "" {
		keyRec.Model = realModel // 上游实际收到的模型名（前缀已剥掉）
	}

	// ① 显式指定了某个反代渠道 → 直连该渠道，失败仍回退账号池
	failedChannel := ""
	fellBack := false
	// 已经尝试过的渠道（去重：回退兜底别把同一个渠道反复打）
	triedIDs := map[string]bool{}
	var attempted []string // 尝试顺序，报错时直接告诉用户"到底试过谁"
	tryChannel := func(ch *relay.Channel, label string, isFallback bool) bool {
		triedIDs[ch.ID] = true
		attempted = append(attempted, ch.Name+"("+ch.ID+")")
		// 渠道模型名映射：客户端叫法 → 上游认的名字（没配映射则原样透传）
		fwdBody := body
		if mapped := ch.MapModel(realModel); mapped != realModel {
			fwdBody = rewriteModel(body, mapped)
			log.Printf("[relay] %s(%s) 模型名映射: %s → %s", ch.Name, ch.ID, realModel, mapped)
		}
		// A 模式：这条渠道下配了多把令牌时，**一把额度用完就换下一把**（用户要求：用完自动换号）
		keys := ch.AccountKeys()
		if len(keys) == 0 {
			keys = []string{""} // 空 → ForwardWithKey 用渠道自己那个 key
		}
		var usage map[string]any
		var ferr error
		for ki, k := range keys {
			usage, ferr = relay.ForwardWithKey(ch, k, fwdBody, w, peek.Stream, timeout)
			if ferr == nil {
				break
			}
			// 换号的两个理由：① 这把额度用完；② 这把被上游限速（429 tpm/rpm，按账号算）。
			// 限速要小小歇一下再换，否则 3 把令牌会在同一秒内一起撞墙。
			quota, rate := isQuotaErr(ferr), isRateLimitErr(ferr)
			if ki < len(keys)-1 && (quota || rate) {
				if rate && !quota {
					time.Sleep(350 * time.Millisecond)
				}
				log.Printf("[relay] %s(%s) 第 %d 把令牌%s → 自动换下一把", ch.Name, ch.ID, ki+1,
					map[bool]string{true: "额度用完", false: "被限速(429)"}[quota])
				continue
			}
			break
		}
		if ferr == nil {
			h.noteRelayOK()
			if isFallback {
				h.noteFallbackTo(ch.ID)
			}
			noteUse(ch.ID, usage)
			return true
		}
		h.noteRelayFail(realModel, ferr)
		failedChannel = ch.ID
		fellBack = true
		log.Printf("[relay] %s(%s) %s失败: %v", ch.Name, ch.ID, label, ferr)
		markFail(label + "失败: " + ch.Name + " " + ferr.Error())
		return false
	}
	if target != "" && target != "lobster" && h.cfg.Relay != nil {
		if ch := h.cfg.Relay.Get(target); ch != nil {
			// 面板默认上游只是"优先"，不是"强制"：它停用了、或压根不认这个模型
			// （如默认设成 ds，客户端却发 glm-5.3）→ 别硬塞过去吃 404，
			// 直接落回下面的接管规则 / 账号池。客户端显式点名的渠道不在此列。
			switch {
			case explicit:
				if tryChannel(ch, "指定渠道转发", false) {
					return
				}
			case !ch.Enabled:
				log.Printf("[relay] 默认上游 %s(%s) 已停用 → 改走接管规则", ch.Name, ch.ID)
			case !ch.Serves(realModel):
				log.Printf("[relay] 默认上游 %s(%s) 不接管模型 %s → 改走接管规则", ch.Name, ch.ID, realModel)
			default:
				if tryChannel(ch, "指定渠道转发", false) {
					return
				}
			}
		}
	}

	// ③ 模型名账号池不认识，但有反代明确声明接管它 → 直接交给那个反代。
	//
	// 实测背景（2026-09-22）：默认上游 = 龙虾账号池时，客户端发 `tierflow`（只有 Tier 反代认得这个名字），
	// 账号池会回一个"空 200"（content 空、连 usage 都没有）而且**被判为成功** →
	// 永远走不到声明接管它的反代，用户看到的就是"我加的反代不生效"。
	// 判据只用"池子自己的模型表里没有这个名字"：池子认识的名字仍然完全按优先级表走，
	// 所以拖动排序的语义一点没变（账号池认识的模型照旧可能排在反代前面）。
	if (target == "" || target == "lobster") && h.cfg.Relay != nil && strings.TrimSpace(realModel) != "" {
		if !h.poolKnowsModel(realModel) {
			if ch := h.cfg.Relay.MatchExplicit(realModel); ch != nil && !triedIDs[ch.ID] {
				log.Printf("[relay] 账号池模型表里没有 %q → 直接交给声明接管它的反代 %s(%s)",
					realModel, ch.Name, ch.ID)
				if tryChannel(ch, "反代接管（池不识）", false) {
					return
				}
			}
		}
	}

	// ② 没指定（或指定了账号池 = 不走渠道）→ 按优先级顺序挑上游。
	// 顺序表里含 "lobster"：轮到它就落账号池 —— 这是「拖动优先级」的核心，账号池也能排前面。
	if target != "lobster" && h.cfg.Relay != nil {
		switch pick := h.cfg.Relay.Pick(realModel); pick {
		case "", "lobster":
			// 没有渠道接管这个模型，或优先级把账号池排在前面 → 落账号池
		default:
			if !triedIDs[pick] {
				if ch := h.cfg.Relay.Get(pick); ch != nil {
					if tryChannel(ch, "接管转发", false) {
						return
					}
				}
			}
		}
	}

	// 反代失败 → 按设置的回退目标处理
	if fellBack && h.cfg.Relay != nil {
		// 兜底：按回退目标没救活时，依次试其余启用中的渠道（跳过已失败的那个）。
		// 为什么需要（2026-09-21 实测）：面板默认把 fallback_target 设成跟 default 同一个渠道，
		// 一旦它挂了就走 default 分支的 `fb != failedChannel` 判断 → 直接 502，
		// 明明池里还有别的渠道可用却不用，用户视角就是"失败回退没效果"。
		tryOtherChannels := func() bool {
			for _, ch := range h.cfg.Relay.List() {
				if !ch.Enabled || triedIDs[ch.ID] {
					continue
				}
				if tryChannel(ch, "回退兜底", true) {
					return true
				}
			}
			return false
		}
		switch fb := h.cfg.Relay.Fallback(); fb {
		case "":
			writeOpenAIError(w, http.StatusBadGateway, "relay_error",
				"反代失败，已关闭回退（不消耗龙虾积分）: "+h.routeLastErr())
			return
		case "auto":
			matched := false
			if ch := h.cfg.Relay.Match(realModel); ch != nil && !triedIDs[ch.ID] {
				if tryChannel(ch, "回退自动匹配", true) {
					return
				}
				matched = true
			}
			// Match 命中但失败了、或压根没匹配上 → 兜底扫其余渠道
			if matched || h.cfg.Relay.MatchCount() > 0 {
				if tryOtherChannels() {
					return
				}
			}
		case "lobster":
			// 落到龙虾账号池
		default:
			// 回退目标 == 刚失败的那个（面板常见配法）时 triedIDs 里已经有它 → 直接扫其余渠道
			if !triedIDs[fb] {
				if ch := h.cfg.Relay.Get(fb); ch != nil {
					if tryChannel(ch, "回退指定渠道", true) {
						return
					}
				}
			}
			if tryOtherChannels() {
				return
			}
			writeOpenAIError(w, http.StatusBadGateway, "relay_error",
				"反代与其回退渠道都失败了（已试 "+strings.Join(attempted, " → ")+"）: "+h.routeLastErr())
			return
		}
	}
	h.notePool(fellBack)

	var lastErr error
	// 上游抖动容错：一轮账号轮换全失败后，若失败原因在"上游侧"（5xx / 传输错误），
	// 短暂退避后整轮重试，把几十秒的抽风窗口消化在网关内，而不是直接甩 503 给客户端。
	const poolRetryRounds = 3
	for round := 0; round < poolRetryRounds; round++ {
		if round > 0 {
			backoff := time.Duration(round) * 900 * time.Millisecond
			log.Printf("[pool] 上游抖动，退避 %v 后重试（第 %d/%d 轮）lastErr=%v",
				backoff, round+1, poolRetryRounds, lastErr)
			select {
			case <-time.After(backoff):
			case <-r.Context().Done():
				return
			}
		}
		tried := map[string]bool{}
		// 上游 5xx 是"网关/模型后端"层的故障，跟具体账号无关，
		// 连续撞到 2 个就说明整条上游在抽风，继续换号纯属浪费时间 → 直接进下一次退避重试
		serverErrs := 0
	rotateLoop:
		for i := 0; i < h.cfg.MaxRotate; i++ {
			acct := h.cfg.Pool.PickExcluding(tried)
			if acct == nil {
				break
			}
			tried[acct.UID] = true
			poolAcct = acct.UID // 记进调用流水（面板「记录」里能看出这单用的哪个号）
			defer h.cfg.Pool.Release(acct.UID)

			// token 临近过期 → 先 refresh（失败冷却换号）
			if acct.NeedsRefresh(h.cfg.RefreshSkew) {
				if err := h.cfg.Upstream.RefreshToken(acct); err != nil {
					lastErr = err
					var ue *upstream.Error
					if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
						h.cfg.Pool.Disable(acct.UID, "refresh session dead")
					} else if errors.As(err, &ue) && ue.Status >= 500 {
						// 上游 5xx 导致刷新失败 → 账号未必坏，只记全局故障
						h.noteUpstreamFault("refresh", err.Error())
					} else {
						// 传输层失败 / 未知错误：不直接判账号死刑，先换号；
						// 连续 err_threshold 次才冷却（阈值已放宽到 5，冷却降到 3m）。
						h.cfg.Pool.NoteErrorMsg(acct.UID, h.cfg.ErrThreshold, h.cfg.ErrCooldown, "refresh: "+err.Error())
					}
					continue
				}
				_ = acct.SaveAtomic()
			}

			rc, status, terr := h.cfg.Upstream.ChatStream(acct, body)
			if terr != nil {
				// 上游把错误塞在 200 流的第一块里（HTTP 状态码是 200！）。
				// 最典型：`模型不可见或无访问权限` (40301) —— 实测同一个模型同一时刻，
				// 有的号能出内容、有的号回这个错，说明**权限是按账号算的**。
				// 正确动作是换下一个号重试；既不判上游整体故障，也不罚这个号（它对别的模型是好的）。
				var he *upstream.HeadError
				if errors.As(terr, &he) {
					lastErr = &upstream.Error{Kind: upstream.ErrClient, Status: he.Status, Msg: string(he.Body)}
					if upstream.IsQuotaText(string(he.Body)) {
						// 顺带：流首就是"额度用完"的，也给这个号上硬冷却
						h.cfg.Pool.Cooldown(acct.UID, pool.CoolHard, h.cfg.HardCooldown, "流首额度用完")
					}
					log.Printf("[pool] uid=%s 流首报错（换下一个号试）: %s", acct.UID, brief(string(he.Body)))
					// 模型名不存在这类"请求本身的问题"，换号也没用 → 立刻停，别白打剩下几个号
					if isModelMissingErr(he.Body) {
						log.Printf("[pool] 上游明确说这个模型不支持（换号无意义）→ 停止换号")
						break rotateLoop
					}
					continue
				}
				lastErr = terr
				// 传输层故障（超时/连接被重置/连不上）= 网络与上游的问题，
				// 不是这个账号的错 → 只记全局故障窗口，绝不累计账号 errCount。
				h.noteUpstreamFault("transport", terr.Error())
				log.Printf("[pool] uid=%s transport error (account NOT penalized): %v", acct.UID, terr)
				continue
			}
			if status >= 400 {
				kind := upstream.Classify(status, string(h.cfg.Upstream.LastBody))
				switch kind {
				case upstream.ErrHardCredit:
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolHard, h.cfg.HardCooldown, "余额不足")
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					continue
				case upstream.ErrSoftRate:
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolSoft, h.cfg.SoftCooldown, "429 rate limit")
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					continue
				case upstream.ErrSessionDead:
					h.cfg.Pool.Disable(acct.UID, "session dead")
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					continue
				case upstream.ErrBanned:
					// 40302 = 账号被有道封禁，终止性的，重试无意义。
					// 旧版本归 ErrClient（不惩罚账号）→ 死号每次都被选中，
					// 实测 40 号里 38 个被封后直接报 no_healthy_account。
					// 这里直接禁用，后续选号碰都不碰。
					h.cfg.Pool.Disable(acct.UID, "账号已被有道封禁 (40302)")
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					log.Printf("[pool] uid=%s 封号 → 已禁用: %s", acct.UID, brief(string(h.cfg.Upstream.LastBody)))
					continue
				case upstream.ErrNotFound:
					// 404 短冷却不累计 errCount（防雪崩）
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolSoft, h.cfg.SoftCooldown, "upstream 404")
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					continue
				case upstream.ErrServer:
					// 5xx = 有道网关/模型后端故障，账号本身没问题 →
					// 只记全局故障窗口，换号继续，不冷却账号。
					// （历史坑：这里原本记 errCount，上游抽风 3 次就把整池刷冷却）
					h.noteUpstreamFault(fmt.Sprintf("http %d", status), string(h.cfg.Upstream.LastBody))
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					log.Printf("[pool] uid=%s upstream %d (account NOT penalized): %s",
						acct.UID, status, brief(string(h.cfg.Upstream.LastBody)))
					serverErrs++
					if serverErrs >= 2 {
						log.Printf("[pool] 连续 %d 个账号均 5xx → 判定上游整体故障，停止换号，转入退避重试", serverErrs)
						break rotateLoop
					}
					continue
				default:
					// 其余 4xx（400/403/422…）：同一个 body 打哪个账号都会被拒，
					// 属于「请求形态错误」，不是账号的错 → 换号但不惩罚账号。
					h.noteRequestFault(fmt.Sprintf("http %d", status), string(h.cfg.Upstream.LastBody))
					lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(h.cfg.Upstream.LastBody)}
					log.Printf("[pool] uid=%s request rejected %d (account NOT penalized): %s",
						acct.UID, status, brief(string(h.cfg.Upstream.LastBody)))
					if isModelMissingErr(h.cfg.Upstream.LastBody) {
						log.Printf("[pool] 上游 HTTP %d 明确说这个模型不支持（换号无意义）→ 停止换号", status)
						break rotateLoop
					}
					continue
				}
			}
			defer rc.Close()
			h.cfg.Pool.NoteSuccess(acct.UID)
			if peek.Stream {
				usage, quotaSeen, _ := upstream.Stream(w, rc)
				noteUse("lobster", usage)
				h.noteAccountUsage(acct.UID, usage)
				// 流中途上游说"额度用完"：字节已经吐给客户端了，本单没法重试，
				// 但**必须立刻把这个号冷却掉** —— 否则下一单还会选中它，
				// 用户看到的就是"正在重新连接 / 5"反复出现（2026-09-22 实测）。
				if quotaSeen {
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolHard, h.cfg.HardCooldown, "流中途额度用完")
					log.Printf("[pool] uid=%s 流中途额度用完 → 已硬冷却（本单不可重试，下一单不再选它）", acct.UID)
				}
				return
			}
			resp, err := upstream.Aggregate(rc)
			if err != nil {
				markFail("上游返回无法解析: " + err.Error())
				writeOpenAIError(w, http.StatusBadGateway, "upstream_parse", err.Error())
				return
			}
			// 池子回了个「空 200」（模型名它其实认不出/这个号没权限）→ 如果某条反代明确声明了这个模型，改走它。
			// 2026-09-22 实测：`glm-5.3-flash` 在池子模型表里，但上游回空 200，
			// 客户端看到"有回复但是空的"。注意：这里还没往客户端写过任何字节，所以可以安全改道。
			if relay.IsEmptyCompletion(resp) && h.cfg.Relay != nil {
				if ch := h.cfg.Relay.MatchExplicit(realModel); ch != nil && !triedIDs[ch.ID] {
					log.Printf("[pool] 池子回空内容（模型 %s）→ 改走声明它的反代 %s(%s)", realModel, ch.Name, ch.ID)
					if tryChannel(ch, "池子空回退", true) {
						return
					}
				}
			}
			if u, ok := resp["usage"].(map[string]any); ok {
				noteUse("lobster", u)
				h.noteAccountUsage(acct.UID, u)
			}
			writeJSON(w, http.StatusOK, resp)
			return
		}
		// 只有"上游侧"错误才值得退避重试；请求形态错误(4xx)重试无意义，直接收尾
		var ue *upstream.Error
		if lastErr != nil && errors.As(lastErr, &ue) && ue.Kind == upstream.ErrClient {
			break
		}
		if lastErr == nil {
			break
		}
	}
	// ③ 龙虾池空了（全封号 / 全冷却 / 全 0 分）→ 自动改走反代渠道（双向互通）。
	// 用户硬规则：一边没号了另一边顶上，Token 不受影响。
	// 只在池里真的无号可选时触发（避免把正常流量偷到反代）。
	if h.cfg.Pool != nil && !h.cfg.Pool.HasPickable() && h.cfg.Relay != nil {
		poolEmpty := lastErr
		// 池空改道也要去重：同一个渠道别因为"匹配一次 + 兜底扫一轮"被打两遍
		poolTried := map[string]bool{}
		if failedChannel != "" {
			poolTried[failedChannel] = true
		}
		fallbackToRelay := func(ch *relay.Channel) bool {
			poolTried[ch.ID] = true
			fwdBody := body
			if mapped := ch.MapModel(realModel); mapped != realModel {
				fwdBody = rewriteModel(body, mapped)
				log.Printf("[pool-empty] %s(%s) 模型名映射: %s → %s", ch.Name, ch.ID, realModel, mapped)
			}
			usage, ferr := relay.Forward(ch, fwdBody, w, peek.Stream, timeout)
			if ferr != nil {
				h.noteRelayFail(realModel, ferr)
				log.Printf("[pool-empty] 回退渠道 %s(%s) 失败: %v", ch.Name, ch.ID, ferr)
				return false
			}
			h.noteRelayOK()
			h.noteFallbackTo(ch.ID)
			noteUse(ch.ID, usage)
			log.Printf("[pool-empty] 龙虾池无号 → 已改走渠道 %s(%s)", ch.Name, ch.ID)
			return true
		}
		// 优先用面板里设的回退目标；没设就按模型匹配
		fb := h.cfg.Relay.Fallback()
		switch {
		case fb == "lobster" || fb == "":
			// 回退目标就是龙虾池（但池已空）→ 退而按模型匹配反代
			if ch := h.cfg.Relay.Match(realModel); ch != nil {
				if fallbackToRelay(ch) {
					return
				}
			}
		case fb == "auto":
			if ch := h.cfg.Relay.Match(realModel); ch != nil {
				if fallbackToRelay(ch) {
					return
				}
			}
		default:
			if ch := h.cfg.Relay.Get(fb); ch != nil && ch.Enabled {
				if fallbackToRelay(ch) {
					return
				}
			}
		}
		// 匹配不到 / 失败 → 依次试其余启用中的渠道
		for _, ch := range h.cfg.Relay.List() {
			if !ch.Enabled || poolTried[ch.ID] {
				continue
			}
			if fallbackToRelay(ch) {
				return
			}
		}
		if poolEmpty != nil {
			lastErr = poolEmpty
		}
	}

	// 归因（2026-09-22 体检修）：最后一次失败如果是"请求本身被上游拒了"
	// （模型名不存在 / 参数不对），那就不是"没号可用" —— 别再报 503，
	// 客户端会以为"线路全挂了"，然后无脑重试。
	var ueFinal *upstream.Error
	if errors.As(lastErr, &ueFinal) && (ueFinal.Kind == upstream.ErrClient || ueFinal.Kind == upstream.ErrNotFound) {
		code, status := "upstream_rejected", http.StatusBadRequest
		if isModelMissingErr([]byte(ueFinal.Msg)) {
			code, status = "model_not_found", http.StatusNotFound
		}
		markFail("上游拒绝: " + ueFinal.Msg)
		writeOpenAIError(w, status, code, "上游拒绝了这个请求（与账号无关）: "+ueFinal.Msg)
		return
	}
	msg := "all accounts unavailable (cooling/disabled)"
	if lastErr != nil {
		msg += ": " + lastErr.Error()
	}
	if lastErr != nil {
		markFail(lastErr.Error())
	} else {
		markFail("没有可用账号（全冷却/禁用）")
	}
	writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", msg)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// brief 截断长文本（用于把上游错误记到账号上）
func brief(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " "))
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// briefN 把空白压成单空格并截到 n 个字符（面板「记录」里的"消息"列用；按 rune 截，别切坏中文）
func briefN(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if n <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	// 统一兜一层：绝不把上游的推广/额度文案原样透传（用户要求"不要显示这个"）
	msg = sanitizeUpstreamMsg(msg)
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    "api_error",
			"code":    code,
		},
	})
}
