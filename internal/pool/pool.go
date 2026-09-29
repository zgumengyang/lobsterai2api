// Package pool 账号池：内存索引 + 冷却/禁用状态机 + state.json 持久化。
// 挑选策略：先按 inflight 最小（并发均衡），同档内按 UID 轮询（串行流量不会只压一个号）。
package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"lobsterai2api/internal/auth"
)

// CoolKind 冷却类型。
type CoolKind int

const (
	CoolHard CoolKind = iota // 余额不足 → 长冷却
	CoolSoft                 // 429 → 短冷却
	CoolErr                  // 连续错误 → 中冷却
)

func (k CoolKind) String() string {
	switch k {
	case CoolHard:
		return "hard_credit"
	case CoolSoft:
		return "soft_rate"
	case CoolErr:
		return "error_threshold"
	}
	return "unknown"
}

// Status 单个账号对外暴露的状态（脱敏）。
type Status struct {
	UID       string    `json:"uid"`
	Nickname  string    `json:"nickname,omitempty"`
	Credits   int64     `json:"credits"`
	Cooling   bool      `json:"cooling"`
	Until     time.Time `json:"until,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	Disabled  bool      `json:"disabled"`
	ErrCount  int       `json:"err_count,omitempty"`
	LastErr   string    `json:"last_err,omitempty"`
	LastErrAt time.Time `json:"last_err_at,omitempty"`
	Inflight  int       `json:"inflight,omitempty"`
	// 今日用量（面板按号显示；跨天自动清零，落盘 data/account-use.json）
	TodayRequests int64 `json:"today_requests,omitempty"`
	TodayTokens   int64 `json:"today_tokens,omitempty"`
	TodayBurn     int64 `json:"today_burn,omitempty"`
	// AddedAt 这个账号的「时间」——优先用账号自己的**首次登录时间**
	// （auth 文件里的 firstKeyfrom，登录时就写死了，跨重启不变），
	// 拿不到才退回池子的首次记录（data/pool-added.json）。
	// 2026-09-25 用户要求：面板「龙虾账号池」按账号的时间先后排。
	AddedAt time.Time `json:"added_at,omitempty"`
	// 邀请进度（2026-09-26 用户要求：每个号"被邀请/已邀请"几人要看得见）。
	// 口径 = 上游 GET /api/invitation/progress 的 invitedCount（该账号已邀请几人），
	// 由调度器在刷新余额时顺带拉取。
	InvitedCount     int     `json:"invited_count,omitempty"`
	InviteCode       string  `json:"invite_code,omitempty"`
	InviteStage1     int     `json:"invite_stage1,omitempty"`      // 一档门槛（人数）
	InviteStage1Done bool    `json:"invite_stage1_done,omitempty"` // 一档是否达成
	InviteReward     float64 `json:"invite_reward,omitempty"`      // 邀请累计奖励积分
}

// UseStat 某账号"当天"的用量。
// Burn 是"今日消耗的积分"：每次刷新余额时，比上次低的那部分累加进来
// （余额上升=签到/奖励，不算消耗）。
type UseStat struct {
	Day      string `json:"day"`
	Requests int64  `json:"requests"`
	Tokens   int64  `json:"tokens"`
	Burn     int64  `json:"burn"`
}

type entry struct {
	a            *auth.Auth
	credits      int64
	disabled     bool
	reason       string
	until        time.Time
	errCount     int
	lastErr      string
	lastErrAt    time.Time
	inflight     int
	addedAt      time.Time // 首次被池子看到的时刻（= 添加时间，落盘 data/pool-added.json）
	invited      int       // 邀请进度（上游 invitedCount）
	inviteCode   string    // 该账号的邀请码
	inviteS1     int       // 一档门槛人数
	inviteS1Done bool      // 一档是否达成
	inviteReward float64   // 邀请累计奖励积分
}

func (e *entry) healthy(now time.Time) bool {
	if e.disabled {
		return false
	}
	if !e.until.IsZero() && now.Before(e.until) {
		return false
	}
	return true
}

// stateFile 持久化格式。
type stateFile struct {
	Accounts map[string]struct {
		Credits   int64     `json:"credits"`
		Disabled  bool      `json:"disabled"`
		Reason    string    `json:"reason,omitempty"`
		Until     time.Time `json:"until,omitempty"`
		LastErr   string    `json:"last_err,omitempty"`
		LastErrAt time.Time `json:"last_err_at,omitempty"`
	} `json:"accounts"`
}

// Pool 账号池。
type Pool struct {
	mu          sync.RWMutex
	byUID       map[string]*entry
	stateFp     string
	eventsFp    string
	useFp       string // 按账号的今日用量（data/account-use.json）
	use         map[string]*UseStat
	addedFp     string // 账号添加时间（data/pool-added.json）：uid -> 首次进池时刻
	added       map[string]time.Time
	lastUseSave time.Time // 上次把用量写盘的时间（节流用）
}

// Event 一次账号状态变更事件（冷却 / 解冻 / 禁用 / 恢复），落 events.jsonl 便于事后复盘。
type Event struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	UID    string    `json:"uid,omitempty"`
	Reason string    `json:"reason,omitempty"`
}

// eventsMaxBytes 事件日志轮转阈值（超过后旧文件改名 .old）。
const eventsMaxBytes = 2 << 20

// New 构建池；stateFp 非空时尝试加载旧状态。
func New(stateFp string) *Pool {
	p := &Pool{byUID: map[string]*entry{}, stateFp: stateFp, use: map[string]*UseStat{}}
	p.added = map[string]time.Time{}
	if stateFp != "" {
		p.eventsFp = filepath.Join(filepath.Dir(stateFp), "events.jsonl")
		p.useFp = filepath.Join(filepath.Dir(stateFp), "account-use.json")
		p.addedFp = filepath.Join(filepath.Dir(stateFp), "pool-added.json")
		p.load()
		p.loadUse()
		p.loadAdded()
	}
	return p
}

// logEventLocked 追加一条事件（调用方需持有 p.mu）。
func (p *Pool) logEventLocked(kind, uid, reason string) {
	if p.eventsFp == "" {
		return
	}
	if fi, err := os.Stat(p.eventsFp); err == nil && fi.Size() > eventsMaxBytes {
		_ = os.Rename(p.eventsFp, p.eventsFp+".old")
	}
	b, err := json.Marshal(Event{At: time.Now(), Kind: kind, UID: uid, Reason: reason})
	if err != nil {
		return
	}
	f, err := os.OpenFile(p.eventsFp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

// RecentEvents 返回最近 n 条事件（按时间先后，新的在后）。
func (p *Pool) RecentEvents(n int) []Event {
	if p.eventsFp == "" || n <= 0 {
		return nil
	}
	raw, err := os.ReadFile(p.eventsFp)
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\r\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	out := make([]Event, 0, len(lines))
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		var e Event
		if json.Unmarshal([]byte(ln), &e) == nil {
			out = append(out, e)
		}
	}
	return out
}

// Add 加入账号；已存在则保留原状态、更新凭证。
func (p *Pool) Add(a *auth.Auth) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.noteAddedLocked(a.UID) // 先记"第一次见到"，再决定是不是新建 entry
	if e, ok := p.byUID[a.UID]; ok {
		e.a = a // 保留 credits/cooling 状态
		return
	}
	p.byUID[a.UID] = &entry{a: a, addedAt: p.added[a.UID]}
}

// SyncToDir 用最新扫描结果对齐池：新账号加入、消失的账号剔除（状态保留）。
func (p *Pool) SyncToDir(auths []*auth.Auth) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	for _, a := range auths {
		seen[a.UID] = true
		p.noteAddedLocked(a.UID) // 第一次见到 = 你刚把它加进来，记下时刻
		if e, ok := p.byUID[a.UID]; ok {
			e.a = a
		} else {
			p.byUID[a.UID] = &entry{a: a, addedAt: p.added[a.UID]}
		}
	}
	for uid := range p.byUID {
		if !seen[uid] {
			delete(p.byUID, uid)
		}
	}
	p.pruneAddedLocked(seen) // 删掉的账号不再留添加时间：以后再加回来算新的一次
}

// Count 返回池中账号总数（只读，用于热重载前后比对，判断是否需要刷新配额）。
func (p *Pool) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byUID)
}

// Pick 返回"按账号顺序"的第一个可用账号；无可用返回 nil。
// 注意：不占并发位（仅用于探活/测试），调用方无需 Release。
func (p *Pool) Pick() *auth.Auth {
	return p.pick(nil, false)
}

// PickExcluding 同上，但跳过 tried 中的 uid（请求级轮换），并占用一个并发位
// （调用方请求结束时必须 Release）。
//
// 选号规则（2026-09-26 用户要求改成"按账号顺序用完再切"）：
//  1. 只从 healthy 且积分 > 0 的账号里选；池里有正分账号时 0 分账号永不参与。
//  2. 顺序 = 面板里看到的顺序 = 账号首次登录时间（auth.FirstKeyfrom）升序，
//     同一时刻（或拿不到时间）按 uid 兜底。
//  3. 取顺序上第一个可用号，一直用它；等它 0 分（上游确认无积分 → 硬冷却 12h）
//     或被限流/报错（软冷却）→ 下一单自动落到顺序上的下一个号。
//
// 历史（别再改回去）：2026-09-21 曾经为了"治频繁冷却"改成 UID 轮询 + inflight 均衡，
// 当时 40 号池串行流量全压一个号、被限流。现在用户明确要"按顺序把一个号的积分用完
// 再切下一个"，所以顺序优先、不再按 inflight 摊并发（并发请求会落在同一个号上，
// 这是预期行为）。被限流时靠软冷却自动跳号兜底。
// 想切回轮询：把下面的 orderLessLocked 换成 uid 轮询 + rr 游标即可（代码在 git 历史里）。
func (p *Pool) PickExcluding(tried map[string]bool) *auth.Auth {
	return p.pick(tried, true)
}

func (p *Pool) pick(tried map[string]bool, hold bool) *auth.Auth {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	// 只要池子里存在 healthy 且积分 >0 的账号，0 积分账号永不参与选号
	// （0 积分 = 上游余额不足，选它必然失败，纯属浪费轮换次数）
	hasPositive := false
	for _, e := range p.byUID {
		if tried != nil && tried[e.a.UID] {
			continue
		}
		if e.healthy(now) && e.credits > 0 {
			hasPositive = true
			break
		}
	}
	// 收集所有可选号（不按 inflight 分档：并发请求就该落在同一个号上，这是"顺序用"的语义）
	var cands []*entry
	for uid, e := range p.byUID {
		if tried != nil && tried[uid] {
			continue
		}
		if !e.healthy(now) {
			continue
		}
		if hasPositive && e.credits <= 0 {
			continue
		}
		cands = append(cands, e)
	}
	if len(cands) == 0 {
		return nil
	}
	// 按「账号顺序」排序，取第一个 —— 用完这个号的积分再切下一个
	sort.Slice(cands, func(i, j int) bool { return p.orderLessLocked(cands[i], cands[j]) })
	best := cands[0]
	if hold {
		best.inflight++
	}
	return best.a
}

// orderKeyLocked 返回账号的"顺序时间"：优先首次登录时间（auth.FirstKeyfrom），
// 拿不到才退回池子首次记录（data/pool-added.json）。零值 = 没有时间信息。
// 与面板显示的顺序同一口径（panel 排序用的就是这个 added_at）。
func (p *Pool) orderKeyLocked(e *entry) time.Time {
	if ms, err := strconv.ParseInt(strings.TrimSpace(e.a.FirstKeyfrom), 10, 64); err == nil && ms > 0 {
		return time.UnixMilli(ms)
	}
	return p.added[e.a.UID]
}

// orderLessLocked 按"账号顺序"比较：时间早的在前；没时间的排最后；同一时刻按 uid。
func (p *Pool) orderLessLocked(x, y *entry) bool {
	tx, ty := p.orderKeyLocked(x), p.orderKeyLocked(y)
	if !tx.Equal(ty) {
		if tx.IsZero() {
			return false // 没时间的排后面
		}
		if ty.IsZero() {
			return true
		}
		return tx.Before(ty)
	}
	return x.a.UID < y.a.UID
}

// HasPickable 报告当前是否还有可选账号（只读，不占并发位、不动轮询游标）。
// 供 handler 判断“龙虾池是否已经空了” —— 空了就改走反代渠道（双向互通），
// 避免整个网关只能扔 503。
// 判定口径与 pick() 一致：有正分号就只算正分号，否则算健康号。
func (p *Pool) HasPickable() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	hasPositive := false
	for _, e := range p.byUID {
		if e.healthy(now) && e.credits > 0 {
			hasPositive = true
			break
		}
	}
	for _, e := range p.byUID {
		if !e.healthy(now) {
			continue
		}
		if hasPositive && e.credits <= 0 {
			continue
		}
		return true
	}
	return false
}

// Release 请求结束后释放一次并发占位（与 PickExcluding 配对，用 defer 调用）。
func (p *Pool) Release(uid string) {
	p.mu.Lock()
	if e, ok := p.byUID[uid]; ok && e.inflight > 0 {
		e.inflight--
	}
	p.mu.Unlock()
}

// SetCredits 更新账号余额。
func (p *Pool) SetCredits(uid string, credits int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		p.accBurnLocked(uid, e.credits, credits)
		e.credits = credits
	}
	p.saveLocked()
}

// SetInvite 记录某账号的邀请进度（面板显示用，由调度器刷新余额时顺带拉取）。
// 只放在内存里、不落盘：它是上游的展示数据，重启后下一次刷新（≤10 分钟）就会回来。
func (p *Pool) SetInvite(uid string, count int, code string, stage1 int, stage1Done bool, reward float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.invited = count
	e.inviteCode = code
	e.inviteS1 = stage1
	e.inviteS1Done = stage1Done
	e.inviteReward = reward
}

// Cooldown 冷却账号至 now+d。
func (p *Pool) Cooldown(uid string, kind CoolKind, d time.Duration, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.until = time.Now().Add(d)
		e.reason = reason
		e.errCount = 0
		e.lastErr = reason
		e.lastErrAt = time.Now()
		p.logEventLocked("cooldown:"+kind.String(), uid, reason)
	}
	p.saveLocked()
}

// Disable 永久禁用（session 死亡），需人工重登后手工恢复或文件替换。
func (p *Pool) Disable(uid, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.disabled = true
		e.reason = reason
		e.lastErr = reason
		e.lastErrAt = time.Now()
		p.logEventLocked("disable", uid, reason)
	}
	p.saveLocked()
}

// ReenableIfCredits 签到后解冻：仅当 remain > 0 且账号处于冷却（非禁用）时恢复。
func (p *Pool) ReenableIfCredits(uid string, remain int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		p.accBurnLocked(uid, e.credits, remain)
		e.credits = remain
		if remain > 0 && !e.disabled {
			if !e.until.IsZero() {
				p.logEventLocked("reenable", uid, "quota>0")
			}
			e.until = time.Time{}
			e.reason = ""
			e.errCount = 0
		}
	}
	p.saveLocked()
}

// NoteError 记录一次非余额/非 429 错误；达到 threshold 自动冷却 d 时长。
func (p *Pool) NoteError(uid string, threshold int, d time.Duration) {
	p.NoteErrorMsg(uid, threshold, d, "")
}

// NoteErrorMsg 同 NoteError，但额外记录具体错误原因（便于排查频繁冷却）。
func (p *Pool) NoteErrorMsg(uid string, threshold int, d time.Duration, why string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.errCount++
		if why != "" {
			e.lastErr = why
			e.lastErrAt = time.Now()
		}
		if e.errCount >= threshold {
			e.until = time.Now().Add(d)
			e.reason = "consecutive errors"
			e.errCount = 0
			p.logEventLocked("cooldown:error_threshold", uid, why)
		}
	}
	p.saveLocked()
}

// NoteSuccess 成功请求重置错误计数。
func (p *Pool) NoteSuccess(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.errCount = 0
	}
}

// ClearOne 解除单个账号的冷却/禁用/错误计数。
func (p *Pool) ClearOne(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.until = time.Time{}
	e.errCount = 0
	e.disabled = false
	e.reason = ""
	p.logEventLocked("clear_one", uid, "")
	p.saveLocked()
	return true
}

// ClearCooldown 清除所有账号的冷却/禁用/错误计数，返回被解冻的账号数。
func (p *Pool) ClearCooldown() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.byUID {
		if !e.until.IsZero() || e.disabled || e.errCount > 0 {
			n++
		}
		e.until = time.Time{}
		e.errCount = 0
		e.disabled = false
		e.reason = ""
	}
	if n > 0 {
		p.logEventLocked("clear_all", "", "")
		p.saveLocked()
	}
	return n
}

// AuthByUID 返回账号的完整凭证（给调度器/运维接口用）。
func (p *Pool) AuthByUID(uid string) *auth.Auth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if e, ok := p.byUID[uid]; ok {
		return e.a
	}
	return nil
}

// List 返回所有账号状态（按 UID 排序，稳定输出）。
func (p *Pool) List() []Status {
	p.mu.RLock()
	defer p.mu.RUnlock()
	uids := make([]string, 0, len(p.byUID))
	for uid := range p.byUID {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	out := make([]Status, 0, len(uids))
	for _, uid := range uids {
		out = append(out, p.statusOf(uid, p.byUID[uid]))
	}
	return out
}

func (p *Pool) statusOf(uid string, e *entry) Status {
	now := time.Now()
	st := Status{
		UID:       uid,
		Nickname:  e.a.Nickname,
		Credits:   e.credits,
		Cooling:   !e.until.IsZero() && now.Before(e.until),
		Until:     e.until,
		Reason:    e.reason,
		Disabled:  e.disabled,
		ErrCount:  e.errCount,
		LastErr:   e.lastErr,
		LastErrAt: e.lastErrAt,
		Inflight:  e.inflight,
	}
	// 「账号的时间」= 首次登录时间（auth.FirstKeyfrom），拿不到退回池子首次记录。
	// 跟 pick() 的顺序口径同源（orderKeyLocked）—— 面板看到的顺序 = 选号顺序。
	if t := p.orderKeyLocked(e); !t.IsZero() {
		st.AddedAt = t
	}
	// 今日用量（只读，别在这儿建新条目 —— 调用方持的是读锁）
	if u := p.use[uid]; u != nil && u.Day == todayKey() {
		st.TodayRequests, st.TodayTokens, st.TodayBurn = u.Requests, u.Tokens, u.Burn
	}
	// 邀请进度（面板显示：该号已邀请几人 + 邀请码 + 一档门槛）
	st.InvitedCount = e.invited
	st.InviteCode = e.inviteCode
	st.InviteStage1 = e.inviteS1
	st.InviteStage1Done = e.inviteS1Done
	st.InviteReward = e.inviteReward
	return st
}

// ---------------------------------------------------------------------------
// 持久化
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 按账号记账（今日用量：单数 / token / 消耗积分）
// ---------------------------------------------------------------------------

func todayKey() string { return time.Now().Format("2006-01-02") }

// useForLocked 取（或新建）某账号当天的计费行；跨天自动清零。调用方需持锁。
func (p *Pool) useForLocked(uid string) *UseStat {
	if p.use == nil {
		p.use = map[string]*UseStat{}
	}
	u := p.use[uid]
	if day := todayKey(); u == nil || u.Day != day {
		u = &UseStat{Day: day}
		p.use[uid] = u
	}
	return u
}

// NoteRequest 记一次成功请求：单数 +1、token 累加。
// 面板账号卡片上的「今日 N 单 · X token」就是从这儿来的。
func (p *Pool) NoteRequest(uid string, tokens int64) {
	if uid == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	u := p.useForLocked(uid)
	u.Requests++
	if tokens > 0 {
		u.Tokens += tokens
	}
	p.saveUseLocked(false)
}

// accBurnLocked 余额下降的部分记成"今日消耗"（余额上升=签到/奖励，不算）。调用方需持锁。
func (p *Pool) accBurnLocked(uid string, oldCredits, newCredits int64) {
	if uid == "" || newCredits >= oldCredits {
		return
	}
	p.useForLocked(uid).Burn += oldCredits - newCredits
	// 积分变化不频繁（10 分钟一轮刷新 + 签到），这里强制落盘，别被节流吃掉
	p.saveUseLocked(true)
}

// UseSnapshot 返回某天的按账号用量（day 为空 = 今天）。
func (p *Pool) UseSnapshot(day string) map[string]*UseStat {
	if day == "" {
		day = todayKey()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := map[string]*UseStat{}
	for uid, u := range p.use {
		if u.Day != day {
			continue
		}
		cp := *u
		out[uid] = &cp
	}
	return out
}

func (p *Pool) loadUse() {
	if p.useFp == "" {
		return
	}
	raw, err := os.ReadFile(p.useFp)
	if err != nil {
		return
	}
	var d struct {
		Use map[string]*UseStat `json:"use"`
	}
	if json.Unmarshal(raw, &d) != nil || d.Use == nil {
		return
	}
	p.mu.Lock()
	p.use = d.Use
	p.mu.Unlock()
}

// saveUseLocked 落盘（调用方需持锁）。
// force=false 时节流（最多 2 秒写一次）—— 请求很密，不能每单都写文件；
// 积分/签到这种低频但重要的变化传 force=true，立即落盘。
func (p *Pool) saveUseLocked(force bool) {
	if p.useFp == "" {
		return
	}
	if !force && time.Since(p.lastUseSave) < 2*time.Second {
		return
	}
	p.lastUseSave = time.Now()
	day := todayKey()
	cur := map[string]*UseStat{}
	for uid, u := range p.use {
		if u.Day == day {
			cur[uid] = u
		}
	}
	b, err := json.MarshalIndent(map[string]any{"use": cur, "saved_at": time.Now()}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(p.useFp, b, 0o644)
}

// ---------------------------------------------------------------------------
// 添加时间（data/pool-added.json）：uid -> 第一次进池的时刻
// 用途：面板「账号池」按用户添加的先后排（2026-09-25 用户要求）。
// ---------------------------------------------------------------------------

func (p *Pool) loadAdded() {
	if p.addedFp == "" {
		return
	}
	raw, err := os.ReadFile(p.addedFp)
	if err != nil {
		return
	}
	m := map[string]time.Time{}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	for uid, t := range m {
		p.added[uid] = t
		if e, ok := p.byUID[uid]; ok {
			e.addedAt = t
		}
	}
}

// noteAddedLocked 第一次见到这个 uid 时记下时刻（调用方持锁）。
func (p *Pool) noteAddedLocked(uid string) {
	if uid == "" {
		return
	}
	if _, ok := p.added[uid]; ok {
		return
	}
	p.added[uid] = time.Now()
	if e, ok := p.byUID[uid]; ok {
		e.addedAt = p.added[uid]
	}
	p.saveAddedLocked()
}

func (p *Pool) saveAddedLocked() {
	if p.addedFp == "" {
		return
	}
	b, err := json.MarshalIndent(p.added, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(p.addedFp, b, 0o600)
}

// pruneAddedLocked 丢掉已经不在池子里的记录（调用方持锁）。
func (p *Pool) pruneAddedLocked(seen map[string]bool) {
	changed := false
	for uid := range p.added {
		if !seen[uid] {
			delete(p.added, uid)
			changed = true
		}
	}
	if changed {
		p.saveAddedLocked()
	}
}

func (p *Pool) load() {
	raw, err := os.ReadFile(p.stateFp)
	if err != nil {
		return
	}
	var sf stateFile
	if json.Unmarshal(raw, &sf) != nil {
		return
	}
	for uid, s := range sf.Accounts {
		p.byUID[uid] = &entry{
			a:         &auth.Auth{UID: uid}, // placeholder，Add 时会换成完整凭证
			credits:   s.Credits,
			disabled:  s.Disabled,
			reason:    s.Reason,
			until:     s.Until,
			lastErr:   s.LastErr,
			lastErrAt: s.LastErrAt,
		}
	}
}

func (p *Pool) saveLocked() {
	if p.stateFp == "" {
		return
	}
	sf := stateFile{Accounts: map[string]struct {
		Credits   int64     `json:"credits"`
		Disabled  bool      `json:"disabled"`
		Reason    string    `json:"reason,omitempty"`
		Until     time.Time `json:"until,omitempty"`
		LastErr   string    `json:"last_err,omitempty"`
		LastErrAt time.Time `json:"last_err_at,omitempty"`
	}{}}
	for uid, e := range p.byUID {
		sf.Accounts[uid] = struct {
			Credits   int64     `json:"credits"`
			Disabled  bool      `json:"disabled"`
			Reason    string    `json:"reason,omitempty"`
			Until     time.Time `json:"until,omitempty"`
			LastErr   string    `json:"last_err,omitempty"`
			LastErrAt time.Time `json:"last_err_at,omitempty"`
		}{
			Credits:   e.credits,
			Disabled:  e.disabled,
			Reason:    e.reason,
			Until:     e.until,
			LastErr:   e.lastErr,
			LastErrAt: e.lastErrAt,
		}
	}
	raw, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return
	}
	if dir := filepath.Dir(p.stateFp); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := p.stateFp + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, p.stateFp)
}
