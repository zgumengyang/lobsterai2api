// Package relay 管理外部 OpenAI 兼容「反代」渠道：自定义地址 + Token，按模型名接管。
// 命中渠道模型名的请求直接转发到该反代；未命中的继续走龙虾账号池。
package relay

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lobsterai2api/internal/upstream"
)

// ChannelLogin 反代后台的一个登录态（一条 = 面板上的一行账号）。
type ChannelLogin struct {
	// Token 原始凭据：可能是控制台打包的 JSON（user/uid/cookie）、裸 token 或 Cookie 串。
	// 解析见 server.parseLoginCred。
	Token string `json:"token"`
	// Label 面板上给这个号起的备注（可选）
	Label string `json:"label,omitempty"`
	// Phone / UID / Username 加进来时抓到的身份快照 —— 用来判重（同号再次登录就覆盖，不会堆两条）
	Phone    string `json:"phone,omitempty"`
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty"`
	// APIKey 这个后台账号自己的 sk- 令牌。**转发用**：一个渠道下配了多个，
	// 就按请求轮流用（A 模式，见 Channel.PickAPIKey）。
	// 留空则该条不参与轮询（只用来看余额）。
	APIKey string `json:"api_key,omitempty"`
	// Frozen 手工冻结（2026-09-22 用户要求：每个后台账号都能单独冻结/解冻）。
	// 冻结的号：① 不参与转发轮询（见 AccountKeys）② 面板上标「已冻结」
	// —— 跟"删掉"的区别是配置全留着，随时解冻就回来。
	Frozen bool `json:"frozen,omitempty"`
	// AddedAt 加进来的时间
	AddedAt time.Time `json:"added_at,omitempty"`
}

// Channel 一个外部反代渠道。
type Channel struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	BaseURL string   `json:"base_url"`
	APIKey  string   `json:"api_key"`
	Models  []string `json:"models"` // 接管的模型名；["*"] = 全部接管
	// SeenModels 最近一次连通测试实测到的模型名（由 SetSeen 写入）。
	// 不参与路由判定，只解决「面板加的反代在模型测试里看不到」的问题：
	// ① 模型测试下拉按这个把反代列出来；② 支持一键把实测模型写成接管。
	SeenModels []string `json:"seen_models,omitempty"`
	// LoginToken 上游后台的登录态（老字段，单账号时代留下的）。
	// 现在用 Logins 存多个账号；启动加载时会把 LoginToken 迁移成 Logins 的第一条。
	LoginToken string `json:"login_token,omitempty"`
	// Logins 这个反代后台**多个账号**的登录态。
	// 为什么要多个（2026-09-22 用户实测）：一个反代后台常挂好几个号，
	// 以前只有单个 login_token，登第二个就把第一个冲掉了 —— 面板只能看到一个账号。
	// 只用来读账号信息/余额（/api/user/self），**不参与转发鉴权**（转发永远用 APIKey）。
	Logins []ChannelLogin `json:"logins,omitempty"`
	// ModelMap 模型名映射：客户端请求的 model 名 → 该渠道上游真正认的名字。
	// 为什么需要（2026-09-21 实测）：龙虾池的叫法和各反代的叫法常常对不上
	// （如客户端发 deepseek-flash，ds 渠道只认 deepseek-v4-flash）→ 上游回 404
	// "Model not supported"，用户看到的就是"反代没效果"。
	// 配了映射就自动翻译，客户端不用改。
	ModelMap map[string]string `json:"model_map,omitempty"`
	Enabled  bool              `json:"enabled"`
	Note     string            `json:"note,omitempty"`
	// Priority 优先级：数字小的先试（0 = 未设置，新建时自动排到末尾）。
	// 用在两处：① 多个渠道都接管同一个模型时 Match 选谁；② 回退兜底 / 池空改道扫渠道的顺序。
	Priority  int       `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
	OK        bool      `json:"ok"`        // 最近一次连通测试结果
	LastTest  string    `json:"last_test"` // 最近一次测试描述
	TestedAt  time.Time `json:"tested_at"`
}

// Store 渠道存储：JSON 文件 + 进程内缓存。
type Store struct {
	mu    sync.RWMutex
	file  string
	items []*Channel
	def   string // 默认上游："" = 自动（按接管规则），"lobster" = 账号池，其他 = 渠道 ID
	fb    string // 反代失败时的回退目标："" = 不回退，"lobster" = 账号池，"auto" = 自动匹配其它渠道，其他 = 渠道 ID
	// order 路由顺序（权威）：里面装的渠道 ID 和 "lobster"（账号池的位置）。
	// Pick() 就是从上往下扫这个列表 —— 轮到谁先用谁，龙虾池排第几就第几个被用。
	order []string
}

// NewStore 打开（或创建）渠道存储；file 为空 = 仅内存。
func NewStore(file string) *Store {
	s := &Store{file: file, fb: "lobster"}
	s.load()
	return s
}

func (s *Store) load() {
	if s.file == "" {
		return
	}
	raw, err := os.ReadFile(s.file)
	if err != nil {
		return
	}
	var d struct {
		Channels        []*Channel `json:"channels"`
		DefaultUpstream string     `json:"default_upstream"`
		Fallback        *bool      `json:"fallback"`
		FallbackTarget  string     `json:"fallback_target"`
		Order           []string   `json:"order"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	s.items = d.Channels
	// 老文件没有 priority 字段（全是 0）：按文件里的原有顺序补编号，行为跟以前完全一致。
	allZero := true
	for _, c := range s.items {
		if c.Priority != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		for i, c := range s.items {
			c.Priority = (i + 1) * 10
		}
	}
	// 老文件只有单个 login_token → 迁移成 Logins 的第一条（多账号改动之前加的登录态不丢）
	for _, c := range s.items {
		if len(c.Logins) == 0 && strings.TrimSpace(c.LoginToken) != "" {
			c.Logins = []ChannelLogin{{Token: c.LoginToken, AddedAt: time.Now()}}
		}
	}
	// 路由顺序：order 字段是权威（含 "lobster" = 龙虾账号池的位置）；
	// 老文件没有 order → 按渠道优先级排好、龙虾池放最后（跟以前的行为一致）。
	s.order = s.normalizeOrderLocked(d.Order)
	s.applyOrderLocked()
	s.def = d.DefaultUpstream
	// fallback_target 是权威字段；fallback 布尔只用于兼容老格式文件。
	// 旧逻辑在这里先读 target、再用布尔覆盖成 "lobster"，
	// 导致文件里明明存着 "ch_xxx"（ds），服务重启后却被悄悄重置回龙虾账号池。
	s.fb = ""
	switch {
	case d.FallbackTarget != "":
		s.fb = d.FallbackTarget
	case d.Fallback != nil:
		// 老格式（只有 fallback 布尔，没有 fallback_target）
		if *d.Fallback {
			s.fb = "lobster"
		}
	}
}

// saveLocked 落盘（调用方需持写锁）。
func (s *Store) saveLocked() {
	if s.file == "" {
		return
	}
	b, err := json.MarshalIndent(map[string]any{
		"channels":         s.items,
		"default_upstream": s.def,
		"fallback":         s.fb != "",
		"fallback_target":  s.fb,
		"order":            s.order,
		"saved_at":         time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(s.file, b, 0644)
}

// Fallback 反代失败时的回退目标："" = 不回退，"lobster" = 账号池，"auto" = 自动匹配，其他 = 渠道 ID。
func (s *Store) Fallback() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fb
}

// SetFallback 设置回退目标；目标必须是 ""、"lobster"、"auto" 或已存在的渠道 ID。
func (s *Store) SetFallback(target string) bool {
	target = strings.TrimSpace(target)
	if target != "" && target != "lobster" && target != "auto" {
		found := false
		s.mu.RLock()
		for _, c := range s.items {
			if c.ID == target {
				found = true
				break
			}
		}
		s.mu.RUnlock()
		if !found {
			return false
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fb = target
	s.saveLocked()
	return true
}

// Default 返回默认上游："" = 自动，"lobster" = 账号池，其他 = 渠道 ID。
func (s *Store) Default() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.def
}

// SetDefault 设置默认上游，id 必须是 ""、"lobster" 或已存在的渠道 ID。
func (s *Store) SetDefault(id string) bool {
	id = strings.TrimSpace(id)
	if id != "" && id != "lobster" {
		found := false
		s.mu.RLock()
		for _, c := range s.items {
			if c.ID == id {
				found = true
				break
			}
		}
		s.mu.RUnlock()
		if !found {
			return false
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.def = id
	s.saveLocked()
	return true
}

// normalizeOrderLocked 把外部给的顺序整理成"合法且完整"的顺序（调用方需持锁）。
// 规则：① 先按输入顺序收已知 ID（去重）；② 没出现在输入里的渠道按优先级补在后面；
// ③ "lobster"（账号池）若缺失则放最末尾 —— 新加的反代默认排在龙虾池前面。
func (s *Store) normalizeOrderLocked(in []string) []string {
	known := map[string]bool{"lobster": true}
	for _, c := range s.items {
		known[c.ID] = true
	}
	out := make([]string, 0, len(known))
	seen := map[string]bool{}
	for _, id := range in {
		if !known[id] || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	rest := make([]*Channel, 0, len(s.items))
	for _, c := range s.items {
		if !seen[c.ID] {
			rest = append(rest, c)
		}
	}
	sort.SliceStable(rest, func(i, j int) bool {
		if rest[i].Priority != rest[j].Priority {
			return rest[i].Priority < rest[j].Priority
		}
		return rest[i].CreatedAt.Before(rest[j].CreatedAt)
	})
	for _, c := range rest {
		seen[c.ID] = true
		out = append(out, c.ID)
	}
	if !seen["lobster"] {
		out = append(out, "lobster")
	}
	return out
}

// applyOrderLocked 按 s.order 重新编渠道优先级并重排 items（调用方需持锁）。
func (s *Store) applyOrderLocked() {
	pos := make(map[string]int, len(s.order))
	for i, id := range s.order {
		pos[id] = i
	}
	for _, c := range s.items {
		if p, ok := pos[c.ID]; ok {
			c.Priority = (p + 1) * 10
		}
	}
	s.sortLocked()
}

// Order 返回当前路由顺序（含 "lobster" = 账号池的位置）。
func (s *Store) Order() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.order...)
}

// byIDLocked 按 ID 取渠道（调用方需持锁）。
func (s *Store) byIDLocked(id string) *Channel {
	for _, c := range s.items {
		if c.ID == id {
			return c
		}
	}
	return nil
}

// Pick 按优先级顺序挑上游：
//
//	返回 "lobster" → 轮到账号池了，该走池；
//	返回渠道 ID   → 用这个渠道；
//	返回 ""       → 顺序里没有任何可用上游。
//
// 顺序里龙虾池排第几，就说明排它前面那些渠道都不接管这个模型时才轮到池。
func (s *Store) Pick(model string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.order) == 0 {
		return ""
	}
	for _, id := range s.order {
		if id == "lobster" {
			return "lobster"
		}
		c := s.byIDLocked(id)
		if c == nil || !c.Enabled {
			continue
		}
		if c.Serves(model) {
			return c.ID
		}
	}
	return ""
}

// sortLocked 按优先级重排（调用方需持锁）。
// 排序键：Priority 小的在前 → 创建时间早的在前 → ID 兜底。
// 路由全靠这个顺序：Match 取第一个命中的，回退兜底从前往后扫。
func (s *Store) sortLocked() {
	sort.SliceStable(s.items, func(i, j int) bool {
		a, b := s.items[i], s.items[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
}

// Reorder 按给定顺序重排路由优先级。
// 列表里可以有 "lobster"（账号池的位置）和任意多个渠道 ID，第 1 个 = 优先级 10。
// 没出现在列表里的渠道排在后面（保持原有相对顺序）；"lobster" 缺失则补到最末。
// 第二个返回值 false = 列表里有未知 ID，整个操作放弃（避免面板传错把顺序搞乱）。
func (s *Store) Reorder(ids []string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	known := map[string]bool{"lobster": true}
	for _, c := range s.items {
		known[c.ID] = true
	}
	for _, id := range ids {
		if !known[id] {
			return 0, false
		}
	}
	s.order = s.normalizeOrderLocked(ids)
	s.applyOrderLocked()
	s.saveLocked()
	n := 0
	for _, id := range s.order {
		if id != "lobster" {
			n++
		}
	}
	return n, true
}

// List 返回全部渠道快照（拷贝，按优先级从高到低）。
func (s *Store) List() []*Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Channel, 0, len(s.items))
	for _, c := range s.items {
		cp := *c
		cp.Models = append([]string(nil), c.Models...)
		cp.SeenModels = append([]string(nil), c.SeenModels...)
		cp.Logins = append([]ChannelLogin(nil), c.Logins...)
		out = append(out, &cp)
	}
	return out
}

// Get 按 ID 取渠道。
func (s *Store) Get(id string) *Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.items {
		if c.ID == id {
			cp := *c
			cp.Models = append([]string(nil), c.Models...)
			cp.SeenModels = append([]string(nil), c.SeenModels...)
			cp.Logins = append([]ChannelLogin(nil), c.Logins...)
			return &cp
		}
	}
	return nil
}

// Add 新增渠道（自动生成 ID + 去重模型名）。
func (s *Store) Add(c *Channel) *Channel {
	if strings.TrimSpace(c.ID) == "" {
		c.ID = newID()
	}
	c.BaseURL = strings.TrimSpace(c.BaseURL)
	c.APIKey = strings.TrimSpace(c.APIKey)
	c.Name = strings.TrimSpace(c.Name)
	c.Models = normalizeModels(c.Models)
	c.SeenModels = normalizeModels(c.SeenModels)
	c.Logins = normalizeLogins(c.Logins)
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 没指定优先级 → 排到末尾（新建的反代默认最后被选中，符合直觉）
	if c.Priority == 0 {
		max := 0
		for _, x := range s.items {
			if x.Priority > max {
				max = x.Priority
			}
		}
		c.Priority = max + 10
	}
	s.items = append(s.items, c)
	// 新渠道插到龙虾池前面（默认最后被选中，但账号池仍保持最末），再整体归一化一遍保证不丢 ID
	base := make([]string, 0, len(s.order)+1)
	inserted := false
	for _, id := range s.order {
		if id == c.ID {
			continue
		}
		if id == "lobster" && !inserted {
			base = append(base, c.ID)
			inserted = true
		}
		base = append(base, id)
	}
	if !inserted {
		base = append(base, c.ID)
	}
	s.order = s.normalizeOrderLocked(base)
	s.applyOrderLocked()
	s.saveLocked()
	return c
}

// Delete 删除渠道。
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.items {
		if c.ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			// 顺序表里也要把它摘掉（被删的 ID 不能留在 order 里）
			no := make([]string, 0, len(s.order))
			for _, oid := range s.order {
				if oid != id {
					no = append(no, oid)
				}
			}
			s.order = s.normalizeOrderLocked(no)
			s.applyOrderLocked()
			if s.def == id {
				s.def = ""
			}
			if s.fb == id {
				// 被删的正是回退目标：关闭回退，不静默切回龙虾账号池（那会悄悄烧积分）。
				s.fb = ""
			}
			s.saveLocked()
			return true
		}
	}
	return false
}

// Replace 用同 ID 的新值覆盖已有渠道（字段已由调用方处理好）。
func (s *Store) Replace(c *Channel) bool {
	if c == nil {
		return false
	}
	c.BaseURL = strings.TrimSpace(c.BaseURL)
	c.APIKey = strings.TrimSpace(c.APIKey)
	c.Name = strings.TrimSpace(c.Name)
	c.Models = normalizeModels(c.Models)
	c.SeenModels = normalizeModels(c.SeenModels)
	c.Logins = normalizeLogins(c.Logins)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, old := range s.items {
		if old.ID == c.ID {
			s.items[i] = c
			s.saveLocked()
			return true
		}
	}
	return false
}

// SetEnabled 启停渠道。
func (s *Store) SetEnabled(id string, on bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.items {
		if c.ID == id {
			c.Enabled = on
			s.saveLocked()
			return true
		}
	}
	return false
}

// SetTest 记录连通测试结果。
func (s *Store) SetTest(id string, ok bool, desc string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.items {
		if c.ID == id {
			c.OK = ok
			c.LastTest = desc
			c.TestedAt = time.Now()
			s.saveLocked()
			return
		}
	}
}

// SetSeen 记录最近一次连通测试实测到的模型名。
// 面板据此在「模型测试」里把每个反代都列出来（哪怕它没勾「接管模型」），
// 并支持一键把这些实测模型写成接管。路由判定不看这个字段。
func (s *Store) SetSeen(id string, models []string) {
	ms := normalizeModels(models)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.items {
		if c.ID == id {
			if sameFoldList(c.SeenModels, ms) {
				return
			}
			c.SeenModels = ms
			s.saveLocked()
			return
		}
	}
}

// sameFoldList 忽略大小写比较两个模型名列表（顺序敏感）。
func sameFoldList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(strings.TrimSpace(a[i]), strings.TrimSpace(b[i])) {
			return false
		}
	}
	return true
}

// channelDeclares 渠道是否「明确声明」接管这个模型（Models ∪ SeenModels 精确命中、或 "*"、或 ModelMap 命中）。
// 跟 Serves 的区别：Serves 把"没声明任何模型"当成不限制（什么都接，用作兜底扫描），
// channelDeclares 只认真正写出来的名字。
func channelDeclares(c *Channel, model string) bool {
	if c == nil {
		return false
	}
	m := strings.ToLower(strings.TrimSpace(model))
	if m == "" {
		return false
	}
	if len(c.ModelMap) > 0 && c.MapModel(model) != model {
		return true
	}
	for _, x := range append(append([]string(nil), c.Models...), c.SeenModels...) {
		x = strings.ToLower(strings.TrimSpace(x))
		if x == "" {
			continue
		}
		if x == "*" || x == m {
			return true
		}
	}
	return false
}

// MatchExplicit 按模型名找「明确声明接管它」的渠道（按优先级顺序取第一个）。
// 用途：账号池根本不认识这个模型名时，直接把请求交给真正声明它的反代，
// 别再让池子先撞一个"空 200"（见 handler 路由第 ③ 步）。
func (s *Store) MatchExplicit(model string) *Channel {
	if strings.TrimSpace(model) == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.items {
		if c == nil || !c.Enabled {
			continue
		}
		if channelDeclares(c, model) {
			cp := *c
			cp.Models = append([]string(nil), c.Models...)
			cp.SeenModels = append([]string(nil), c.SeenModels...)
			cp.Logins = append([]ChannelLogin(nil), c.Logins...)
			return &cp
		}
	}
	return nil
}

// Match 按模型名选渠道：精确命中优先，其次 "*" 兜底；未启用/未命中返回 nil。
func (s *Store) Match(model string) *Channel {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var wildcard *Channel
	for _, c := range s.items {
		if !c.Enabled {
			continue
		}
		for _, m := range c.Models {
			m = strings.ToLower(strings.TrimSpace(m))
			switch {
			case m == "":
			case m == model:
				cp := *c
				return &cp
			case m == "*" && wildcard == nil:
				cp := *c
				wildcard = &cp
			}
		}
	}
	return wildcard
}

// MatchCount 返回启用中的渠道数（面板统计用）。
func (s *Store) MatchCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, c := range s.items {
		if c.Enabled {
			n++
		}
	}
	return n
}

// MapModel 按渠道的 ModelMap 把客户端模型名翻译成该上游真正认的名字。
// 没配映射、或没命中映射时原样返回（保证"不配映射 = 老行为"）。
func (c *Channel) MapModel(model string) string {
	if c == nil || len(c.ModelMap) == 0 {
		return model
	}
	// 先精确匹配，再小写匹配（上游对大小写不敏感，但用户填的时候没准）
	if v, ok := c.ModelMap[model]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	lower := strings.ToLower(strings.TrimSpace(model))
	for k, v := range c.ModelMap {
		if strings.ToLower(strings.TrimSpace(k)) == lower && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return model
}

// Serves 判断该渠道接不接得住这个模型名（用于"面板默认上游"的可用性预判）。
//   - 没声明任何模型（Models 为空）→ 不限制，接得住（保留"默认上游无条件走它"的老语义）
//   - 声明了 ["*"] 或命中某个模型名 → 接得住
//   - 配了 ModelMap 且命中 → 接得住（转发时上游会做名字翻译）
//
// 为什么需要（2026-09-21 实测）：面板把默认上游设成只认 deepseek-* 的渠道后，
// 客户端再发 glm/kimi 这类账号池模型会被硬塞过去 → 上游 404，
// 用户视角就是"设了默认反代以后别的模型全废"。
func (c *Channel) Serves(model string) bool {
	if c == nil {
		return false
	}
	m := strings.ToLower(strings.TrimSpace(model))
	if m == "" {
		return true
	}
	if len(c.ModelMap) > 0 && c.MapModel(model) != model {
		return true
	}
	if len(c.Models) == 0 {
		return true
	}
	for _, x := range c.Models {
		x = strings.ToLower(strings.TrimSpace(x))
		if x == "" {
			continue
		}
		if x == "*" || x == m {
			return true
		}
	}
	return false
}

// Find 按渠道名或 ID（不区分大小写）查找渠道，未启用也算命中（用于显式指定）。
func (s *Store) Find(nameOrID string) *Channel {
	q := strings.ToLower(strings.TrimSpace(nameOrID))
	if q == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.items {
		if strings.ToLower(c.ID) == q || strings.ToLower(strings.TrimSpace(c.Name)) == q {
			cp := *c
			cp.Models = append([]string(nil), c.Models...)
			cp.SeenModels = append([]string(nil), c.SeenModels...)
			return &cp
		}
	}
	return nil
}

// keyRR 按渠道轮询用的计数器（进程内，不落盘 —— 重启后从头开始无所谓）
var keyRR sync.Map // channelID -> *uint64

func rrCounter(id string) *uint64 {
	if v, ok := keyRR.Load(id); ok {
		return v.(*uint64)
	}
	var n uint64
	v, _ := keyRR.LoadOrStore(id, &n)
	return v.(*uint64)
}

// AccountKeys 这个渠道下「配了 sk- 令牌」的后台账号的 key（按加入顺序）
func (c *Channel) AccountKeys() []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.Logins))
	for _, l := range c.Logins {
		if l.Frozen {
			continue // 手工冻结的号不参与转发轮询（解冻即恢复）
		}
		if k := strings.TrimSpace(l.APIKey); k != "" {
			out = append(out, k)
		}
	}
	return out
}

// PickAPIKey 选这次转发用哪个 key：
//   - 有配了令牌的后台账号 → **在这些 key 之间轮流用**（A 模式：多个号一起出力）
//   - 没有 → 回落渠道自己那个 APIKey
func (c *Channel) PickAPIKey() string {
	if c == nil {
		return ""
	}
	keys := c.AccountKeys()
	if len(keys) == 0 {
		return strings.TrimSpace(c.APIKey)
	}
	if len(keys) == 1 {
		return keys[0]
	}
	n := atomic.AddUint64(rrCounter(c.ID), 1)
	return keys[int(n-1)%len(keys)]
}

// Endpoint 归一化 chat/completions 端点（支持用户填到 /v1 或填全路径）。
func (c *Channel) Endpoint() string {
	b := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	switch {
	case b == "":
		return ""
	case strings.HasSuffix(b, "/chat/completions"):
		return b
	case strings.HasSuffix(b, "/v1"):
		return b + "/chat/completions"
	default:
		return b + "/v1/chat/completions"
	}
}

// ModelsEndpoint 归一化 /models 端点。
func (c *Channel) ModelsEndpoint() string {
	b := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	b = strings.TrimSuffix(b, "/chat/completions")
	if b == "" {
		return ""
	}
	if strings.HasSuffix(b, "/v1") {
		return b + "/models"
	}
	return b + "/v1/models"
}

// Test 拉 /models 验证地址与 Token 是否可用，返回模型数量与样例。
func Test(c *Channel, timeout time.Duration) (int, []string, error) {
	ep := c.ModelsEndpoint()
	if ep == "" {
		return 0, nil, fmt.Errorf("地址为空")
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	req, err := http.NewRequest("GET", ep, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return 0, nil, fmt.Errorf("http %d: %s", resp.StatusCode, snippet(raw))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return 0, nil, fmt.Errorf("返回不是 OpenAI 格式: %s", snippet(raw))
	}
	ids := make([]string, 0, len(parsed.Data))
	for _, m := range parsed.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return len(ids), ids, nil
}

// ChatProbe 真发一单最小 chat 请求（max_tokens=1），验证渠道"能不能真正出话"。
//
// 为什么需要（2026-09-21 实测）：柚子api 的 /models 是免费接口 —— 账号余额 ￥0、
// 额度已耗尽，它照样返回 200 和 14 个模型名。面板于是显示"测试通过，14 个模型"，
// 可真发 chat 立刻 403「用户额度不足」/ 429「rpm exhausted」。
// 用户视角就是"反代测着是好的，一用就没效果"。只测 /models = 骗自己。
func ChatProbe(c *Channel, model string, timeout time.Duration) error {
	ep := c.Endpoint()
	if ep == "" {
		return fmt.Errorf("地址为空")
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return fmt.Errorf("没有可探测的模型名")
	}
	if timeout <= 0 {
		timeout = 25 * time.Second
	}
	payload, err := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": "ping"}},
		"max_tokens": 1,
		"stream":     false,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", ep, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<18))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, snippet(raw))
	}
	// 2xx 也可能是"假成功"，必须校验响应形状（2026-09-21 踩坑实测）：
	//   - MiniMax 系：HTTP 200 + {"base_resp":{"status_code":2049,"status_msg":"invalid api key"}}
	//   - 部分中转：HTTP 200 + {"error":{...}}
	// 只看状态码会把上面两种判成"测试通过"，又是一个"测着好好的、一用就没效果"。
	var probe struct {
		Error    json.RawMessage   `json:"error"`
		Choices  []json.RawMessage `json:"choices"`
		BaseResp struct {
			StatusCode *int   `json:"status_code"`
			StatusMsg  string `json:"status_msg"`
		} `json:"base_resp"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return fmt.Errorf("返回不是 JSON: %s", snippet(raw))
	}
	if len(probe.Error) > 0 && string(probe.Error) != "null" {
		return fmt.Errorf("上游返回错误对象: %s", snippet(probe.Error))
	}
	if probe.BaseResp.StatusCode != nil && *probe.BaseResp.StatusCode != 0 {
		return fmt.Errorf("上游业务错误 %d: %s", *probe.BaseResp.StatusCode, probe.BaseResp.StatusMsg)
	}
	if len(probe.Choices) == 0 {
		return fmt.Errorf("响应里没有 choices（模型不可用或不是 chat 接口）: %s", snippet(raw))
	}
	return nil
}

// Forward 把请求体原样转发到渠道。
// 渠道返回 >=400 时不向客户端写任何字节，返回 error 供上层回退到账号池。
func Forward(c *Channel, body []byte, w http.ResponseWriter, stream bool, timeout time.Duration) (map[string]any, error) {
	return ForwardWithKey(c, c.PickAPIKey(), body, w, stream, timeout)
}

// ForwardWithKey 用指定的 key 转发（A 模式下逐个 key 重试用；见 handler 的 tryChannel）。
func ForwardWithKey(c *Channel, apiKey string, body []byte, w http.ResponseWriter, stream bool, timeout time.Duration) (map[string]any, error) {
	ep := c.Endpoint()
	if ep == "" {
		return nil, fmt.Errorf("渠道 %s 地址为空", c.Name)
	}
	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	req, err := http.NewRequest("POST", ep, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(apiKey) == "" {
		apiKey = c.APIKey
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "text/event-stream, application/json")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("渠道 %s http %d: %s", c.Name, resp.StatusCode, snippet(raw))
	}
	isSSE := strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
	if stream && isSSE {
		return streamOut(resp, w)
	}
	if isSSE {
		// 客户端要非流式但渠道硬吐 SSE：聚合成单条响应
		agg, err := upstream.Aggregate(resp.Body)
		if err != nil {
			return nil, err
		}
		if isEmptyCompletion(agg) {
			return nil, fmt.Errorf("渠道 %s 返回空内容（模型不支持或额度用尽）", c.Name)
		}
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, http.StatusOK, agg)
		if u, ok := agg["usage"].(map[string]any); ok {
			return u, nil
		}
		return nil, nil
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var parsed map[string]any
	if json.Unmarshal(raw, &parsed) == nil {
		// 200 里塞错误 / 空壳响应：不下发，让上层回退账号池
		if _, hasErr := parsed["error"]; hasErr {
			return nil, fmt.Errorf("渠道 %s 返回错误: %s", c.Name, snippet(raw))
		}
		if isEmptyCompletion(parsed) {
			return nil, fmt.Errorf("渠道 %s 返回空内容（模型不支持或额度用尽）", c.Name)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
		if u, ok := parsed["usage"].(map[string]any); ok {
			return u, nil
		}
		return nil, nil
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	_, _ = w.Write(raw)
	return nil, nil
}

// streamOut 原样透传 SSE 并顺手抓 usage。
// 先缓冲到「确认有真实内容」才写响应头：渠道返空流时一个字节都不下发，交给上层回退账号池。
func streamOut(resp *http.Response, w http.ResponseWriter) (map[string]any, error) {
	br := bufio.NewReaderSize(resp.Body, 64*1024)
	flusher, _ := w.(http.Flusher)
	var (
		usage   map[string]any
		buf     []string
		started bool
		payload bool
	)
	flush := func() {
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		for _, l := range buf {
			_, _ = io.WriteString(w, l)
		}
		buf = nil
		if flusher != nil {
			flusher.Flush()
		}
		started = true
	}
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			s := strings.TrimSpace(line)
			if strings.HasPrefix(s, "data:") {
				p := strings.TrimSpace(strings.TrimPrefix(s, "data:"))
				if p != "" && p != "[DONE]" {
					var chunk map[string]any
					if json.Unmarshal([]byte(p), &chunk) == nil {
						if u, ok := chunk["usage"].(map[string]any); ok {
							usage = u
						}
						if chunkHasPayload(chunk) {
							payload = true
						}
					}
				}
			}
			if started {
				// 流式也要挡上游的额度/推广文案（整行换成中立 error 事件）
				if _, werr := io.WriteString(w, upstream.SanitizeSSELine(line)); werr != nil {
					break
				}
				if flusher != nil {
					flusher.Flush()
				}
			} else {
				buf = append(buf, upstream.SanitizeSSELine(line))
				if payload {
					flush()
				}
			}
		}
		if err != nil {
			break
		}
	}
	if !started {
		return nil, fmt.Errorf("渠道流式响应为空（模型不支持或额度用尽）")
	}
	return usage, nil
}

// isEmptyCompletion 判断非流式响应是不是空壳（没有任何可展示内容）。
// IsEmptyCompletion 给外面用：这次回包是不是「空回复」（没有 choices / 没内容 / 带 error）。
// 用途：池子回了空 200 时，网关可以改走明确声明该模型的反代（见 handler 池子段）。
func IsEmptyCompletion(m map[string]any) bool { return isEmptyCompletion(m) }

func isEmptyCompletion(m map[string]any) bool {
	if m == nil {
		return true
	}
	if _, hasErr := m["error"]; hasErr {
		return true
	}
	ch, ok := m["choices"].([]any)
	if !ok || len(ch) == 0 {
		return true
	}
	for _, ci := range ch {
		c, _ := ci.(map[string]any)
		if c == nil {
			continue
		}
		msg, _ := c["message"].(map[string]any)
		if msg == nil {
			continue
		}
		if s, _ := msg["content"].(string); strings.TrimSpace(s) != "" {
			return false
		}
		if s, _ := msg["reasoning_content"].(string); strings.TrimSpace(s) != "" {
			return false
		}
		if tc, ok := msg["tool_calls"].([]any); ok && len(tc) > 0 {
			return false
		}
	}
	return true
}

// chunkHasPayload 判断一个流式分片是否带真实内容。
func chunkHasPayload(chunk map[string]any) bool {
	if _, ok := chunk["usage"]; ok {
		return true
	}
	ch, ok := chunk["choices"].([]any)
	if !ok {
		return false
	}
	for _, ci := range ch {
		c, _ := ci.(map[string]any)
		if c == nil {
			continue
		}
		if d, ok := c["delta"].(map[string]any); ok {
			if s, _ := d["content"].(string); strings.TrimSpace(s) != "" {
				return true
			}
			if s, _ := d["reasoning_content"].(string); strings.TrimSpace(s) != "" {
				return true
			}
			if tc, ok := d["tool_calls"].([]any); ok && len(tc) > 0 {
				return true
			}
		}
		if fr, ok := c["finish_reason"].(string); ok && fr != "" {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

// normalizeLogins 去掉空凭据、按 token 去重，保持顺序。
func normalizeLogins(in []ChannelLogin) []ChannelLogin {
	out := []ChannelLogin{}
	seen := map[string]bool{}
	for _, l := range in {
		l.Token = strings.TrimSpace(l.Token)
		l.Label = strings.TrimSpace(l.Label)
		if l.Token == "" || seen[l.Token] {
			continue
		}
		seen[l.Token] = true
		out = append(out, l)
	}
	return out
}

// normalizeModels 去重 + 去空白。
func normalizeModels(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, m := range in {
		m = strings.TrimSpace(m)
		if m == "" || seen[strings.ToLower(m)] {
			continue
		}
		seen[strings.ToLower(m)] = true
		out = append(out, m)
	}
	return out
}

func newID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("ch_%d", time.Now().UnixNano())
	}
	return "ch_" + hex.EncodeToString(b)
}
