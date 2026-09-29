// lobsterai2api 可视化面板 v2：状态卡片 + 模型列表 + 对话测试 + 账号管理
// 监听 :8368，后端代理 :8367 避免 CORS
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	listen   = flag.String("listen", ":8368", "panel listen address")
	upstream = flag.String("upstream", "http://127.0.0.1:8367", "lobsterai2api backend")
	apiKey   = flag.String("apikey", "sk-blueapi-local-2026", "backend api key（仅在 config.json 读不到时兜底）")
)

// 运行时 API Key 列表（历史全保留，全部有效）；config.json 读写
var (
	keyMu      sync.RWMutex
	keyList    []string
	configPath = "C:\\lobsterai2api\\config.json"
)

// getKeys 返回 key 列表副本
func getKeys() []string {
	keyMu.RLock()
	defer keyMu.RUnlock()
	out := make([]string, len(keyList))
	copy(out, keyList)
	return out
}

// proxyKey 代理调后端时用的 key（取列表第一个）
func proxyKey() string {
	ks := getKeys()
	if len(ks) == 0 {
		return ""
	}
	return ks[0]
}

func setKeys(ks []string) {
	keyMu.Lock()
	keyList = ks
	keyMu.Unlock()
}

// loadKeysFromConfig 从 config.json 读 api_keys 数组（兼容旧 api_key 字段）
func loadKeysFromConfig() {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		setKeys([]string{*apiKey})
		return
	}
	var cfg struct {
		APIKey  string   `json:"api_key"`
		APIKeys []string `json:"api_keys"`
	}
	if json.Unmarshal(raw, &cfg) != nil {
		setKeys([]string{*apiKey})
		return
	}
	var out []string
	seen := map[string]bool{}
	add := func(k string) {
		k = strings.TrimSpace(k)
		if k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, k := range cfg.APIKeys {
		add(k)
	}
	add(cfg.APIKey)
	if len(out) == 0 {
		out = []string{*apiKey}
	}
	setKeys(out)
}

// saveKeysToConfig 写回 config.json：api_keys 数组 + api_key 兼容字段（第一个）
func saveKeysToConfig(ks []string) error {
	var cfg map[string]any
	raw, err := os.ReadFile(configPath)
	if err == nil {
		_ = json.Unmarshal(raw, &cfg)
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	cfg["api_keys"] = ks
	if len(ks) > 0 {
		cfg["api_key"] = ks[0]
	} else {
		cfg["api_key"] = ""
	}
	out, _ := json.MarshalIndent(cfg, "", "  ")
	return os.WriteFile(configPath, out, 0644)
}

// keyAlphabet base62 字符集：只用大小写字母 + 数字。
// 故意不含 - 和 _，免得被某些客户端当分隔符截断或做 URL 转义。
const keyAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// genKeyPrefix 生成 Key 的前缀。想换品牌名只改这一处。
const genKeyPrefix = "sk-blueapi-"

// genKeyRandLen 随机部分长度（32 位 base62 ≈ 190 bit 熵，比原来 24 位 hex 的 96 bit 强一倍）。
const genKeyRandLen = 32

// genKey 生成随机 key。
//
// 2026-09-21 改格式：原来是 sk-lobster- + 24 位小写 hex。
// 两个毛病：① 前缀是旧品牌名（面板早改名 BlueAPI 了）；② 字符集只有 [0-9a-f]，
// 又短又单调，看着不像正规 key。
// 现在：sk-blueapi- + 32 位 base62（大小写数字混排），长度 43。
//
// 注意：Key 合法与否只看它是否存在于 config.json 的 api_keys 里，**跟格式无关**，
// 所以这次改格式不会让任何历史 Key 失效。
func genKey() string {
	out := make([]byte, 0, genKeyRandLen)
	buf := make([]byte, 64)
	for len(out) < genKeyRandLen {
		if _, err := rand.Read(buf); err != nil {
			// 极端情况（系统熵不可用）：退回时间戳兜底，至少不是空 key
			return genKeyPrefix + time.Now().Format("20060102150405") + "0000000000000000"
		}
		for _, b := range buf {
			if b >= 248 { // 拒绝采样：248 = 62*4，绕开取模偏倚，保证字符均匀
				continue
			}
			out = append(out, keyAlphabet[int(b)%len(keyAlphabet)])
			if len(out) == genKeyRandLen {
				break
			}
		}
	}
	return genKeyPrefix + string(out)
}

// 积分缓存：默认 60 秒内直接复用，避免每次刷新都打上游
var (
	creditsMu      sync.Mutex
	creditsCache   map[string]float64
	creditsCacheAt time.Time
	creditsTTL     = 60 * time.Second
)

// liveCreditsCached 带缓存的实时积分；force=true 强制回源
func liveCreditsCached(force bool) (map[string]float64, time.Time, bool) {
	creditsMu.Lock()
	defer creditsMu.Unlock()
	if !force && creditsCache != nil && time.Since(creditsCacheAt) < creditsTTL {
		return creditsCache, creditsCacheAt, true // true = 命中缓存
	}
	fresh := liveCredits()
	if len(fresh) > 0 {
		creditsCache = fresh
		creditsCacheAt = time.Now()
	}
	return creditsCache, creditsCacheAt, false
}

// upstreamBase 上游 API 基址：优先 config.json 的 upstream.base_url，其次环境变量，最后默认值
func upstreamBase() string {
	if raw, err := os.ReadFile(configPath); err == nil {
		var cfg struct {
			Upstream struct {
				BaseURL string `json:"base_url"`
			} `json:"upstream"`
		}
		if json.Unmarshal(raw, &cfg) == nil && cfg.Upstream.BaseURL != "" {
			return strings.TrimRight(cfg.Upstream.BaseURL, "/")
		}
	}
	if v := os.Getenv("LB2A_UPSTREAM_BASE"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://lobsterai-server.youdao.com"
}

// liveCredits 实时查各账号上游积分：uid -> credits（失败账号不返回）
func liveCredits() map[string]float64 {
	out := map[string]float64{}
	authDir := filepath.Join(filepath.Dir(configPath), "auths")
	files, err := filepath.Glob(filepath.Join(authDir, "lobsterai-*.json"))
	if err != nil {
		return out
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var probe map[string]json.RawMessage
		if json.Unmarshal(raw, &probe) != nil {
			continue
		}
		var token, uid string
		if _, nested := probe["auth"]; nested {
			var n struct {
				Auth struct {
					AccessToken string `json:"accessToken"`
				} `json:"auth"`
				Account struct {
					UID string `json:"uid"`
				} `json:"account"`
			}
			if json.Unmarshal(raw, &n) != nil {
				continue
			}
			token, uid = n.Auth.AccessToken, n.Account.UID
		} else {
			var fl struct {
				AccessToken string `json:"accessToken"`
				UID         string `json:"uid"`
			}
			if json.Unmarshal(raw, &fl) != nil {
				continue
			}
			token, uid = fl.AccessToken, fl.UID
		}
		if uid == "" && token == "" {
			continue
		}
		if uid == "" {
			uid = filepath.Base(f)
		}
		req, err := http.NewRequest("GET", upstreamBase()+"/api/user/profile-summary", nil)
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("User-Agent", "LobsterAI/0.1.0")
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var env struct {
			Data struct {
				TotalCreditsRemaining float64 `json:"totalCreditsRemaining"`
			} `json:"data"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&env)
		resp.Body.Close()
		if decodeErr != nil {
			continue
		}
		out[uid] = env.Data.TotalCreditsRemaining
	}
	return out
}

const page = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<link rel="icon" href="data:,">
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BlueAPI 面板</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
/* 整体界面放大倍数：改这一个数就能缩放到合适大小（1.0 = 原始大小，1.2 = 放大 20%）。
   --fold-scale 要等于 1.9 ÷ --ui-zoom，这样折叠三角维持用户确认过的大小。 */
:root { --ui-zoom:1.0; --fold-scale:1.9; } /* 1.9 × 1.0 = 1.9 */
/* 各区块标题 / 可折叠卡片标题前面的蓝色三角放大倍数（2 = 两倍）。卡片上那个折叠三角
   用的是 --fold-scale，不在这一条里。 */
:root { --tri-scale:2; }
body { font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif; background:#f5f7fa; color:#1f2937; min-height:calc(100vh / var(--ui-zoom)); zoom:var(--ui-zoom); }
.wrap { max-width:960px; margin:0 auto; padding:20px 16px 40px; }
h1 { font-size:21px; font-weight:700; margin-bottom:3px; color:#111827; }
.sub { color:#6b7280; font-size:12px; margin-bottom:16px; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr)); gap:12px; margin-bottom:12px; align-items:start; }
.card { background:#ffffff; border:1px solid #e5e7eb; border-radius:10px; padding:14px 18px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
.card h2 { font-size:12px; color:#6b7280; font-weight:600; margin-bottom:10px; text-transform:uppercase; letter-spacing:.5px; }
.acc { display:flex; align-items:center; gap:10px; padding:7px 0; border-bottom:1px solid #f3f4f6; }
.acc:last-child { border-bottom:none; padding-bottom:0; }
.acc:first-child { padding-top:0; }
.acc-main { flex:1; min-width:0; }
.acc .name { font-size:14px; font-weight:600; color:#111827; }
.acc .uid { font-size:11px; color:#9ca3af; margin-top:1px; }
.acc-credits { font-size:13px; color:#374151; font-variant-numeric:tabular-nums; white-space:nowrap; }
/* 龙虾账号那一行的「积分」配色：
   2026-09-25 用户要求过红色；2026-09-26 又要求改成蓝色 → 现在用品牌蓝 #2563eb */
.acc-credits.cred-blue { color:#2563eb; font-weight:600; }
/* 账号顺序号（#1 #2 …）：按提交先后排，让"谁先提交的"一眼看得见 */
.acc-seq { display:inline-block; min-width:22px; margin-right:6px; padding:0 5px; border-radius:999px;
           background:#f3f4f6; color:#6b7280; font-size:11px; font-weight:600;
           font-variant-numeric:tabular-nums; text-align:center; }
.del-x { background:transparent; border:1px solid #e5e7eb; color:#9ca3af; font-size:12px; line-height:1; cursor:pointer; padding:4px 10px; border-radius:6px; white-space:nowrap; transition:all .15s; }
.del-x:hover { border-color:#fecaca; color:#dc2626; background:#fef2f2; }
.pill { padding:3px 9px; border-radius:20px; font-size:11px; font-weight:700; white-space:nowrap; }
.pill { padding:3px 9px; border-radius:20px; font-size:11px; font-weight:700; }
.pill.green { background:#dcfce7; color:#15803d; }
.pill.red { background:#fee2e2; color:#b91c1c; }
.pill.yellow { background:#fef3c7; color:#a16207; }
.pill.gray { background:#f3f4f6; color:#6b7280; }
.big { font-size:26px; font-weight:800; color:#111827; }
.meta { font-size:11px; color:#9ca3af; margin-top:3px; }
.models { display:flex; flex-wrap:wrap; gap:6px; }
.model-tag { background:#f3f4f6; border:1px solid #e5e7eb; border-radius:6px; padding:5px 9px; font-size:11px; color:#374151; font-family:"Cascadia Code",Consolas,monospace; }
.pick-model { cursor:pointer; transition:all .12s; }
.pick-model:hover { background:#dbeafe; border-color:#93c5fd; color:#1d4ed8; }
.up-toolbar { display:flex; align-items:center; flex-wrap:wrap; gap:6px; }
.up-label { font-size:12px; font-weight:800; padding:4px 10px; border-radius:6px; margin-right:2px; white-space:nowrap; letter-spacing:.3px; }
.up-label.def { color:#6d28d9; background:#f3e8ff; border:1px solid #ddd6fe; }
.up-label.fb { color:#b45309; background:#fff7e6; border:1px solid #fde68a; }
.up-sep { width:1px; height:16px; background:#e5e7eb; margin:0 6px; }
.up-hint { font-size:12px; color:#6b7280; white-space:nowrap; }
.drag-handle { cursor:grab; color:#9ca3af; user-select:none; margin-right:4px; font-size:14px; }
.drag-handle:hover { color:#2563eb; }
/* 手机端的 ⬆⬇ 调序按钮：桌面用拖动，默认藏起来（见 @media max-width:760px） */
.mv-btn { display:none; }
/* 「使用说明」折叠按钮：桌面默认藏（说明常显），手机端在 @media 里打开 */
.m-help-toggle { display:none; }
/* 「上游反代」页的卡片没接拖动（顺序表里的龙虾账号池不在那一页），别显示这个手柄骗人 */
#ch-list .drag-handle { display:none; }
.up-block.dragging { opacity:.45; border-style:dashed; border-color:#93c5fd; }
.up-block[draggable="true"] .up-head { cursor:grab; }
.up-block[draggable="true"] .up-head:active { cursor:grabbing; }
.up-chip { display:inline-block; padding:5px 12px; border:1px solid #e5e7eb; border-radius:14px; font-size:12px; cursor:pointer; margin:0; background:#ffffff; color:#374151; transition:all .12s; }
.ch-card { border:1px solid #eef0f2; border-radius:10px; padding:14px 16px; background:#ffffff; }
.ch-top { display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap; }
.ch-name { font-size:15px; font-weight:700; color:#111827; }
.ch-btns { display:flex; gap:6px; }
.ch-line { font-size:12px; color:#6b7280; margin-top:8px; line-height:1.8; word-break:break-all; }
.up-block { border:1px solid #eef0f2; border-radius:10px; padding:12px 14px; margin-bottom:12px; background:#ffffff; }
.up-block.sel { border-color:#bfdbfe; background:#f8fbff; }
.up-head { display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:6px; }
/* 折叠三角：用 scale 视觉放大（font-size 加大会顶高整行，scale 不动布局）。
   --fold-scale 已按整体 zoom 折算过：1.46 × 1.3 ≈ 1.9，跟用户确认的大小一致。 */
.up-fold { cursor:pointer; user-select:none; display:inline-block; width:22px; color:#2563eb; font-size:20px; line-height:1; transition:transform .15s; transform:scale(var(--fold-scale)); transform-origin:center center; margin:0 4px; }
.up-fold:hover { color:#1d4ed8; }
.up-block.collapsed .up-fold { transform:scale(var(--fold-scale)) rotate(-90deg); }
.up-block.collapsed .acc-list, .up-block.collapsed .ch-body { display:none; }
.acc-list { display:flex; flex-direction:column; }
.ch-line-inline { font-size:12px; color:#9ca3af; margin-left:6px; }
/* 池子「总积分」：2026-09-26 用户要求"调大一点换个颜色" ——
   从灰字里拎出来：数字加大加粗用品牌绿，标签压深一点保证可读 */
.pool-credits-label { color:#374151; font-weight:600; }
.pool-credits { font-size:17px; font-weight:800; color:#0ea472; font-variant-numeric:tabular-nums; }
.pill.blue { background:#dbeafe; color:#1d4ed8; }
.keyno { flex:0 0 auto; font-size:12px; font-weight:700; color:#6b7280; background:#f3f4f6; border:1px solid #e5e7eb; border-radius:6px; padding:6px 8px; min-width:36px; text-align:center; font-variant-numeric:tabular-nums; }
.note { font-size:11px; color:#6b7280; cursor:pointer; border:1px dashed #e5e7eb; border-radius:6px; padding:1px 7px; margin-left:8px; transition:all .12s; }
.note:hover { border-color:#93c5fd; color:#1d4ed8; background:#f8fbff; }
.note.empty { color:#c3c8d0; }
.note-input { font-size:11px; padding:1px 7px; margin-left:8px; border:1px solid #93c5fd; border-radius:6px; width:180px; color:#111827; }
.note-input:focus { outline:none; border-color:#2563eb; }
.keylimit { font-size:12px; padding:5px 8px; border:1px solid #d1d5db; border-radius:6px; background:#ffffff; color:#374151; margin:0; min-width:92px; }
.keydaily { min-width:104px; }
.model-pick { margin-top:6px; min-width:280px; max-width:100%; }
.keylimit:focus { outline:none; border-color:#2563eb; }
/* 每个 key 的「记录」：key 行 + 可展开的调用明细块（2026-09-22 加） */
.key-row { display:flex; gap:6px; align-items:center; margin-bottom:6px; }
.key-what { background:#fbfcfe; border:1px solid #eef0f2; border-left:3px solid #93c5fd;
            border-radius:8px; padding:8px 12px; margin:0 0 10px 0; }
.kw-list { max-height:230px; overflow:auto; }
.kw-row { display:flex; gap:10px; align-items:baseline; font-size:11px; font-family:Consolas,monospace;
          padding:3px 0; border-bottom:1px dashed #eef0f2; white-space:nowrap; }
.kw-row:last-child { border-bottom:none; }
.kw-t { color:#9ca3af; }
.kw-m { color:#111827; min-width:120px; max-width:260px; overflow:hidden; text-overflow:ellipsis; }
.kw-s { color:#2563eb; min-width:60px; }
.kw-k { color:#6b7280; min-width:80px; text-align:right; }
.kw-ok { color:#15803d; }
.kw-bad { color:#dc2626; overflow:hidden; text-overflow:ellipsis; }
/* 「记录」那张流水表（时间 / 消息 / 模型 / 输入 / 缓存命中 / 输出 / 消耗） */
.kw-bar { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:4px 0 8px; }
.kw-lab { font-size:12px; color:#6b7280; }
.kw-date { font-size:12px; padding:4px 6px; border:1px solid #d1d5db; border-radius:6px;
           background:#ffffff; color:#374151; font-family:inherit; }
.kw-arrow { color:#9ca3af; }
.kw-total { margin-left:auto; font-size:12px; color:#6b7280; }
.kw-tablewrap { overflow-x:auto; border:1px solid #eef0f2; border-radius:8px; background:#ffffff; }
.kw-table { border-collapse:collapse; width:100%; font-size:12px; }
.kw-table th { text-align:left; color:#6b7280; font-weight:600; padding:8px 10px;
               background:#f9fafb; white-space:nowrap; border-bottom:1px solid #eef0f2; }
.kw-table td { padding:7px 10px; border-bottom:1px solid #f3f4f6; white-space:nowrap; }
.kw-table tr:last-child td { border-bottom:none; }
.kw-c-time { color:#6b7280; font-variant-numeric:tabular-nums; }
.kw-c-msg { max-width:320px; overflow:hidden; text-overflow:ellipsis; }
.kw-num { text-align:right; font-variant-numeric:tabular-nums; }
.kw-badrow { background:#fff7f7; }
/* 流水表下面的分页条（2026-09-22 加） */
.kw-pager { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:8px 0 0; }
.kw-pager .kw-lab { font-size:12px; color:#6b7280; }
.kw-size, .kw-goto { font-size:12px; padding:4px 6px; border:1px solid #d1d5db; border-radius:6px;
                     background:#ffffff; color:#374151; font-family:inherit; }
.kw-goto { width:58px; }
.kw-pager button[disabled] { opacity:.45; cursor:not-allowed; }
/* 页头（标题+副标题） + 右上角（状态胶囊 + 用户牌）—— 2026-09-22 照参考站样式 */
.topbar-wrap { display:flex; justify-content:space-between; align-items:flex-start;
               gap:12px; flex-wrap:wrap; margin:-2px 0 14px; }
.page-head { min-width:0; }
.page-title { font-size:20px; font-weight:800; color:#111827; line-height:1.25; }
.page-sub { font-size:12px; color:#6b7280; margin-top:3px; }
.topbar-right { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.conn-pill { font-size:12px; color:#6b7280; background:#f3f4f6; border-radius:20px;
             padding:4px 10px; white-space:nowrap; }
/* 侧栏那条状态文字已整块删除（页头右边有胶囊），这里不用再藏 */
.me { position:relative; display:flex; align-items:center; gap:9px; background:#ffffff;
      border:1px solid #e5e7eb; border-radius:24px; padding:4px 12px 4px 4px; cursor:pointer;
      user-select:none; box-shadow:0 1px 2px rgba(15,23,42,.05); }
.me:hover { border-color:#93c5fd; }
.me-serial { width:34px; height:34px; border-radius:50%; background:#0ea472; color:#ffffff;
             font-weight:800; font-size:14px; display:flex; align-items:center;
             justify-content:center; font-variant-numeric:tabular-nums; }
.me-name { font-size:13px; color:#111827; font-weight:600; max-width:190px;
           overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.me-caret { color:#9ca3af; font-size:12px; }
.me-menu { display:none; position:absolute; right:0; top:46px; min-width:126px; background:#ffffff;
           border:1px solid #e5e7eb; border-radius:10px; box-shadow:0 8px 24px rgba(15,23,42,.12);
           padding:6px 0; z-index:60; }
.me-menu.on { display:block; }
.me-item { padding:8px 14px; font-size:13px; color:#374151; cursor:pointer; white-space:nowrap; }
.me-item:hover { background:#f3f4f6; color:#2563eb; }
.up-chip:hover { border-color:#93c5fd; color:#1d4ed8; }
.up-chip.on { background:#dbeafe; border-color:#93c5fd; color:#1d4ed8; font-weight:600; }
.up-info { margin-top:4px; font-size:11px; color:#6b7280; line-height:1.7; background:#f9fafb; border:1px solid #eef0f2; border-radius:6px; padding:6px 9px; word-break:break-all; }
.section { background:#ffffff; border:1px solid #e5e7eb; border-radius:10px; padding:12px 18px; margin-bottom:8px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
.section h2 { font-size:12px; color:#6b7280; font-weight:600; margin:0; text-transform:uppercase; letter-spacing:.5px; cursor:pointer; user-select:none; display:flex; align-items:center; gap:8px; min-height:26px; }
.section h2:hover { color:#2563eb; }
.section h2::before { content:'▾'; font-size:26px; line-height:1; color:#2563eb; transition:transform .15s; transform:scale(var(--tri-scale)); transform-origin:center center; }
.section.collapsed h2::before { content:'▸'; }
.section.collapsed .sec-body { display:none; }
.sec-body { margin-top:12px; }
.foldable-card h2 { cursor:pointer; user-select:none; display:flex; align-items:center; gap:8px; }
.foldable-card h2::before { content:'▾'; font-size:22px; line-height:1; color:#2563eb; transform:scale(var(--tri-scale)); transform-origin:center center; }
.foldable-card.collapsed h2::before { content:'▸'; }
.foldable-card.collapsed .sec-body { display:none; }
textarea { width:100%; min-height:80px; background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:10px 12px; font-size:14px; resize:vertical; font-family:inherit; }
textarea:focus { outline:none; border-color:#2563eb; }
select { background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:8px 12px; font-size:13px; font-family:inherit; margin-top:10px; margin-right:10px; min-width:200px; }
select option { background:#ffffff; color:#111827; }
button { background:#3b82f6; color:#fff; border:none; border-radius:8px; padding:10px 20px; font-size:14px; font-weight:600; cursor:pointer; margin-top:10px; white-space:nowrap; }
button:hover { background:#1d4ed8; }
button:disabled { background:#e5e7eb; color:#9ca3af; cursor:not-allowed; }
button.gray { background:#f3f4f6; color:#374151; border:1px solid #d1d5db; }
button.gray:hover { background:#e5e7eb; }
button.mini { padding:6px 12px; font-size:12px; margin-top:0; border-radius:6px; }
#chatlog { background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px; padding:12px; min-height:120px; max-height:360px; overflow-y:auto; font-size:13px; line-height:1.7; margin-top:12px; white-space:pre-wrap; color:#374151; }
.msg-user { color:#2563eb; }
.msg-assistant { color:#15803d; }
.err { color:#dc2626; }
.usage { font-size:11px; color:#6b7280; margin:2px 0 8px 0; padding-left:2px; font-family:Consolas,monospace; }
.usage b { color:#b45309; font-weight:600; }
.usage .hit { color:#15803d; }
.usage .miss { color:#dc2626; }
.stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,120px),1fr)); gap:12px; margin-top:12px; }
.stat { background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px; padding:10px 14px; }
.stat .k { font-size:11px; color:#6b7280; }
.stat .v { font-size:18px; font-weight:700; margin-top:2px; color:#111827; }
.footer { margin-top:32px; text-align:center; color:#9ca3af; font-size:12px; }
/* minmax(0,1fr) 而不是 1fr：1fr = minmax(auto,1fr)，列的"自动最小宽度"会被内容的
   min-content 顶开 —— 手机实测（2026-09-22）就是这里把表单列撑到 389px，
   溢出父容器 65px，右边那截在手机上根本看不见（看得见但点不到）。 */
.add-form { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:10px; }
.add-form .full { grid-column:1 / -1; }
.add-form input { background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:8px 12px; font-size:13px; font-family:inherit; width:100%; }
.add-form input:focus { outline:none; border-color:#2563eb; }
.add-form label { font-size:11px; color:#6b7280; display:block; margin-bottom:4px; }
.hint { font-size:12px; color:#6b7280; margin-top:8px; line-height:1.7; }
.hint code { background:#f3f4f6; padding:1px 6px; border-radius:4px; font-size:11px; color:#374151; }
.del { background:transparent; color:#c06060; border:1px solid #3a2020; padding:4px 10px; border-radius:6px; font-size:11px; cursor:pointer; margin:0; white-space:nowrap; }
.del:hover { background:#fee2e2; color:#dc2626; border-color:#fecaca; }
#accmsg { margin-top:10px; font-size:13px; }
.sep { text-align:center; color:#9ca3af; font-size:12px; padding:6px 0; }
.usage-row { display:flex; gap:20px; align-items:flex-start; flex-wrap:wrap; }
.usage-col { min-width:118px; }
.daily-head { font-size:12px; color:#2563eb; cursor:pointer; user-select:none; margin-top:10px; display:inline-flex; align-items:center; gap:5px; }
.daily-head:hover { text-decoration:underline; }
.daily-list { margin-top:8px; border-top:1px solid #f3f4f6; max-height:220px; overflow-y:auto; }
/* 折叠三角（最近回退明细 / 最近访问明细）放大 3 倍：用 scale 不动行高，只是画得更大 */
.daily-tri { display:inline-block; transform:scale(3); transform-origin:center center; margin:0 10px 0 6px; }
.daily-item { display:flex; justify-content:space-between; gap:10px; font-size:12px; padding:6px 2px; border-bottom:1px solid #f9fafb; color:#374151; }
.daily-item .d { color:#6b7280; white-space:nowrap; }
.daily-item.today { font-weight:700; color:#111827; }
.daily-item.today .d { color:#2563eb; }
.app { display:flex; min-height:calc(100vh / var(--ui-zoom)); align-items:flex-start; }
.sidebar { width:184px; flex:0 0 184px; background:#ffffff; border-right:1px solid #e5e7eb; min-height:calc(100vh / var(--ui-zoom)); padding:18px 0; position:sticky; top:0; }
.brand { font-size:14px; font-weight:800; color:#111827; padding:0 18px 16px; }
.nav-item { display:block; padding:13px 18px; font-size:14px; color:#374151; cursor:pointer; border-right:3px solid transparent; user-select:none; transition:all .12s; }
.nav-item:hover { background:#f9fafb; color:#2563eb; }
.nav-item.on { color:#2563eb; font-weight:700; background:#eef4ff; border-right-color:#2563eb; }
/* 导航可以随意拖动排序（按住拖到别的位置松手，顺序会被记住） */
.nav-item.dragging { opacity:.4; background:#eef4ff; border-top:2px dashed #2563eb; }
.main { flex:1; min-width:0; padding:18px 24px 40px; }
.page { display:none; }
.page.on { display:block; }
.side-foot { padding:16px 18px 0; font-size:11px; color:#9ca3af; line-height:1.6; }
.kpi-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,210px),1fr)); gap:12px; margin-bottom:12px; }
.kpi { background:#ffffff; border:1px solid #e5e7eb; border-radius:10px; padding:16px 18px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
.kpi-k { font-size:12px; color:#6b7280; }
.kpi-v { font-size:26px; font-weight:800; color:#111827; margin-top:6px; font-variant-numeric:tabular-nums; }
.kpi-m { font-size:11px; color:#9ca3af; margin-top:6px; line-height:1.6; }
/* 积分卡顶部的来源切换（池子总积分 / 各反代余额）：长得像 kpi-k 标签，但是个下拉 */
.credit-src { font-size:12px; color:#6b7280; background:#ffffff; border:1px solid #e5e7eb; border-radius:6px; padding:2px 4px; margin:0 0 2px -4px; max-width:100%; cursor:pointer; }
.credit-src:hover { border-color:#93c5fd; color:#1d4ed8; }
.credit-src:focus { outline:none; border-color:#2563eb; }
.route-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,120px),1fr)); gap:10px; }
.rcell { background:#f9fafb; border:1px solid #eef0f2; border-radius:8px; padding:10px 12px; }
.rcell .k { font-size:11px; color:#6b7280; }
.rcell .v { font-size:20px; font-weight:700; color:#111827; margin-top:2px; font-variant-numeric:tabular-nums; }
.rcell .v.ok { color:#15803d; }
.rcell .v.bad { color:#dc2626; }
.rcell .v.warn { color:#b45309; }
/* 手机端（2026-09-22 用户实测：导航被切掉、"后端在线"显示两遍、KPI 卡片右边被切） */
@media (max-width:760px) {
  html, body { max-width:100%; overflow-x:hidden; }
  .app { flex-direction:column; }
  /* 导航条：手机上变成一条横向可滑的标签条。
     关键是把 nav 自己撑成"内容多宽就多宽"（width:max-content），
     否则 nav 只有屏宽、里面的标签溢出到外面，看起来就是"被切掉 + 右边一块空白"。 */
  .sidebar { width:100%; max-width:100%; flex:none; min-height:0; position:static; padding:10px 0 0;
             overflow-x:auto; overflow-y:hidden; -webkit-overflow-scrolling:touch; white-space:nowrap; }
  .sidebar nav { display:flex; width:max-content; min-width:100%; }
  .brand { padding:0 14px 10px; }
  .nav-item { display:inline-block; flex:0 0 auto; border-right:none; border-bottom:3px solid transparent; padding:10px 14px; }
  .nav-item.on { border-bottom-color:#2563eb; }
  /* 侧栏底部那条状态在手机上跟内容区顶部的 #conn 完全重复 → 藏一个 */
  .side-foot { display:none; }
  .main { padding:14px; width:100%; }
  /* 卡片只排一列：手机屏窄，两列会被切掉右边 */
  .kpi-grid, .grid, .route-grid, .add-form, .stats { grid-template-columns:minmax(0,1fr) !important; }
  .kpi, .card { padding:12px 14px; }
  /* 大数字跟着屏宽缩，再长也不会把卡片撑破 */
  .kpi-v { font-size:clamp(20px, 6vw, 26px); overflow-wrap:anywhere; }
  .rcell .v { font-size:17px; overflow-wrap:anywhere; }
  table { font-size:12px; }
  .kpi-m, .ch-line, .uid { overflow-wrap:anywhere; }
  /* 下面这些行里的元素太多，手机上会被直接切掉（看得见但点不到）→ 一律允许换行 */
  .acc, .ch-btns, .up-toolbar, .usage-row, .daily-item, .models { flex-wrap:wrap; }
  /* API Key 那一行：#n + key + 状态 + 限速 + 每日额度 + 严格 + 复制 + 删
     一行绝对放不下（实测第 4 个控件就跑到屏幕外 730px 了）→ 换行 + 控件收缩 */
  .key-row { flex-wrap:wrap; }
  .key-row > code { flex:1 1 120px; min-width:0 !important; }
  #keylist .keylimit, #keylist .keydaily { min-width:0; flex:1 1 88px; }
  #keylist .usage { white-space:normal !important; }
  /* 「记录」展开的明细在窄屏上竖着排，别横向溢出 */
  .kw-row { flex-wrap:wrap; white-space:normal; }
  .kw-m { min-width:0; max-width:100%; }
  .kw-s, .kw-k { min-width:0; }
  /* 流水表在手机上横着滑，别把整页撑宽 */
  .kw-c-msg { max-width:150px; }
  .kw-table { font-size:11px; }
  .kw-table th, .kw-table td { padding:6px 8px; }
  .model-pick { min-width:0 !important; }
  .ch-btns button, .acc button { flex:0 0 auto; }
  /* ===== 2026-09-22 §113 手机端手感优化 =====
     问题（手机截图量出来的）：卡片头部 5 个按钮排成 3 行、每个才 30px 高；
     「删」只有 34x22 又被挤到最后一行；账号行的「积分」飘在右边跟信息脱节。
     这里按"手指能点得准 + 别浪费竖向空间"来调。 */
  /* ① 按钮整体放大到能点（34px 是手指能稳定点中的下限） */
  button, .gray, .gray.mini, .btn, .up-chip { min-height:34px; }
  .gray.mini { padding:7px 12px; font-size:12.5px; }
  .del-x { padding:7px 12px; font-size:13px; border-color:#e5e7eb; color:#9ca3af; min-width:42px; }
  /* ② 卡片头部：名字一行、按钮一行，按钮之间留 8px，别挤成 3 行 */
  .up-head { align-items:flex-start; gap:8px; }
  .ch-name { font-size:16px; }
  .ch-btns { gap:8px; row-gap:8px; }
  .up-block { padding:12px 12px 4px; }
  /* ③ 账号行：行间距松一点好点，但「积分」仍跟在账号名那一行右边
        （§113 第一版把积分挤成单独一行，实测每个号多占 40px、11 个号多滚半屏，已回退） */
  .acc { padding:10px 0; gap:8px 10px; align-items:flex-start; }
  .acc .name { font-size:15px; }
  /* ⑥ 手机上调优先级：卡片头那两个 ⬆⬇（HTML5 拖放在触摸屏上不触发，见 moveCard） */
  #accounts .mv-btn { display:inline-flex; align-items:center; justify-content:center;
                      width:34px; min-width:34px; height:34px; padding:0; font-size:15px;
                      line-height:1; border:1px solid #d1d5db; background:#ffffff; color:#374151;
                      border-radius:8px; cursor:pointer; }
  #accounts .mv-btn:active { background:#eff6ff; border-color:#93c5fd; }
  /* ⑦「上游反代」页那一大段使用说明：手机上折起来，别一屏全是字
        （这个按钮桌面端才藏、手机端要显示 —— §113 第一版忘写手机端的 display，按钮压根看不见） */
  .m-help-toggle { display:inline-block; margin:2px 0 8px; }
  .ch-help { display:none; }
  .ch-help.open { display:block; }
  /* ④ 顶部页头/工具栏收一点，别占掉半屏 */
  .topbar-wrap { margin:-2px 0 10px; gap:8px; }
  .page-title { font-size:18px; }
  .up-toolbar { gap:6px; }
  .up-chip { padding:6px 10px; font-size:12px; }
  /* ⑤ 导航条当前项在手机上自动滑进视野（见 showPage 里的 scrollActiveNav） */
}
</style>
</head>
<body>
<div class="app">
  <aside class="sidebar">
    <div class="brand">🔷 BlueAPI</div>
    <nav>
      <div class="nav-item" data-page="overview">数据概览</div>
      <div class="nav-item" data-page="pool">账号池</div>
      <div class="nav-item" data-page="addacc">添加账号</div>
      <div class="nav-item" data-page="relay">上游反代</div>
      <div class="nav-item" data-page="phone">手机登录</div>
      <div class="nav-item" data-page="apikey">API Key</div>
      <div class="nav-item" data-page="chat">模型测试</div>
      <div class="nav-item" data-page="visitors">访问 IP</div>
      <div class="nav-item" data-page="admin">后台管理</div>
    </nav>
    <!-- 侧栏底部那块（状态文字 + 改密码/退出登录）2026-09-22 §110 整块删掉：
         状态已经挪到页头右上角的胶囊，改密码/退出登录走右上角用户牌的下拉菜单。 -->
  </aside>
  <main class="main">
  <!-- 页头（标题 + 副标题，2026-09-22 照参考站样式做）+ 右上角：状态 + 用户牌 -->
  <div class="topbar-wrap">
    <div class="page-head">
      <h1 class="page-title" id="page-title">数据概览</h1>
      <div class="page-sub" id="page-sub">欢迎回来！这是您账户的概览。</div>
    </div>
    <div class="topbar-right">
      <span class="conn-pill" id="conn">连接中...</span>
      <div class="me" id="me-box" style="display:none">
        <span class="me-serial" id="me-serial">1</span>
        <span class="me-name" id="me-name">admin</span>
        <span class="me-caret">&#9662;</span>
        <div class="me-menu" id="me-menu">
          <div class="me-item" data-go="passwd">改密码</div>
          <div class="me-item" data-go="logout">退出登录</div>
        </div>
      </div>
    </div>
  </div>

  <div class="page" data-page="pool">
    <div class="card foldable-card" id="pool-card">
      <h2>账号池（自动轮换选号）</h2>
      <div id="up-pick" class="up-toolbar" style="margin-bottom:10px"></div>
      <div class="hint" id="up-warn" style="margin:0 0 12px"></div>
      <div id="pool-chan-form" class="add-form" style="display:none; margin-bottom:14px; border:1px solid #eef0f2; border-radius:10px; padding:12px 14px; background:#fcfcfd">
        <div><label>名称</label><input id="p-name" placeholder="如：中转A"></div>
        <div><label>地址</label><input id="p-url" placeholder="https://xxx.com/v1"></div>
        <div class="full"><label>Token</label><input id="p-key" placeholder="sk-..."></div>
        <div class="full"><label>接管模型（逗号分隔；* = 全部接管；留空 = 只存不接管）</label><input id="p-models" placeholder="kimi-k3, sensenova-6.8-flash-lite">
          <select id="p-models-pick" class="model-pick"><option value="">— 从可用模型里选（选一次加一个，也可以直接手输） —</option></select></div>
        <div class="full"><label>模型映射（可选：客户端名=上游真名，逗号分隔）</label>
          <input id="p-modelmap" placeholder="如：deepseek-flash=DeepSeek-V4.1-Flash, glm-5.2=GLM-5.3"></div>
        <div class="full"><label>备注（可选，也可以之后在卡片上点「＋备注」直接写）</label><input id="p-note" placeholder="如：备用线路"></div>
        <div class="full"><button id="p-save">保存反代</button> <button id="p-cancel" class="gray">取消</button></div>
      </div>
      <div id="accounts"><div class="meta">加载中...</div></div>
      <div class="hint" style="margin-top:12px">所有账号合并为一个池：任意 Token 调用时自动选积分最高且健康的账号；单次请求可用模型名前缀（<span id="prefix-hint">--</span>）单独指定上游。</div>
    </div>
  </div>

  <div class="page" data-page="overview">
    <div class="kpi-grid">
      <div class="kpi">
        <select id="credit-src" class="credit-src" title="切换这个卡片显示哪里的积分/余额"></select>
        <div class="kpi-v" id="credits">--</div>
        <div class="kpi-m" id="credits-meta"></div>
        <button id="refresh-credits" class="gray mini" style="margin-top:10px">刷新</button>
      </div>
      <div class="kpi">
        <div class="kpi-k">今日 Token 消耗</div>
        <div class="kpi-v" id="usage-today">--</div>
        <div class="kpi-m" id="usage-today-detail"></div>
      </div>
      <div class="kpi">
        <div class="kpi-k">累计 Token 消耗</div>
        <div class="kpi-v" id="usage-total">--</div>
        <div class="kpi-m" id="usage-detail"></div>
        <div class="kpi-m" id="price-line" style="margin-top:6px"></div>
      </div>
      <!-- 2026-09-26 用户要求：概览里加一张「生成了多少 API 密钥」的卡片。
           数字由 loadKeys() 顺手更新（它本来每 30 秒就会拉一次 /api/apikey），不额外发请求。 -->
      <div class="kpi">
        <div class="kpi-k">API 密钥</div>
        <div class="kpi-v" id="key-count">--</div>
        <!-- 2026-09-26 用户要求：这张卡下面只留「N 启用」，其它说明文字一律删掉。
             「启用」口径 = 有过调用记录的密钥数 -->
        <div class="kpi-m"><b id="key-on" title="有过调用记录的密钥数（其余从未使用）">-</b> 启用</div>
      </div>
    </div>

    <div class="card" style="margin-bottom:12px">
      <h2>上游路由</h2>
      <div class="route-grid">
        <div class="rcell"><div class="k">反代成功</div><div class="v ok" id="rt-ok">0</div></div>
        <div class="rcell"><div class="k">反代失败</div><div class="v bad" id="rt-fail">0</div></div>
        <div class="rcell"><div class="k" id="rt-fb-label">回退次数</div><div class="v warn" id="rt-fb">0</div></div>
        <div class="rcell"><div class="k">正常走池</div><div class="v" id="rt-pool">0</div></div>
        <div class="rcell" id="rt-vis-cell" title="谁在访问这个面板（按 IP + 浏览器算一条）">
          <div class="k">访问 IP</div><div class="v" id="rt-vis">0</div>
        </div>
      </div>
      <div class="meta" id="route-since" style="margin-top:8px"></div>
      <div class="meta" id="route-dest" style="margin-top:4px"></div>
      <div id="up-note" class="hint" style="margin-top:12px"></div>
        <div class="daily-head" id="fb-head">最近回退明细 <span class="daily-tri" id="fb-tri">&#9656;</span></div>
      <div class="daily-list" id="fb-list" style="display:none"></div>
    </div>

    <div class="card">
      <h2>按天明细</h2>
      <div class="daily-list" id="daily-list" style="display:block; border-top:none; max-height:300px"></div>
    </div>
    <div class="card" style="margin-top:12px">
      <h2>按上游用量（今日 / 累计）</h2>
      <div class="meta" style="margin-bottom:6px">反代（含回退兜底走到的反代）的消耗，同样计入上面「今日 / 累计 Token 消耗」的总数；这里只是把总数拆开给你看。两边用同一个累加器，<b>相加恒等于总数</b>。注：本次改动之前的历史流量统一归到「账号池」名下，之后按真实来源记。</div>
      <div class="daily-list" id="src-list" style="display:block; border-top:none; max-height:320px"></div>
    </div>
  </div>

  <div class="page" data-page="phone">
  <div class="section">
    <h2>手机验证码登录</h2>
    <div class="hint">服务器浏览器自动打开登录页并填写提交。你只需输手机号和短信验证码，登录后账号自动进池子。</div>
    <div style="display:flex; gap:8px; align-items:center; margin-top:10px; flex-wrap:wrap">
      <input id="scan-invite" placeholder="邀请码（可选，填了给邀请人+300分）" style="background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:9px 12px; font-size:14px; width:260px">
    </div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:10px">
      <input id="scanp-phone" placeholder="手机号" style="background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:9px 12px; font-size:14px; width:180px">
      <button id="scanp-send" class="gray">发送验证码</button>
    </div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:8px">
      <input id="scanp-sms" placeholder="短信验证码" style="background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:9px 12px; font-size:14px; width:140px">
      <button id="scanp-login">提交登录</button>
    </div>
    <div id="scanp-msg" style="margin-top:10px; font-size:13px"></div>
  </div>

  </div>

  <div class="page" data-page="addacc">
  <div class="section">
    <h2>添加账号</h2>
    <div class="hint">龙虾是 OAuth 扫码登录，没有账号密码。从其他已登录设备导出 auth JSON 粘贴到这里即可。
    获取方式：已登录客户端目录下 <code>auths\lobsterai-&lt;uid&gt;.json</code>，或客户端数据库 kv 表的 <code>auth_tokens</code> 字段。</div>
    <div style="margin-top:12px">
      <div class="add-form">
        <div class="full"><label>完整 auth JSON（自动解析所有字段）</label>
          <textarea id="authjson" style="min-height:140px" placeholder='{"accessToken":"...","refreshToken":"...","uid":"...","nickname":"...","uuid":"...","firstKeyfrom":"official","latestKeyfrom":"official"}'></textarea></div>
      </div>
      <button id="addacc" title="后端是热生效：加完立刻能用，不会重启服务（重启只会掐断正在跑的请求）">添加账号（热生效，不重启）</button>
      <div id="accmsg"></div>
    </div>
  </div>

  </div>

  <div class="page" data-page="relay">
  <div class="section">
    <h2>上游反代（多渠道）</h2>
    <!-- 手机上这段说明占满整屏（表单被顶到第二屏），所以手机端折起来，点这个按钮看 -->
    <button type="button" class="gray mini m-help-toggle" id="ch-help-btn">使用说明 ▾</button>
    <div class="hint ch-help" id="ch-help">粘贴别人的 OpenAI 兼容中转（反代）地址 + Token，按<strong>模型名</strong>接管：命中该模型的请求直连这个反代，其余继续走龙虾账号池。
    地址支持 <code>https://xxx.com</code>、<code>https://xxx.com/v1</code> 或全路径 <code>.../v1/chat/completions</code>。<br>
    <strong>指定上游</strong>：模型名写 <code>渠道名/模型</code> 强制走该反代（如 <span id="prefix-hint2">渠道名/模型</span>）；写 <code>lobster/模型</code> 强制走龙虾账号池（如 <code>lobster/deepseek-v4-pro</code>）；不带前缀则按接管规则自动选。
    这两种前缀写法在下面「可用模型」里已经列好了，直接选就行。</div>
    <div style="margin-top:12px">
      <div class="add-form">
        <div><label>名称</label><input id="ch-name" placeholder="如：中转A"></div>
        <div><label>地址</label><input id="ch-url" placeholder="https://xxx.com/v1"></div>
        <div class="full"><label>Token</label><input id="ch-key" placeholder="sk-..."></div>
        <div class="full"><label>接管模型（逗号分隔；* = 全部接管；留空 = 只存不接管）</label>
          <input id="ch-models" placeholder="gpt-4o, claude-sonnet-4, deepseek-chat">
          <select id="ch-models-pick" class="model-pick"><option value="">— 从可用模型里选（选一次加一个，也可以直接手输） —</option></select></div>
        <div class="full"><label>备注（可选，也可以之后在卡片上点「＋备注」直接写）</label>
          <input id="ch-noteinput" placeholder="如：备用线路"></div>
        <div class="full"><label>模型映射（可选：客户端名=上游真名，逗号分隔）</label>
          <input id="ch-modelmap" placeholder="如：deepseek-flash=DeepSeek-V4.1-Flash, glm-5.2=GLM-5.3"></div>
      </div>
      <button id="ch-add">添加反代（自动测连通）</button>
      <button id="ch-cancel" class="gray" style="display:none">取消编辑</button>
      <div id="ch-msg"></div>
    </div>
    <div id="ch-list" style="margin-top:14px"></div>
  </div>

  </div>

  <div class="page" data-page="apikey">
  <div class="section">
    <h2>API Key（历史全保留，均已生效）<span id="keycount" style="font-weight:400; color:#9ca3af"></span></h2>
    <div class="hint">客户端（Cherry Studio / NextChat / SDK）填任意一个 Key 都能访问；生成新 Key 后老 Key 继续有效，不失效。</div>
    <div class="hint">左边的 <b>#1 #2 #3 …</b> 是按添加顺序编的序号：<b>#1</b> 最早生成，数字越大越新。</div>
    <div class="hint">每个 Key 右边两个闸门：<b>每分钟限速</b>（滑动窗口，超了默认<b>排队等待</b>，照样能用只是变慢；勾上「严格」就改成直接 <code>429</code>）＋ <b>每日额度</b>（按单数，超了<b>直接 429 不排队</b>，第二天自动重置，卖套餐用这个）。面板自己的探活/管理请求不计入。</div>
    <div class="hint">随机生成的 Key 格式：<code>sk-blueapi-</code> 加 32 位随机字符（大小写字母 + 数字，总长 43）。要自定义就直接填下面的输入框，<b>任何格式</b>都能用。</div>
    <div class="hint">每个 Key 右边有个 <b>「记录」</b> 按钮：点开是一张<b>调用流水表</b> —— <b>时间 / 消息 / 模型 / 输入 TOKENS / 缓存命中 TOKENS / 输出 TOKENS / 消耗(积分估算) / 费用估算 / 上游·结果</b>，
    可以按 <b>日期范围</b> 筛，右上角显示 <b>共 N 条调用</b>。流水落盘在 <code>data\keycalls.jsonl</code>（主服务重启也不丢），最多留最近 12000 条。</div>
    <div style="margin-top:12px; background:#f9fafb; border:1px solid #eef0f2; border-radius:8px; padding:10px 12px">
      <div class="meta">接口地址（Cherry Studio / NextChat / SDK 的 Base URL 填这个）</div>
      <div style="display:flex; gap:8px; align-items:center; margin-top:6px; flex-wrap:wrap">
        <code id="api-base" style="background:#ffffff; border:1px solid #e5e7eb; border-radius:6px; color:#1d4ed8; padding:6px 10px; font-size:13px; word-break:break-all">--</code>
        <button class="gray mini" id="copy-base">复制</button>
        <button class="gray mini" id="test-base">测一下</button>
        <span class="meta" id="api-full"></span>
      </div>
    </div>
    <div id="keylist" style="margin-top:12px"></div>
    <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:12px">
      <input id="keycustom" placeholder="输入自定义 Key（留空则随机生成 sk-blueapi- 开头的 Key）" style="background:#ffffff; border:1px solid #d1d5db; border-radius:8px; color:#111827; padding:10px 14px; font-size:14px; font-family:inherit; flex:1; min-width:260px">
      <button id="keygen">生成 / 添加新 Key</button>
    </div>
    <div id="keymsg" style="margin-top:10px; font-size:13px"></div>
  </div>

  </div>

  <div class="page" data-page="chat">
  <div class="section">
    <h2>模型测试</h2>
    <textarea id="prompt" placeholder="输入消息，如：你好"></textarea>
    <div>
      <select id="model"></select>
      <button id="send">发送</button>
      <button id="clear" class="gray">清空</button>
    </div>
    <div id="chatlog"></div>
    <div class="stats" id="stats" style="display:none">
      <div class="stat"><div class="k">本次缓存命中</div><div class="v hit" id="s-hit">0</div></div>
      <div class="stat"><div class="k">本次缓存未命中</div><div class="v miss" id="s-miss">0</div></div>
      <div class="stat"><div class="k">本次输出 token</div><div class="v" id="s-out">0</div></div>
      <div class="stat"><div class="k">本次思考 token</div><div class="v" id="s-reason">0</div></div>
      <div class="stat"><div class="k">累计请求</div><div class="v" id="s-req">0</div></div>
      <div class="stat"><div class="k">累计缓存命中率</div><div class="v" id="s-rate">0%</div></div>
    </div>
  </div>

  <div class="section">
    <h2>可用模型 (<span id="modelcount">0</span>)</h2>
    <div class="models" id="models"><span class="model-tag">加载中...</span></div>
    <div class="hint">点模型名 = 直接填进上面的下拉框。列表按来源分组，顺序和分组数量跟「账号池」页的优先级表一致。</div>
  </div>

  </div>

  <div class="page" data-page="visitors">
    <div class="card">
      <h2>访问 IP（谁在访问这个面板）</h2>
      <div class="hint" style="margin:0 0 10px">
        电脑和手机会分开列（同一台路由器后面也算两条）。标「公网」的是从外网进来的 ——
        看到不认识的 IP，可以直接<b>禁用</b>（该 IP 一律 403）或<b>删掉</b>这条记录；
        也能顺手换掉 API Key，或把 8368 端口只放行你自己的 IP。
        <button id="vis-refresh" class="gray mini" style="margin-left:8px">刷新</button>
      </div>
      <div class="meta" id="vis-you" style="margin-bottom:8px"></div>
      <div class="hint" id="vis-msg" style="margin:6px 0"></div>
      <div id="vis-list"><div class="meta">加载中...</div></div>
      <div style="margin-top:16px">
        <div class="daily-head" id="vis-log-head" title="点一下收起 / 展开">最近访问明细（每次打开面板记一条，最多 500 条，面板重启也不丢） <span class="daily-tri" id="vis-log-tri">&#9662;</span></div>
        <div class="daily-list" id="vis-log" style="max-height:320px"><div class="meta">加载中...</div></div>
      </div>
    </div>
  </div>

  <div class="page" data-page="admin">
    <div class="card">
      <h2>后台管理（面板自己的门锁）</h2>
      <div class="hint" style="margin:0 0 10px">
        这里管的是<b>面板登录</b>本身：开不开登录、要不要数字验证码、账号密码、被锁的 IP、以及登录记录。
        （跟业务无关 —— 业务相关的在「账号池 / 上游反代 / API Key」那几页。）
        <button id="adm-refresh" class="gray mini" style="margin-left:8px">刷新</button>
      </div>
      <div class="hint" id="adm-msg" style="margin:6px 0"></div>
      <div id="adm-body"><div class="meta">加载中...</div></div>
    </div>
  </div>

  <div class="footer">BlueAPI panel v3 · 数据来自后端 :8367</div>
  </main>
</div>

<div id="confirm-modal" style="display:none; position:fixed; inset:0; background:rgba(15,23,42,.35); z-index:999; align-items:center; justify-content:center">
  <div style="background:#fff; border-radius:14px; padding:22px 26px; min-width:300px; max-width:420px; box-shadow:0 12px 48px rgba(0,0,0,.18)">
    <div id="cm-title" style="font-size:15px; font-weight:700; color:#111827; margin-bottom:10px">确认删除</div>
    <div id="cm-text" style="font-size:13px; color:#6b7280; margin-bottom:20px; line-height:1.7; word-break:break-all"></div>
    <div style="display:flex; gap:10px; justify-content:flex-end">
      <button id="cm-cancel" class="gray" style="margin-top:0">取消</button>
      <button id="cm-ok" style="margin-top:0; background:#dc2626">确认删除</button>
    </div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);

let lastCreditsAt = 0;
// 积分卡的可切换来源：'' = 池子总积分（龙虾账号池），渠道 id = 那个反代的余额/用量
let creditPool = { total: 0, at: 0 };
let creditChan = null;   // 后端 /channels/balance 的返回（含 id）

// 来源下拉：池子总积分 + 每个反代
function fillCreditSrc() {
  const s = $('credit-src');
  if (!s) return;
  const keep = s.value;
  const chs = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []);
  s.innerHTML = '<option value="">池子总积分</option>'
    + chs.map(c => '<option value="' + escHtml(c.id) + '">' + escHtml(c.name) + ' 余额</option>').join('');
  if (keep && Array.prototype.some.call(s.options, o => o.value === keep)) s.value = keep;
  s.onchange = function() { creditChan = null; renderCreditCard(true); };
}

// 画积分卡：按当前来源显示
async function renderCreditCard(force) {
  const kb = $('credits'), km = $('credits-meta');
  if (!kb || !km) return;
  const src = $('credit-src') ? $('credit-src').value : '';
  if (!src) {
    kb.textContent = String(Math.round(creditPool.total * 100) / 100);
    km.textContent = creditPool.at
      ? '数据 ' + relTime(new Date(creditPool.at).toISOString()) + ' · 60 秒缓存'
      : '等待后端数据';
    return;
  }
  if (!force && creditChan && creditChan.id === src) { paintChanBalance(creditChan); return; }
  kb.textContent = '...'; km.textContent = '查询中（最多 15 秒）';
  try {
    const d = await fetchJSON('/api/channelbalance?id=' + encodeURIComponent(src) + (force ? '&force=1' : ''));
    if (!d) throw new Error('empty');
    d.id = src;
    creditChan = d;
    paintChanBalance(d);
  } catch (e) {
    kb.textContent = '--';
    km.textContent = '查询失败: ' + (e && e.message ? e.message : e);
  }
}

function paintChanBalance(d) {
  const kb = $('credits'), km = $('credits-meta');
  if (!kb || !km) return;
  // 一个反代可能挂了好几个后台账号 → 卡片要显示**合计**（用户要求：Tier 显示总余额）
  const list = (d.accounts && d.accounts.length) ? d.accounts : ((d.balance) ? [{ balance: d.balance }] : []);
  const oks = list.map(a => (a.balance ? a.balance : a)).filter(b => b && b.ok);
  if (oks.length > 1) {
    const cur = oks[0].currency;
    const sameCur = oks.every(b => b.currency === cur);
    if (sameCur) {
      const sym = (String(cur || '').toUpperCase() === 'USD') ? '$' : '¥';
      const r2 = n => Math.round(n * 100) / 100;
      const sumT = oks.reduce((s, b) => s + (typeof b.total === 'number' ? b.total : (typeof b.remaining === 'number' ? b.remaining : 0)), 0);
      const sumU = oks.reduce((s, b) => s + (typeof b.used === 'number' ? b.used : 0), 0);
      kb.textContent = sym + r2(sumT);
      const each = oks.map(b => (b.phone || b.username || '?') + ' ' + sym + r2(b.total !== undefined ? b.total : 0)).join(' · ');
      km.textContent = oks.length + ' 个号合计（' + each + '）· 累计已用 ' + sym + r2(sumU)
        + (d.cached ? ' · 60 秒缓存' : '');
      return;
    }
  }
  const b = d.balance || {};
  const sym = (String(b.currency || '').toUpperCase() === 'USD') ? '$' : '¥';
  if (typeof b.total === 'number') {
    kb.textContent = sym + (Math.round(b.total * 100) / 100);
  } else if (typeof b.remaining === 'number') {
    kb.textContent = sym + (Math.round(b.remaining * 100) / 100);
  } else if (typeof b.used === 'number') {
    kb.textContent = '已用 ' + sym + (Math.round(b.used * 10000) / 10000);
  } else {
    kb.textContent = '--';
  }
  const head = b.ok ? '' : '拿不到：';
  km.textContent = (head + (b.detail || '')).slice(0, 160) + (d.cached ? ' · 60 秒缓存' : '');
}

function fmtNum(n) {
  return (n || 0).toLocaleString('en-US');
}

async function loadUsage() {
  try {
    const d = await fetchJSON('/api/usage');
    if (!d) return;
    const u = d.usage || {};
    const t = d.today || {};
    const todayKey = (d.now || '').slice(0, 10);
    $('usage-today').textContent = fmtNum(t.total_tokens);
    $('usage-total').textContent = fmtNum(u.total_tokens);
    const p = d.price || {};
    priceIn = (typeof p.input_per_m === 'number') ? p.input_per_m : priceIn;
    priceCache = (typeof p.cached_per_m === 'number') ? p.cached_per_m : priceCache;
    priceOut = (typeof p.output_per_m === 'number') ? p.output_per_m : priceOut;
    cnyRate = (typeof p.cny_rate === 'number') ? p.cny_rate : cnyRate;
    $('usage-today-detail').textContent = usageText(t) + (t.requests ? ' · ' + moneyOf(t) : '');
    $('usage-detail').textContent = usageText(u) + (u.requests ? ' · ' + moneyOf(u) : '');
    // 把总数拆成「池子 + 反代」，让用户一眼看出反代的消耗确实计入了总数
    const srcT = d.sources_today || {}, srcA = d.sources || {};
    // 反代卡片的「今日 N 单 · X token」要用它（按上游来源记账，见 /api/usage 的 sources_today）
    window.__srcToday = srcT;
    const sumOf = (m, skip) => Object.keys(m).filter(k => k !== skip)
      .reduce((s, k) => s + ((m[k] && m[k].total_tokens) || 0), 0);
    const poolT = (srcT['lobster'] && srcT['lobster'].total_tokens) || 0;
    const poolA = (srcA['lobster'] && srcA['lobster'].total_tokens) || 0;
    $('usage-today-detail').textContent += ' · 池子 ' + fmtNum(poolT) + ' + 反代 ' + fmtNum(sumOf(srcT, 'lobster'));
    $('usage-detail').textContent += ' · 池子 ' + fmtNum(poolA) + ' + 反代 ' + fmtNum(sumOf(srcA, 'lobster'));
    renderSources(d.sources || {}, d.sources_today || {});
    renderPriceLine();
    renderDaily(d.daily || {}, todayKey);
    renderRoute(d.route || {}, d.fallbacks || [], d.route_since);
  } catch(e) {
    $('usage-today').textContent = '--';
    $('usage-total').textContent = '--';
  }
}

function usageText(u) {
  if (!u || !u.requests) return '暂无调用记录';
  const rate = u.prompt_tokens ? Math.round((u.cache_hit_tokens || 0) / u.prompt_tokens * 100) : 0;
  return '输入 ' + fmtNum(u.prompt_tokens) + ' · 输出 ' + fmtNum(u.completion_tokens)
    + ' · 思考 ' + fmtNum(u.reasoning_tokens) + ' · 请求 ' + fmtNum(u.requests) + ' 次 · 缓存命中率 ' + rate + '%';
}

// 估算金额：默认按 OpenAI GPT-4o 官方价（USD / 每百万 token），可在面板改
let priceIn = 2.5;
let priceCache = 1.25;
let priceOut = 10;
let cnyRate = 7.2;
function moneyOf(u) {
  const inTok = u.prompt_tokens || 0;
  const cached = Math.min(u.cache_hit_tokens || 0, inTok);
  const fresh = Math.max(0, inTok - cached);
  const usd = (fresh / 1e6) * priceIn + (cached / 1e6) * priceCache + ((u.completion_tokens || 0) / 1e6) * priceOut;
  const usdStr = usd >= 1 ? usd.toFixed(2) : usd.toFixed(3);
  return '≈ $' + usdStr + (cnyRate > 0 ? '（¥' + (usd * cnyRate).toFixed(2) + '）' : '');
}
function renderPriceLine() {
  const pl = $('price-line');
  if (!pl) return;
  pl.innerHTML = '按 GPT 官方价估算（USD / 每百万 token）：输入 $' + priceIn + ' · 缓存 $' + priceCache + ' · 输出 $' + priceOut + ' · 汇率 ' + cnyRate
    + ' <a href="javascript:void(0)" id="edit-price" style="color:#2563eb;margin-left:4px">改价</a>';
  $('edit-price').onclick = () => {
    pl.innerHTML = '输入 <input id="pi" class="note-input" style="width:56px;margin:0 3px" value="' + priceIn + '">'
      + '缓存 <input id="pc" class="note-input" style="width:56px;margin:0 3px" value="' + priceCache + '">'
      + '输出 <input id="po" class="note-input" style="width:56px;margin:0 3px" value="' + priceOut + '">'
      + '汇率 <input id="pr" class="note-input" style="width:52px;margin:0 3px" value="' + cnyRate + '">'
      + ' <button class="gray mini" id="psave">保存</button>'
      + ' <a href="javascript:void(0)" id="pgpt" style="color:#2563eb;margin-left:8px">恢复 GPT-4o 价</a>'
      + ' <a href="javascript:void(0)" id="pmini" style="color:#2563eb;margin-left:8px">GPT-4o mini 价</a>';
    $('pgpt').onclick = () => { $('pi').value = 2.5; $('pc').value = 1.25; $('po').value = 10; };
    $('pmini').onclick = () => { $('pi').value = 0.15; $('pc').value = 0.075; $('po').value = 0.6; };
    $('psave').onclick = async () => {
      const i = parseFloat($('pi').value);
      const c = parseFloat($('pc').value);
      const o = parseFloat($('po').value);
      const r = parseFloat($('pr').value);
      if (isNaN(i) || isNaN(c) || isNaN(o)) { renderPriceLine(); return; }
      try {
        await fetch('/api/price/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({input_per_m: i, cached_per_m: c, output_per_m: o, cny_rate: isNaN(r) ? 0 : r})});
      } catch(e) {}
      loadUsage();
    };
  };
}

// 上游路由统计 + 最近回退明细（用于判断龙虾积分到底被谁花掉了）
function renderRoute(rt, fbs, since) {
  const set = (id, v) => { const el = $(id); if (el) el.textContent = v; };
  set('rt-ok', rt.relay_ok || 0);
  set('rt-fail', rt.relay_fail || 0);
  set('rt-pool', rt.pool_direct || 0);
  // 回退：去向动态统计（可能回退到任意反代，不再固定龙虾池）
  const dest = rt.fallback_dest || {};
  const names = { lobster: '龙虾账号池', auto: '自动' };
  ((chanData && chanData.channels) || []).forEach(c => { names[c.id] = c.name; });
  const destTotal = Object.keys(dest).reduce((s, k) => s + (dest[k] || 0), 0);
  set('rt-fb', destTotal);
  const target = (chanData && chanData.fallback_target) || '';
  let tname = '已关闭';
  if (target === 'lobster') tname = '龙虾账号池';
  else if (target === 'auto') tname = '自动';
  else if (target) tname = names[target] || target;
  const lbl = $('rt-fb-label');
  if (lbl) lbl.textContent = '回退次数 → ' + tname;
  const destEl = $('route-dest');
  if (destEl) {
    const parts = Object.keys(dest).filter(k => dest[k] > 0)
      .sort((a, b) => dest[b] - dest[a])
      .map(k => (names[k] || k) + ' ' + dest[k]);
    destEl.textContent = parts.length ? ('回退去向：' + parts.join(' · ')) : '';
  }
  const sinceEl = $('route-since');
  if (sinceEl) {
    const t = String(since || '');
    const ok = t.length > 10 && t.slice(0, 4) !== '0001';
    sinceEl.textContent = ok ? '统计起始 ' + t.slice(0, 10) + ' ' + t.slice(11, 19) + ' · 写盘 route.json，重启不清零' : '写盘 route.json，重启不清零';
  }
  const list = $('fb-list');
  if (!list) return;
  if (!fbs.length) {
    list.innerHTML = '<div class="daily-item"><span class="d">没有回退记录</span></div>';
    return;
  }
  list.innerHTML = fbs.slice().reverse().map(f => {
    const t = String(f.at || '');
    return '<div class="daily-item"><span class="d">' + t.slice(11, 19) + '</span>'
      + '<span>' + escHtml(f.model) + ' · ' + escHtml(f.cause) + '</span></div>';
  }).join('');
}

// 按天明细
// 按上游来源的用量明细：池子 + 每个反代（含已删除渠道留下的历史）
function renderSources(total, today) {
  const el = $('src-list');
  if (!el) return;
  const chs = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []);
  const rows = [];
  const mk = function(id, name) {
    const t = today[id] || {}, a = total[id] || {};
    rows.push({
      name: name,
      tr: t.requests || 0, tt: t.total_tokens || 0,
      ar: a.requests || 0,  at: a.total_tokens || 0,
    });
  };
  mk('lobster', '龙虾账号池');
  chs.forEach(function(c) { mk(c.id, c.name + '（反代）'); });
  // 已经删掉的渠道：历史数据还在，列出来但标清楚
  Object.keys(total).forEach(function(k) {
    if (k === 'lobster') return;
    if (chs.some(function(c) { return c.id === k; })) return;
    mk(k, k + '（已删除渠道，历史数据）');
  });
  el.innerHTML = rows.map(function(r) {
    return '<div class="daily-item"><span>' + escHtml(r.name) + '</span>'
      + '<span class="d">今日 <b>' + fmtNum(r.tr) + '</b> 单 · <b>' + fmtNum(r.tt) + '</b> token　｜　累计 <b>'
      + fmtNum(r.ar) + '</b> 单 · <b>' + fmtNum(r.at) + '</b> token</span></div>';
  }).join('');
}

function renderDaily(daily, todayKey) {
  const keys = Object.keys(daily).sort().reverse();
  if (!keys.length) {
    $('daily-list').innerHTML = '<div class="daily-item"><span class="d">暂无数据</span></div>';
    return;
  }
  $('daily-list').innerHTML = keys.slice(0, 60).map(k => {
    const v = daily[k] || {};
    const hit = v.prompt_tokens ? Math.round((v.cache_hit_tokens || 0) / v.prompt_tokens * 100) : 0;
    return '<div class="daily-item' + (k === todayKey ? ' today' : '') + '">'
      + '<span class="d">' + k + (k === todayKey ? ' 今天' : '') + '</span>'
      + '<span>' + fmtNum(v.total_tokens) + ' token · 命中 ' + hit + '% · ' + fmtNum(v.requests) + ' 次 · ' + moneyOf(v) + '</span>'
      + '</div>';
  }).join('');
}

async function loadStatus(forceCredits) {
  try {
    const d = await fetchJSON('/api/status');
    if (!d) {
      // 主服务可能正在重启（重启窗口内后端是空的）→ 别报错，等它起来
      // 文案改准：不是"每次添加都重启"，只是"这一下没响应"
      setConn('后端暂时没响应…（自动重试）', '#b45309');
      if (!window.__retryTimer) {
        window.__retryTimer = setTimeout(() => { window.__retryTimer = null; loadStatus(); }, 2500);
      }
      return;
    }
    const accs = d.accounts || [];
    // 实时积分（带缓存，forceCredits=true 才强制回源），失败时退回服务缓存值
    let live = {};
    try {
      const ld = await fetchJSON('/api/credits' + (forceCredits ? '?force=1' : ''));
      if (!ld) throw new Error('empty');
      live = ld.credits || {};
      if (ld.cached_at) lastCreditsAt = new Date(ld.cached_at).getTime();
    } catch(e) {}
    setConn('后端在线 · ' + (accs.length ? accs.length + ' 个账号' : '0 个账号'), '#15803d');
    let total = 0;
    accs.forEach(a => {
      const hasLive = Object.prototype.hasOwnProperty.call(live, a.uid);
      total += hasLive ? live[a.uid] : (a.credits || 0);
    });
    poolStatus = { accs: accs, live: live, total: total };
    // 积分卡：按下拉里选的来源渲染（默认 = 池子总积分）
    creditPool = { total: total, at: lastCreditsAt };
    renderCreditCard(false);
    // 结构没变就只改积分数字，不重建整块 DOM —— 30 秒轮询时页面不会"闪一下"
    const sig = JSON.stringify(accs.map(a => [a.uid, a.disabled, a.cooling, a.until, a.reason, a.err_count, a.inflight, a.note]));
    if (sig === lastAccsSig) updateCreditCells();
    else { lastAccsSig = sig; renderAccounts(); }
  } catch(e) {
    setConn('后端离线: ' + e.message, '#dc2626');
  }
}

function setConn(text, color) {
  const a = $('conn'), b = $('conn-side');
  if (a) { a.textContent = text; a.style.color = color; }
  if (b) { b.textContent = text; b.style.color = color; }
}

// 安全取 JSON：后端重启窗口内会返回空 body，这里返回 null 而不是抛错
async function fetchJSON(url, opts) {
  try {
    const r = await fetch(url, opts);
    const t = await r.text();
    if (!t) return null;
    return JSON.parse(t);
  } catch(e) {
    return null;
  }
}

// 添加账号后的界面刷新。
//
// 2026-09-22 实测修（用户："为什么添加账号老是重启啊"）：后端早就是**热生效**了
// （面板返回里自己就写着"已热生效（未重启）"），但这段前端代码每次都会：
//   ① 先挂一句"账号已添加，后端重启中…"
//   ② 拿 /api/healthz 当探针 —— 那个接口返回的是**纯文本 ok**，
//      fetchJSON 里 JSON.parse 直接抛错 → 返回 null → 那句 d 为真就 break 永远不成立
//   ③ 于是白等满 40 次（40 秒）；实测点了按钮要卡 26 秒才走出"重启中"
// 现在：直接问一次 /api/status（它是正经 JSON），在就立刻刷新；真没响应才进重试分支。
async function waitBackendThenReload() {
  if (await fetchJSON('/api/status')) {
    setConn('账号已添加，已热生效', '#15803d');
    loadStatus();
    loadModels();
    loadKeys();
    return;
  }
  // 真没响应（主服务正在重启的窗口）→ 才挂重试提示，最多等 20 秒
  setConn('后端暂时没响应，正在重试…', '#b45309');
  for (let i = 0; i < 20; i++) {
    await new Promise(res => setTimeout(res, 1000));
    if (await fetchJSON('/api/status')) break;
  }
  loadStatus();
  loadModels();
  loadKeys();
  loadUsage();
  loadChannels();
}

// 账号区：跟着「默认上游」切换 —— 选龙虾/自动显示龙虾账号列表，选反代显示该反代的详情
let poolStatus = null;
let chanData = null;
let lastAccsSig = ''; // 上一次渲染账号列表时的"结构指纹"，用于避免无谓重建 DOM

// coolRemain 把冷却截止时间格式化成「剩 2m30s」；无有效时间/已过期返回空串。
// 后端 until 是零值 "0001-01-01T00:00:00Z" 表示没在冷却，这里会算出负数直接返回空。
function coolRemain(until) {
  if (!until) return '';
  const t = new Date(until).getTime();
  if (!t || isNaN(t)) return '';
  const s = Math.ceil((t - Date.now()) / 1000);
  if (s <= 0) return '';
  if (s >= 86400) return Math.floor(s / 86400) + 'd';
  if (s >= 3600) return Math.floor(s / 3600) + 'h' + Math.floor((s % 3600) / 60) + 'm';
  if (s >= 60) return Math.floor(s / 60) + 'm' + (s % 60) + 's';
  return s + 's';
}

// 只把积分数字刷新一遍（不重建 DOM）。30 秒轮询里账号结构没变时走这条，
// 避免整块列表被 innerHTML 重写导致的"闪一下"。
function updateCreditCells() {
  if (!poolStatus) return;
  const box = $('accounts');
  if (!box) return;
  poolStatus.accs.forEach(a => {
    const cell = box.querySelector('.acc[data-uid="' + a.uid + '"] .acc-credits');
    if (!cell) return;
    const hasLive = Object.prototype.hasOwnProperty.call(poolStatus.live, a.uid);
    const c = hasLive ? poolStatus.live[a.uid] : (a.credits || 0);
    cell.textContent = '积分 ' + String(Math.round(c * 100) / 100) + (hasLive ? '' : '*');
  });
}

// 访问 IP：谁在用电脑、谁在用手机访问这个面板（同一台路由器后面也分开列）
async function loadVisitors() {
  const box = $('vis-list');
  if (!box) return;
  const d = await fetchJSON('/api/visitors');
  if (!d || !d.visitors) { box.innerHTML = '<div class="err">加载失败</div>'; return; }
  // 概览页那一排 KPI 里的「访问 IP」：数字 = 不同 IP 数，鼠标悬停看设备明细
  const visKpi = $('rt-vis');
  if (visKpi) visKpi.textContent = String(typeof d.distinct_ips === 'number' ? d.distinct_ips : (d.total || 0));
  const visCell = $('rt-vis-cell');
  if (visCell) {
    const dc = d.devices || {};
    const parts = Object.keys(dc).map(k => k + ' ' + dc[k]);
    visCell.title = '不同 IP ' + (d.distinct_ips || 0) + ' 个 · 设备记录 ' + (d.total || 0) + ' 条'
      + (parts.length ? '（' + parts.join(' · ') + '）' : '');
  }
  const you = d.you || {};
  const youEl = $('vis-you');
  if (youEl) {
    youEl.innerHTML = '当前设备：<b>' + escHtml(you.device || '?') + '</b> · IP <b>' + escHtml(you.ip || '?')
      + '</b> · 已记录 ' + (d.total || 0) + ' 条设备记录';
  }
  if (!d.visitors.length) { box.innerHTML = '<div class="meta">还没有记录</div>'; return; }
  box.innerHTML = d.visitors.map(v => {
    const icon = v.device === '手机' ? '📱' : (v.device === '平板' ? '📱' : '💻');
    const scope = v.local ? '<span class="pill gray">本机/内网</span>' : '<span class="pill blue">公网</span>';
    const me = v.you ? ' <span class="pill green">当前设备</span>' : '';
    const ban = v.blocked ? ' <span class="pill red">已拉黑</span>' : '';
    const meta = [v.os, v.browser].filter(Boolean).join(' · ') || '未识别';
    const act = v.blocked
      ? '<button class="gray mini vis-unban" data-ip="' + escHtml(v.ip) + '">解禁</button>'
      : '<button class="gray mini vis-ban" data-ip="' + escHtml(v.ip) + '" style="color:#b45309">禁用</button>';
    return '<div class="acc" data-vip="' + escHtml(v.ip) + '" data-vua="' + escHtml(v.ua) + '"><div>'
      + '<div class="name">' + icon + ' ' + escHtml(v.device || '未知') + ' · ' + escHtml(v.ip || '?') + me + ' ' + scope + ban + '</div>'
      + '<div class="uid">' + escHtml(meta) + ' · 共 ' + v.count + ' 次 · 最后 ' + escHtml(relTime(v.last_at)) + '（' + escHtml(v.last_path || '/') + '）</div>'
      + '<div class="uid" style="color:#9ca3af">首次 ' + escHtml(new Date(v.first_at).toLocaleString()) + '</div>'
      + '<div class="uid" style="color:#9ca3af;word-break:break-all;max-width:880px">UA: ' + escHtml(String(v.ua || '').slice(0, 170)) + '</div>'
      + '</div>'
      + act
      + '<button class="del-x vis-del" title="删掉这条访问记录">删</button></div>';
  }).join('');
  bindVisitorBtns();
  // 最近访问明细（打开页面才记；API 轮询不记）
  const logEl = $('vis-log');
  if (logEl) {
    const lg = d.log || [];
    if (!lg.length) {
      logEl.innerHTML = '<div class="meta">还没有明细</div>';
    } else {
      const pad = n => String(n).padStart(2, '0');
      logEl.innerHTML = lg.map(e => {
        const icon = e.device === '手机' ? '📱' : (e.device === '平板' ? '📱' : '💻');
        const t = new Date(e.at);
        const ts = pad(t.getMonth() + 1) + '-' + pad(t.getDate()) + ' ' + pad(t.getHours()) + ':' + pad(t.getMinutes()) + ':' + pad(t.getSeconds());
        return '<div style="display:flex;gap:10px;padding:4px 0;border-bottom:1px solid #f6f7f9;font-size:12px">'
          + '<span style="color:#6b7280;font-variant-numeric:tabular-nums">' + ts + '</span>'
          + '<span>' + icon + '</span>'
          + '<span style="color:#111827">' + escHtml(e.ip || '?') + '</span>'
          + '<span style="color:#9ca3af">' + escHtml(e.device || '') + '</span>'
          + '<span style="color:#9ca3af">' + escHtml(e.path || '/') + '</span></div>';
      }).join('');
    }
  }
}

// 访问 IP 页的按钮：禁用 / 解禁 / 删除
function bindVisitorBtns() {
  const box = $('vis-list');
  const msg = $('vis-msg');
  if (!box) return;
  const say = html => { if (msg) msg.innerHTML = html; };
  const post = async (url, body) => {
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      return await r.json();
    } catch(e) { return { ok: false, error: e.message }; }
  };
  box.querySelectorAll('.vis-ban').forEach(b => b.onclick = () => {
    const ip = b.dataset.ip;
    showConfirm('', async () => {
      const d = await post('/api/visitors/block', { ip: ip, on: true });
      if (!d.ok) say('<span class="err">' + escHtml(d.error || '操作失败') + '</span>');
      else say('<span class="msg-assistant">已拉黑 ' + escHtml(ip) + '</span>');
      loadVisitors();
    }, {
      title: '拉黑这个 IP？', okText: '拉黑',
      html: '<b>' + escHtml(ip) + '</b> 之后访问本面板一律 403。'
        + '<div style="margin-top:8px;color:#6b7280">· 你自己当前用的 IP 不允许拉黑（会被拦下）<br>'
        + '· 要解禁：在这一页点「解禁」；要是把自己关外面了，去服务器删掉 <code>data/panel-blocked.json</code> 里那条</div>',
    });
  });
  box.querySelectorAll('.vis-unban').forEach(b => b.onclick = async () => {
    const d = await post('/api/visitors/block', { ip: b.dataset.ip, on: false });
    say(d.ok ? '<span class="msg-assistant">已解禁 ' + escHtml(b.dataset.ip) + '</span>' : '<span class="err">' + escHtml(d.error || '操作失败') + '</span>');
    loadVisitors();
  });
  box.querySelectorAll('.vis-del').forEach(b => b.onclick = () => {
    const row = b.closest('.acc');
    const ip = row ? row.dataset.vip : '', ua = row ? row.dataset.vua : '';
    showConfirm('', async () => {
      const d = await post('/api/visitors/delete', { ip: ip, ua: ua });
      say(d.ok ? '<span class="msg-assistant">已删掉 ' + escHtml(ip) + ' 的这条记录</span>' : '<span class="err">删除失败</span>');
      loadVisitors();
    }, {
      title: '删掉这条访问记录？', okText: '删除',
      html: '清掉 <b>' + escHtml(ip) + '</b> 这一条历史（只是清记录，不会解禁）。'
        + '<div style="margin-top:8px;color:#6b7280">它下次再来还会出现在列表里；如果已被拉黑会标「已拉黑」。</div>',
    });
  });
}

function renderAccounts() {
  // 正在拖卡片时不要重渲染（30 秒轮询撞上拖动会把被拖的 DOM 换掉，拖到一半就没了）
  if (window.__dragging) return;
  const box = $('accounts');
  const cur = (chanData && chanData.default) || '';
  const note = $('up-note');
  if (note) {
    if (cur && cur !== 'lobster') {
      const cn = ((chanData.channels || []).find(x => x.id === cur) || {}).name || cur;
      const fb = (chanData && chanData.fallback_target) || '';
      let fbName = '未知';
      if (fb === 'lobster') fbName = '龙虾账号池';
      else if (fb === 'auto') fbName = '自动';
      else if (fb) {
        const fc = ((chanData.channels || []).find(x => x.id === fb) || {});
        fbName = fc.name || fb;
      }
      if (fb === 'lobster') {
        note.innerHTML = '<span style="color:#b45309">当前默认上游 = 反代「' + escHtml(cn) + '」（优先：只接管它自己声明/映射得上的模型，其余照旧走接管规则和龙虾账号池）；⚠ 反代失败会回退龙虾账号池（消耗积分），去「账号池」页把「失败回退」点成「关」即可</span>';
      } else if (fb === '') {
        note.innerHTML = '<span style="color:#15803d">当前默认上游 = 反代「' + escHtml(cn) + '」（优先：只接管它自己声明/映射得上的模型，其余照旧走接管规则和龙虾账号池）；已关闭回退，反代失败直接报错，不消耗龙虾积分</span>';
      } else {
        note.innerHTML = '<span style="color:#15803d">当前默认上游 = 反代「' + escHtml(cn) + '」（优先：只接管它自己声明/映射得上的模型，其余照旧走接管规则和龙虾账号池）；反代失败会回退到「' + escHtml(fbName) + '」</span>';
      }
    } else if (cur === 'lobster') {
      // 2026-09-22 改准：以前这句只说"全部请求走账号池"，没点明"反代只在账号池失败时才兜底"，
      // 用户加了反代却感觉"没效果"就是被这句误导的。
      note.innerHTML = '<span style="color:#b45309">当前默认上游 = 龙虾账号池：请求先走账号池，<b>反代只在账号池失败时才兜底</b>。想让某个反代优先用，'
        + '要么把它上面的卡片拖到「龙虾账号池」前面（改优先级），要么客户端模型名写成 <code>渠道名/模型</code>。</span>';
    } else {
      note.innerHTML = '当前默认上游 = 自动：按下面「优先级」顺序挑<b>第一个接管该模型的</b>（账号池也在这个顺序里，谁排前面谁先试）。'
        + '账号池排第一 = 反代基本不会被用到；想把反代用起来就把它拖到账号池上面。';
    }
  }
  const chans = (chanData && chanData.channels) || [];
  // 顺序表（含 "lobster" = 账号池的位置）：卡片按它渲染，拖动改的也是它
  const order = (chanData && chanData.order) || chans.map(c => c.id).concat(['lobster']);
  const poolIdx = order.indexOf('lobster');
  const htmlOf = {};

  // ① 龙虾账号池（永远显示，也参与优先级排序）
  if (poolStatus) {
    const accs = poolStatus.accs, live = poolStatus.live;
    const sel = cur === 'lobster';
    const coolingCount = accs.filter(a => a.cooling || a.disabled).length;
    // 池子级状态汇总：在线 / 冷却 / 禁用 各几个
    const nAlive = accs.filter(a => !a.cooling && !a.disabled).length;
    const nCool = accs.filter(a => a.cooling && !a.disabled).length;
    const isFrz = a => !!a.disabled && /冻结/.test(String(a.reason || ''));
    const nFrz = accs.filter(isFrz).length;
    const nDis = accs.filter(a => a.disabled && !isFrz(a)).length;
    const sumPills = (nAlive ? '<span class="pill green">在线 ' + nAlive + '</span>' : '')
                   + (nCool ? ' <span class="pill yellow">冷却 ' + nCool + '</span>' : '')
                   + (nFrz ? ' <span class="pill gray">已冻结 ' + nFrz + '</span>' : '')
                   + (nDis ? ' <span class="pill red">禁用 ' + nDis + '</span>' : '');
    let ph = '<div class="up-block' + (sel ? ' sel' : '') + '" data-oid="lobster">'
      + '<div class="up-head"><div><span class="drag-handle" title="按住拖动 = 改优先级">⠿</span><span class="mv-btn no-drag" data-dir="up" title="上移一位（手机上拖不动，用这个调优先级）">↑</span><span class="mv-btn no-drag" data-dir="down" title="下移一位">↓</span><span class="up-fold" title="收起/展开账号列表">▾</span><span class="ch-name">龙虾账号池</span>'
      + (sel ? ' <span class="pill blue">默认上游</span>' : '')
      + (poolIdx >= 0 ? ' <span class="pill blue" title="路由优先级：数字小的先试。账号池也在这条顺序里，拖到前面就先走池">优先级 ' + (poolIdx + 1) + '</span>' : '')
      + '<span class="ch-line-inline">' + accs.length + ' 个账号 · <span class="pool-credits-label">总积分</span> <span class="pool-credits">' + (Math.round((poolStatus.total || 0) * 100) / 100) + '</span></span>'
      + ' ' + sumPills + '</div>'
      + '<div class="ch-btns">'
      + (coolingCount ? '<button class="gray mini pool-reset" title="一键解除所有账号的冷却/禁用" style="color:#b45309">清除冷却(' + coolingCount + ')</button>' : '')
      + '<button class="gray mini pool-keepalive" title="立刻刷新所有账号的 token（不必等 22:00 定时任务）">立即续期</button>'
      + '<button class="gray mini pool-checkin" title="立刻给所有账号领今天的「每日积分礼」（每个号每天 100 积分，重复点是安全的）">立即签到</button>'
      + '<button class="gray mini up-setdef" data-id="lobster">设为默认</button>'
      // 2026-09-22 §112 用户要求：龙虾这一行也要有个「删」，样子跟其它三个反代一样。
      // 账号池是内置的、整行删不掉（后端会强制把它留在路由顺序里），所以这里的「删」
      // = 清空账号池：把池里所有账号删掉（账号级单个删在那行右边的「删」）。
      + '<button class="del-x pool-del-all" title="清空账号池：把池里所有账号连登录态一起删掉（不可恢复）">删</button></div></div>';
    // 2026-09-25 用户要求：账号按「账号自己的时间」排先后。
    // 时间来自后端 /api/status 的 added_at = 账号首次登录时间（auth.firstKeyfrom），
    // 拿不到才退回池子的首次记录。老账号也一样有，所以这里排的是真时间。
    // 时间缺失时才退回 uid 升序，保证顺序稳定。
    const uidNum = a => { const n = Number(a.uid); return isFinite(n) ? n : 0; };
    const poolAccs = accs.slice().sort((x, y) => {
      const tx = Date.parse(x.added_at || '') || 0, ty = Date.parse(y.added_at || '') || 0;
      if (tx !== ty) return tx - ty;
      const nx = uidNum(x), ny = uidNum(y);
      if (nx !== ny) return nx - ny;
      return String(x.uid || '').localeCompare(String(y.uid || ''));
    });
    ph += '<div class="acc-list">' + (poolAccs.length ? poolAccs.map((a, ai) => {
      const hasLive = Object.prototype.hasOwnProperty.call(live, a.uid);
      const c = hasLive ? live[a.uid] : (a.credits || 0);
      // ① 状态：在线 / 冷却中（带剩余时间）/ 已禁用（带上游给的原因）
      let stTxt = '在线', stCls = 'green';
      // 冻结（手动）和禁用（上游封号）都是 disabled 状态，靠 reason 区分显示
      const isFrozen = !!a.disabled && /冻结/.test(String(a.reason || ''));
      if (a.disabled) { stTxt = isFrozen ? '已冻结' : '已禁用'; stCls = isFrozen ? 'gray' : 'red'; }
      else if (a.cooling) { stTxt = '冷却中'; stCls = 'yellow'; }
      if (a.cooling) { const r = coolRemain(a.until); if (r) stTxt += ' · 剩 ' + r; }
      const stTitle = a.reason ? a.reason
                    : (a.disabled ? '已被上游禁用' : (a.cooling ? '临时冷却中，稍后自动恢复' : '可正常参与选号'));
      const st = '<span class="pill ' + stCls + '" title="' + escHtml(stTitle) + '">' + escHtml(stTxt) + '</span>';
      // ② 反代/上游归属：这些号都来自本机账号池
      const up = '<span class="pill gray" title="该账号所属上游">龙虾池</span>';
      // ③ 附加状态小字：错误次数 / 正在处理中的请求数（都只在非 0 时显示）
      const bits = [];
      if (a.err_count > 0) bits.push('错误 ' + a.err_count);
      if (a.inflight > 0) bits.push('并发 ' + a.inflight);
      // 今日用量：单数 / token / 消耗积分（服务端按账号记账，跨天自动清零）
      if (a.today_requests) {
        bits.push('今日 ' + a.today_requests + ' 单');
        bits.push(fmtNum(a.today_tokens || 0) + ' token');
        bits.push('消耗 ' + (a.today_burn || 0) + ' 分');
      }
      // 邀请进度（2026-09-26 用户要求：每个号"被邀请/已邀请"几人要看得见）
      // 口径 = 上游 /api/invitation/progress 的 invitedCount。字段没来（还没刷到）就不显示，
      // 免得把"未知"显示成"0 人"。
      if (typeof a.invited_count === 'number') {
        bits.push('邀请 ' + a.invited_count + ' 人');
      }
      const extra = bits.join(' · ');
      const inviteTip = (typeof a.invited_count === 'number' || a.invite_code)
        ? ('邀请码 ' + (a.invite_code || '?') + '　已邀请 ' + (a.invited_count || 0) + ' 人'
           + (a.invite_stage1 ? '（一档 ' + a.invite_stage1 + ' 人'
              + (a.invite_stage1_done ? '，已达成' : '，还差 ' + Math.max(0, a.invite_stage1 - (a.invited_count || 0)) + ' 人') + '）' : '')
           + (a.invite_reward ? '　邀请累计奖励 ' + Math.round(a.invite_reward) + ' 分' : ''))
        : '';
      const addedTxt = a.added_at ? String(a.added_at).replace('T', ' ').slice(0, 16) : '';
      const seqTitle = addedTxt
        ? '按账号的时间排：第 ' + (ai+1) + ' 个（首次登录 ' + addedTxt + '）'
        : '按账号的时间排：第 ' + (ai+1) + ' 个（没记到首次登录时间，按 uid 兜底）';
      return '<div class="acc" data-uid="' + (a.uid||'') + '"><div><div class="name"><span class="acc-seq" title="' + escHtml(seqTitle) + '">#' + (ai+1) + '</span>' + (a.nickname||'?') + '</div>'
           + '<div class="uid">uid ' + (a.uid||'?')
             + (addedTxt ? '<span title="首次登录时间 —— 账号自己的时间，这行的排序就是按它">　· 首次登录 ' + addedTxt.slice(5) + '</span>' : '')
             + noteHtml(a) + '</div>'
           + (extra ? '<div class="uid" style="color:#6b7280"' + (inviteTip ? ' title="' + escHtml(inviteTip) + '"' : '') + '>' + escHtml(extra) + '</div>' : '')
           + (a.last_err ? '<div class="uid" style="color:#b45309;max-width:460px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + escHtml(a.last_err) + '">' + String(a.last_err_at || '').slice(11, 19) + ' 最后错误: ' + escHtml(a.last_err) + '</div>' : '')
           + '</div>'
            // 龙虾账号这一格是「积分」（原来是个光秃秃的数字，容易跟"余额"混）；加个标签
             + '<div class="acc-credits cred-blue">积分 ' + (Math.round(c*100)/100) + (hasLive ? '' : '*') + '</div>'
           + up + st
           // 每个账号都能单独 冻结/解冻（用户要求 2026-09-22）：
           //   冻结 = 从选号里排除（账号、登录态、备注全留着），解冻 = 清冷却+解冻+清错误计数
           + ((a.cooling || a.disabled)
               ? '<button class="gray mini acc-thaw" data-uid="' + (a.uid||'') + '" data-on="0" title="解冻：清掉冷却/冻结，让它重新参与选号">解冻</button>'
               : '<button class="gray mini acc-freeze" data-uid="' + (a.uid||'') + '" data-on="1" title="冻结：把它从选号里排除（账号和登录态都留着，随时能解冻）">冻结</button>')
           + '<button class="del-x" title="删除账号" onclick="delAcc(' + (a.uid||'') + ')">删</button></div>';
    }).join('') : '<div class="err">无账号，请先添加</div>') + '</div>';
    ph += '</div>';
    htmlOf['lobster'] = ph;
  } else {
    htmlOf['lobster'] = '<div class="meta">加载中...</div>';
  }

  // ② 所有反代渠道（全部显示，选中的高亮）
  chans.forEach(c => {
    const oi = order.indexOf(c.id);
    htmlOf[c.id] = channelCardHtml(c, cur === c.id, oi >= 0 ? oi : 0);
  });

  // ③ 按顺序拼起来（顺序表里没列到的也补上，防止漏显示）
  let html = '';
  order.forEach(id => { if (htmlOf[id]) { html += htmlOf[id]; delete htmlOf[id]; } });
  Object.keys(htmlOf).forEach(id => { html += htmlOf[id]; });
  box.innerHTML = html;
  bindChannelCard();
  bindDragOrder(box);
  bindNotes();
  applyPoolFold(box);
  // 账号池页上的反代卡片也要补「后台账号」那一行（同一个渠道在 #accounts 和 #ch-list 各渲染一份）
  fillChanAccounts();
}

// 区块折叠（账号池 + 每个反代卡片都支持）：状态存 localStorage，刷新/切页后保持
function applyPoolFold(root) {
  if (!root) return;
  root.querySelectorAll('.up-block').forEach(blk => {
    const fold = blk.querySelector('.up-fold');
    if (!fold) return;
    // 账号池的三角没有 data-id（用固定 key）；反代卡片用渠道 id 各自单独记
    const id = fold.dataset.id || '';
    const key = id ? ('lb2a-fold-' + id) : 'lb2a-pool-folded';
    try {
      if (localStorage.getItem(key) === '1') blk.classList.add('collapsed');
    } catch(e) {}
    fold.onclick = (ev) => {
      ev.stopPropagation();
      blk.classList.toggle('collapsed');
      try {
        localStorage.setItem(key, blk.classList.contains('collapsed') ? '1' : '0');
      } catch(e) {}
    };
  });
}

// 反代渠道备注
function chNoteHtml(c) {
  const n = c.note || '';
  return '<span class="note ch-note' + (n ? '' : ' empty') + '" data-id="' + c.id + '" data-note="' + escHtml(n) + '" title="点一下改备注（回车保存，清空即删除）">'
    + (n ? escHtml(n) : '＋备注') + '</span>';
}

// 账号备注：点一下就能改，回车保存
function noteHtml(a) {
  const n = a.note || '';
  return '<span class="note' + (n ? '' : ' empty') + '" data-uid="' + escHtml(a.uid || '') + '" data-note="' + escHtml(n) + '" title="点一下改备注（回车保存，清空即删除）">'
    + (n ? escHtml(n) : '＋备注') + '</span>';
}

function bindNotes() {
  const box = $('accounts');
  if (!box) return;
  // 只绑账号备注（渠道备注由 bindChannelCard 负责，别互相覆盖）
  box.querySelectorAll('.note:not(.ch-note)').forEach(el => el.onclick = () => {
    const uid = el.dataset.uid;
    const inp = document.createElement('input');
    inp.className = 'note-input';
    inp.value = el.dataset.note || '';
    inp.placeholder = '备注（回车保存，清空即删除）';
    el.replaceWith(inp);
    inp.focus();
    let done = false;
    const save = async () => {
      if (done) return;
      done = true;
      try {
        await fetch('/api/notes/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({uid: uid, note: inp.value.trim()})});
      } catch(e) {}
      loadStatus();
    };
    inp.onkeydown = e => {
      if (e.key === 'Enter') { e.preventDefault(); save(); }
      if (e.key === 'Escape') { done = true; loadStatus(); }
    };
    inp.onblur = save;
  });
}

// 选中的是反代时，账号区展示这个反代本身
function channelCardHtml(c, selected, idx) {
  const pill = !c.enabled ? '<span class="pill gray">已停用</span>'
    : (c.ok ? '<span class="pill green">正常</span>' : '<span class="pill yellow">未验证</span>');
  // 上游归属 + 接管规模：一眼看出这个反代挂了几个模型、是不是通配
  const nModels = (c.models || []).length;
  const isAll = nModels === 1 && c.models[0] === '*';
  const seenMs = (c.seen_models || []).filter(Boolean);
  const scope = isAll ? '<span class="pill blue">接管全部模型</span>'
              : (nModels ? '<span class="pill gray">接管 ' + nModels + ' 个模型</span>'
                         : (seenMs.length ? '<span class="pill gray">未接管 · 实测可用 ' + seenMs.length + ' 个</span>'
                                          : '<span class="pill gray">未接管模型</span>'));
  // 没填「接管模型」的反代，退而用最近一次「测试」实测到的模型展示 —— 保证它不会在面板里消失
  const usingSeen = !nModels && !isAll && seenMs.length > 0;
  const ms = nModels
    ? c.models.map(m => '<span class="model-tag">' + escHtml(m) + '</span>').join('')
    : (usingSeen
        ? seenMs.map(m => '<span class="model-tag" style="border-style:dashed" title="测试实测，还没接管">' + escHtml(m) + '</span>').join('')
        : '<span class="model-tag">未接管任何模型</span>');
  // 一键把实测模型写成接管
  const takeAllBtn = usingSeen
    ? '<button class="gray mini ch2-takeall" data-id="' + c.id + '" title="把实测到的 ' + seenMs.length + ' 个模型全部写入「接管模型」">全部接管</button>'
    : '';
    // data-oid = 这条在路由顺序表里的 ID，拖动排序靠它对齐服务端的 order
    return '<div class="up-block' + (selected ? ' sel' : '') + '" data-oid="' + c.id + '">'
      + '<div class="up-head">'
      + '<div><span class="drag-handle" title="按住拖动 = 改优先级">⠿</span><span class="mv-btn no-drag" data-dir="up" title="上移一位（手机上拖不动，用这个调优先级）">↑</span><span class="mv-btn no-drag" data-dir="down" title="下移一位">↓</span><span class="up-fold" data-id="' + c.id + '" title="收起/展开">▾</span><span class="ch-name">' + escHtml(c.name) + '</span>'
      + (selected ? ' <span class="pill blue">默认上游</span>' : '')
      + ' ' + pill + ' <span class="pill gray">外部反代</span> ' + scope
      // 配了后台登录态的：表头挂一个「余额 $95」小牌，卡片收起时也看得见
      + (c.has_login ? ' <span class="pill gray ch-acctpill" data-id="' + c.id + '">余额 ...</span>' : '')
      // 优先级：多个渠道都接管同一个模型时按这个顺序选（数字小的先试）
      + (typeof idx === 'number' ? ' <span class="pill blue" title="路由优先级：数字小的先试。多个渠道都接管同一个模型时按这个顺序选；反代失败后兜底扫渠道也用这个顺序。用右边 ↑↓ 调">优先级 ' + (idx + 1) + '</span>' : '')
      + chNoteHtml(c) + '</div>'
      + '<div class="ch-btns">'
      + '<button class="gray mini up-setdef" data-id="' + c.id + '">设为默认</button>'
      + '<button class="gray mini ch2-test" data-id="' + c.id + '">测试</button>'
      // 后台登录：贴上游后台的 access_token（或用户名密码），用来读「账户余额」——
      // 面板积分卡切到这个反代时就能显示真余额，而不是只能看"已用多少"
      // 按钮文字固定「后台登录」（用户要求：登过之后也别变成"后台已登录"，那反而让人以为点不动了）
      + '<button class="gray mini ch2-login" data-id="' + c.id + '" title="点开弹窗 → 粘登录态（可加多个后台账号，一个号一行）。已配 ' + (c.logins_count || 0) + ' 个">后台登录</button>'
      // 一键同步令牌：用登录态替每个后台账号建/认出一把 sk-，自动填进那一行（配够 2 把就开始 A 模式轮询）
      + (c.has_login ? '<button class="gray mini ch2-synckeys" data-id="' + c.id + '" title="用登录态替每个后台账号自动建一把 sk- 令牌并自动填好（新建的令牌名 blueapi-auto，可去它后台删）">同步令牌</button>' : '')
      + takeAllBtn
      // 停用/启用：反代欠费、key 失效时一键摘出路由，不用删（删了模型接管记录也一起没了）
      + '<button class="gray mini ch2-toggle" data-id="' + c.id + '" data-on="' + (c.enabled ? '0' : '1') + '" title="' + (c.enabled ? '停用后不再参与路由（保留配置，随时能启用回来）' : '启用后重新参与路由') + '">' + (c.enabled ? '停用' : '启用') + '</button>'
      + '<button class="gray mini ch2-edit" data-id="' + c.id + '">编辑</button>'
      + '<button class="del-x ch2-del" data-id="' + c.id + '" data-name="' + escHtml(c.name) + '">删</button></div>'
      + '</div>'
      + '<div class="ch-body">'
      + '<div class="ch-line">' + escHtml(c.base_url) + ' &nbsp;·&nbsp; Token ' + escHtml(c.api_key) + '</div>'
      // 配了「后台登录态」的反代：把它的后台账号按「龙虾账号池」那种行样式列出来
      // （名字 / 明细 / 余额 / 状态），异步补内容，见 fillChanAccounts()
      + (c.has_login
          ? '<div class="acc-list ch-acctbox" data-id="' + c.id + '">'
            + '<div class="acc"><div><div class="name">账号 读取中...</div>'
            + '<div class="uid">后台账号 · 登录态已配</div></div>'
            + '<div class="acc-credits">--</div><span class="pill gray">' + escHtml(c.name) + '</span>'
            + '<span class="pill gray">读取中</span></div></div>'
          : '')
      + '<div class="ch-line">' + ms + ' <span style="color:#9ca3af">· ' + escHtml(c.last_test || '未测试') + '</span></div>'
      // 模型映射：客户端发来的名字 → 该上游真正认的名字（配了才显示，方便一眼看出"名字对不上"的问题）
      + ((c.model_map && Object.keys(c.model_map).length)
          ? '<div class="ch-line" style="color:#2563eb; word-break:break-all">模型映射 '
            + Object.keys(c.model_map).length + ' 条：'
            + escHtml(Object.keys(c.model_map).map(function(k) { return k + ' → ' + c.model_map[k]; }).join('；'))
            + '</div>'
          : '')
      + (usingSeen ? '<div class="ch-line" style="color:#9ca3af">虚线框 = 测试实测到、但还没接管；点「全部接管」把它们写进接管列表（客户端发这些模型名时才会优先走它）</div>' : '')
      + '</div>'
      + '</div>';
}

// 左侧导航「随意拖动」排序：点了还是切页面，按住拖到别的位置松手就换序，顺序存 localStorage（刷新也记住）
function bindNavDrag() {
  const nav = document.querySelector('.sidebar nav');
  if (!nav) return;
  const KEY = 'lb2a.navOrder';
  // ① 先按上次存下来的顺序重排（存的是 data-page 列表）
  try {
    const saved = JSON.parse(localStorage.getItem(KEY) || '[]');
    if (saved && saved.length) {
      saved.forEach(function(p) {
        const el = nav.querySelector('.nav-item[data-page="' + p + '"]');
        if (el) nav.appendChild(el);
      });
    }
  } catch(e) {}
  const save = function() {
    try {
      localStorage.setItem(KEY, JSON.stringify(Array.prototype.slice.call(nav.querySelectorAll('.nav-item')).map(function(x) { return x.dataset.page; })));
    } catch(e) {}
  };
  let dragEl = null;
  nav.querySelectorAll('.nav-item').forEach(function(el) {
    el.setAttribute('draggable', 'true');
    if (!el.title) el.title = '点一下切换页面；按住拖到别的位置松手 = 调整顺序（会记住）';
    el.addEventListener('dragstart', function(e) {
      dragEl = el;
      el.classList.add('dragging');
      try { e.dataTransfer.setData('text/plain', el.dataset.page); e.dataTransfer.effectAllowed = 'move'; } catch(err) {}
    });
    el.addEventListener('dragend', function() {
      el.classList.remove('dragging');
      if (dragEl) { dragEl = null; save(); }
    });
    el.addEventListener('dragover', function(e) {
      if (!dragEl || dragEl === el) return;
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const after = (e.clientY - r.top) > r.height / 2;
      nav.insertBefore(dragEl, after ? el.nextSibling : el);
    });
    el.addEventListener('drop', function(e) { e.preventDefault(); });
  });
}

// ===== 渠道卡片上新增的按钮：全局函数 + 事件委托 =====
// 为什么不直接 forEach 绑 onclick：卡片是整块 innerHTML 重建的，绑定有概率被踩掉
// （用户反馈「点一下没效果」）。委托到 document 一次，重建多少次都不会丢。

// 后台登录：给反代配「后台登录态」，用来读它的账户余额（积分卡切到它时显示真余额）
// 让用户在上游后台页面 Console 里跑的一行：把登录态打包复制到剪贴板。
// 为什么要打包（2026-09-22 实测 tierflow.cn）：它的前端是自定义 React，
// axios withCredentials + TF-User 头，**根本没有 access_token**；
// 标准 new-api 后台才把 access_token 放在 localStorage.user 里。两种都带上最省事。
const TOKEN_SNIPPET = "copy(JSON.stringify({user:localStorage.getItem('user'),uid:localStorage.getItem('uid'),cookie:document.cookie}))";

window.chanLoginClick = function(id) {
  const c = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id);
  if (!c) { setChanMsg('<span class="err">找不到这个反代（面板数据可能刚刷新，再点一次）</span>'); return; }
  const inp = 'width:100%;margin-top:6px;background:#ffffff;border:1px solid #d1d5db;border-radius:8px;color:#111827;padding:9px 12px;font-size:13px';
  const html = '<div style="font-size:13px;line-height:1.75;text-align:left">'
    + (c.has_login
        ? '<div style="color:#15803d;margin-bottom:8px">已配 ' + (c.logins_count || 1) + ' 个后台账号'
          + ((c.login_names && c.login_names.length) ? ('（' + c.login_names.map(escHtml).join('、') + '）') : '')
          + '　—— 再登一次会<b>新增一个</b>；同一个号会覆盖旧的</div>'
        : '')
    + '<div>目标反代：<b>' + escHtml(c.name) + '</b>　<code>' + escHtml(c.base_url) + '</code></div>'
    + '<input id="cl-token" placeholder="登录态：粘你从它后台拿到的凭据（Cookie / access_token 都行）" title="不知道去哪拿？在它后台页面上按 F12 → Console，粘这行回车（自动复制）：' + escHtml(TOKEN_SNIPPET) + '" style="' + inp + '">'
    + '<div class="hint" style="margin-top:10px">或者填后台的用户名 / 密码，让服务器替你去登一次（该站开了验证码的话会失败）：</div>'
    + '<input id="cl-user" placeholder="用户名 / 邮箱" style="' + inp + '">'
    + '<input id="cl-pass" type="password" placeholder="密码" style="' + inp + '">'
    + (c.has_login ? '<div style="margin-top:10px"><button class="gray mini" id="cl-clear" type="button">清空全部后台账号</button></div>' : '')
    + '</div>';
  showConfirm('', async () => {
    const token = $('cl-token') ? $('cl-token').value.trim() : '';
    const u = $('cl-user') ? $('cl-user').value.trim() : '';
    const pw = $('cl-pass') ? $('cl-pass').value : '';
    if (!token && (!u || !pw)) {
      // 反馈直接弹回弹窗，别丢到页面底部（用户看不到就会以为"没效果"）
      showResult('没填内容 · ' + c.name, '<div style="font-size:13px;line-height:1.8;text-align:left">'
        + 'access_token 或者 用户名+密码，至少填一组。<br>再点一次「后台登录」重试。</div>');
      return;
    }
    let d = null;
    try {
      const r = await fetch('/api/channelslogin', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id: c.id, token: token, username: u, password: pw}),
      });
      d = await r.json();
    } catch(e) {
      showResult('后台登录失败 · ' + c.name, '<div style="color:#b91c1c;text-align:left">请求出错：' + escHtml(e.message) + '</div>');
      return;
    }
    creditChan = null;
    loadChannels();
    if (d && d.ok) {
      showResult('后台登录成功 · ' + c.name, '<div style="font-size:13px;line-height:1.9;text-align:left">'
        + '<div style="color:#15803d;font-weight:700">' + escHtml(d.detail || '验证通过') + '</div>'
        + '<div>' + escHtml((d.balance && d.balance.detail) || '') + '</div>'
        + (d.balance && d.balance.auth ? '<div class="hint" style="margin-top:4px">（登录态方式：' + escHtml(d.balance.auth) + '）</div>' : '')
        + '<div class="hint" style="margin-top:8px">继续用 relay-login 抓别的号，粘进来就多一行；卡片上会一个号一行、各自带删按钮。</div></div>');
    } else {
      const msg = (d && ((d.error && (d.error.message || d.error.code)) || d.error)) || '未知错误';
      showResult('后台登录失败 · ' + c.name, '<div style="font-size:13px;line-height:1.9;text-align:left">'
        + '<div style="color:#b91c1c">' + escHtml(msg) + '</div>'
        + '<div class="hint" style="margin-top:8px">直接粘贴浏览器里拿的 <code>access_token</code> 最稳。</div></div>');
    }
  }, { title: '上游后台登录 · ' + c.name, okText: '保存并验证', html: html });
  const clr = $('cl-clear');
  if (clr) clr.onclick = async () => {
    try {
      const r = await fetch('/api/channelslogin', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id: c.id, clear: true}),
      });
      const d = await r.json();
      setChanMsg('<span class="msg-assistant">' + escHtml(d.detail || '已清除') + '</span>');
    } catch(e) { setChanMsg('<span class="err">' + escHtml(e.message) + '</span>'); }
    $('cm-cancel').click();
    creditChan = null;
    loadChannels();
  };
};

// 停用 / 启用
window.chanToggleClick = async function(id, btn) {
  const on = btn.dataset.on === '1';
  const c = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id);
  btn.disabled = true;
  try {
    const r = await fetch('/api/channels/toggle', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: id, enabled: on}),
    });
    const dd = await r.json();
    setChanMsg(dd.ok
      ? '<span class="msg-assistant">「' + escHtml(c ? c.name : id) + '」已' + (on ? '启用' : '停用') + (on ? '' : '（不再参与路由，配置和接管模型都留着）') + '</span>'
      : '<span class="err">操作失败</span>');
  } catch(e) {
    setChanMsg('<span class="err">' + escHtml(e.message) + '</span>');
  }
  btn.disabled = false;
  loadChannels();
};

// 一键同步令牌：用登录态替每个后台账号建/认出一把 sk-，自动填好（配够 2 把就开始 A 模式轮询）
window.chanSyncKeys = async function(id, btn) {
  btn.disabled = true;
  const old = btn.textContent;
  btn.textContent = '同步中...';
  let d = null;
  try {
    const r = await fetch('/api/channelslogin', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: id, op: 'synckeys'}),
    });
    d = await r.json();
  } catch(e) {
    btn.disabled = false; btn.textContent = old;
    showResult('同步失败', '<div style="color:#b91c1c">' + escHtml(e.message) + '</div>');
    return;
  }
  btn.disabled = false; btn.textContent = old;
  creditChan = null;
  loadChannels();
  const notes = (d && d.notes && d.notes.length)
    ? '<div class="hint" style="margin-top:8px">' + d.notes.map(escHtml).join('<br>') + '</div>'
    : '';
  showResult(d && d.ok ? '令牌同步完成' : '同步失败',
    '<div style="font-size:13px;line-height:1.8;text-align:left">'
    + '<div>' + escHtml((d && (d.detail || d.error)) || '') + '</div>'
    + '<div class="hint" style="margin-top:6px">新建的令牌名字是 <code>blueapi-auto</code>（在这个反代后台的「令牌」页能看到，随时可删）。</div>'
    + notes + '</div>');
};

// 给某个后台账号配它自己的 sk- 令牌（配了就参与 A 模式轮询）
window.chanAcctSetKey = function(id, index) {
  if (!(index >= 0)) { setChanMsg('<span class="err">这条账号没有下标，刷新页面再试</span>'); return; }
  const c = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id) || { id: id, name: '反代' };
  const inp = 'width:100%;margin-top:6px;background:#ffffff;border:1px solid #d1d5db;border-radius:8px;color:#111827;padding:9px 12px;font-size:13px';
  const html = '<div style="font-size:13px;line-height:1.75;text-align:left">'
    + '<div>给 <b>' + escHtml(c.name) + '</b> 的第 <b>' + (index + 1) + '</b> 个后台账号，配它自己的 <code>sk-</code> 令牌。</div>'
    + '<div class="hint" style="margin-top:8px">在哪建：登录该反代后台 →「令牌 / API Keys」→ 新建 → 复制 <code>sk-...</code> 粘到下面。<br>'
    + '配完的效果：这条渠道的请求会在<b>所有配了令牌的号之间轮流打</b>（A 模式，一起出力）。<br>'
    + '留空保存 = 取消这条的令牌（它就不参与转发，只用来显示余额）。</div>'
    + '<input id="ck-key" placeholder="sk-..." style="' + inp + '">'
    + '</div>';
  showConfirm('', async () => {
    const v = $('ck-key') ? $('ck-key').value.trim() : '';
    let d = null;
    try {
      const r = await fetch('/api/channelslogin', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id: id, op: 'setkey', index: index, api_key: v}),
      });
      d = await r.json();
    } catch(e) {
      showResult('保存失败', '<div style="color:#b91c1c">' + escHtml(e.message) + '</div>');
      return;
    }
    creditChan = null;
    loadChannels();
    showResult(d && d.ok ? '已保存' : '保存失败',
      '<div style="font-size:13px;line-height:1.8;text-align:left">'
      + escHtml((d && (d.detail || (d.error && (d.error.message || d.error.code)) || d.error)) || '') + '</div>');
  }, { title: '配转发令牌 · ' + escHtml(c.name || ''), okText: '保存', html: html });
};

// 冻结 / 解冻某个后台账号（2026-09-22 用户要求：反代里每个账号也要能单独冻）
//   冻结 = 这条不参与转发轮询（后端 relay.Channel.AccountKeys 会跳过它）
//   跟「删」的区别：登录态、令牌、备注全留着，解冻就回来
window.chanAcctFreeze = async function(id, index, on) {
  if (!(index >= 0)) { setChanMsg('<span class="err">这条账号没有下标，刷新页面再试</span>'); return; }
  const c = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id) || { id: id, name: '反代' };
  const doIt = async () => {
    let d = null;
    try {
      const r = await fetch('/api/channelslogin', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id: id, op: 'freeze', index: index, on: on}),
      });
      d = await r.json();
    } catch(e) { setChanMsg('<span class="err">' + escHtml(e.message) + '</span>'); return; }
    creditChan = null;
    loadChannels();
    setChanMsg('<span class="' + ((d && d.ok) ? 'msg-assistant' : 'err') + '">'
      + escHtml((d && (d.detail || d.error)) || '操作失败') + '</span>');
  };
  if (!on) { await doIt(); return; }
  showConfirm('', doIt, {
    title: '冻结这个后台账号？',
    okText: '冻结',
    html: '反代 <b>' + escHtml(c.name) + '</b> 的第 <b>' + (index + 1) + '</b> 个后台账号。'
      + '<div style="margin-top:8px">冻结后它<b>不参与转发轮询</b>；登录态、令牌、备注全都留着，随时点「解冻」就回来。</div>',
  });
};

// 删掉一个后台账号（只删这条登录态，不影响转发用的 API Key）
window.chanAcctDel = async function(id, index, btn) {
  if (!(index >= 0)) { setChanMsg('<span class="err">这条账号没有下标，刷新页面再试</span>'); return; }
  btn.disabled = true;
  try {
    const r = await fetch('/api/channelslogin', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: id, op: 'remove', index: index}),
    });
    const d = await r.json();
    setChanMsg(d.ok
      ? '<span class="msg-assistant">' + escHtml(d.detail || '已删除') + '</span>'
      : '<span class="err">' + escHtml((d.error && (d.error.message || d.error.code)) || d.error || '删除失败') + '</span>');
  } catch(e) {
    setChanMsg('<span class="err">' + escHtml(e.message) + '</span>');
  }
  btn.disabled = false;
  loadChannels();
};

// 一键接管：把最近一次测试实测到的模型全部写进「接管模型」
window.chanTakeAllClick = async function(id, btn) {
  const c = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id);
  if (!c) return;
  const ms = (c.seen_models || []).filter(Boolean);
  if (!ms.length) { setChanMsg('<span class="err">这个反代还没有实测模型，先点「测试」</span>'); return; }
  btn.disabled = true; btn.textContent = '接管中...';
  try {
    const r = await fetch('/api/channels/update', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: c.id, models: ms}),
    });
    const dd = await r.json();
    setChanMsg(dd.ok
      ? '<span class="msg-assistant">「' + escHtml(c.name) + '」已接管 ' + ms.length + ' 个模型：' + escHtml(ms.join(', ')) + '</span>'
      : '<span class="err">接管失败：' + escHtml((dd.error && (dd.error.message || dd.error.code)) || dd.error || '') + '</span>');
  } catch(e) {
    setChanMsg('<span class="err">' + escHtml(e.message) + '</span>');
  }
  btn.disabled = false; btn.textContent = '全部接管';
  loadChannels();
};

// 委托注册一次即可（脚本只执行一次，这里再兜一层防重复）
(function() {
  if (window.__chanBtnsDelegated) return;
  window.__chanBtnsDelegated = true;
  document.addEventListener('click', function(e) {
    const t = e.target && e.target.closest ? e.target.closest('.ch2-login,.ch2-toggle,.ch2-takeall,.ch-acct-del,.ch-acct-key,.ch-acct-frz,.ch2-synckeys') : null;
    if (!t || t.disabled) return;
    e.preventDefault();
    const id = t.dataset.id;
    if (!id) return;
    if (t.classList.contains('ch2-login')) return window.chanLoginClick(id);
    if (t.classList.contains('ch2-toggle')) return window.chanToggleClick(id, t);
    if (t.classList.contains('ch2-takeall')) return window.chanTakeAllClick(id, t);
    if (t.classList.contains('ch-acct-del')) return window.chanAcctDel(id, parseInt(t.dataset.index, 10), t);
    if (t.classList.contains('ch-acct-key')) return window.chanAcctSetKey(id, parseInt(t.dataset.index, 10));
    if (t.classList.contains('ch-acct-frz')) return window.chanAcctFreeze(id, parseInt(t.dataset.index, 10), t.dataset.on === '1');
    if (t.classList.contains('ch2-synckeys')) return window.chanSyncKeys(id, t);
  });
})();

// 当前正在测试的渠道 id：测试按钮全局互斥，避免连点多个渠道同时打上游
let testingChanId = null;

// 渠道按钮绑定（账号池页 / 上游反代页共用同一套）
function bindChannelCard(root) {
  root = root || $('accounts');
  // 折叠三角也在这里兜一层：以前只有账号池页调了 applyPoolFold，
  // 「上游反代」页漏调 → 那边的小三角点不动。放在这里以后任何调用方都不会漏。
  applyPoolFold(root);
  // 清除冷却 / 禁用：会连「已禁用」一起解掉（其中可能有一堆平台封禁号），必须二次确认
  root.querySelectorAll('.pool-reset').forEach(b => b.onclick = () => {
    const accs = (poolStatus && poolStatus.accs) || [];
    const dis = accs.filter(a => a.disabled);
    const banned = dis.filter(a => isBannedAcc(a));
    let html = '会把池子里所有账号的<b>冷却</b>和<b>禁用</b>标记清掉（积分、token 不受影响）。';
    if (dis.length) html += '<div style="margin-top:8px">其中 <b>' + dis.length + '</b> 个当前是「已禁用」，解禁后会重新参与选号。</div>';
    if (banned.length) {
      html += '<div style="margin-top:10px;color:#b91c1c;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:10px">'
        + '⚠ 有 <b>' + banned.length + '</b> 个是<b>平台封禁号</b>：号本身已经不能用了，解禁只会让它们回池子、每次请求白撞一次再冷却。'
        + '<br>号还没恢复的话，建议先别解禁。</div>';
    }
    showConfirm('', async () => {
      try {
        const r = await fetch('/api/pool/reset', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
        const d = await r.json();
        setChanMsg('<span class="msg-assistant">已解除 ' + (d.cleared || 0) + ' 个账号的冷却/禁用</span>');
      } catch(e) { setChanMsg('<span class="err">' + escHtml(e.message) + '</span>'); }
      loadStatus(true);
    }, { title: '确认清除冷却 / 禁用？', okText: '全部解禁', html: html });
  });
  // 清空账号池（2026-09-22 §112：龙虾那一行的「删」，样子跟其它三个反代一样）
  //   账号池整行是内置的、删不掉（relay.normalizeOrderLocked 会强制把 lobster 留在顺序里），
  //   所以这里的「删」= 把池里所有账号删掉（连登录态/auths 文件一起）。删单个号点那一行右边的「删」。
  root.querySelectorAll('.pool-del-all').forEach(b => b.onclick = () => {
    const accs = ((poolStatus && poolStatus.accs) || []).slice();
    const n = accs.length;
    const online = accs.filter(a => !a.cooling && !a.disabled).length;
    if (!n) { setChanMsg('<span class="err">池子里没有账号</span>'); return; }
    const html = '会把池子里 <b>' + n + '</b> 个账号<b>全部删掉</b>（其中在线 ' + online + ' 个、冷却/禁用 ' + (n - online) + ' 个）：'
      + '<div style="margin-top:8px">· 账号配置 + 登录态一起删，服务器上的 <code>auths/*.json</code> 也删掉，<b>不可恢复</b></div>'
      + '<div>· Token（API Key）不受影响，客户端那边不用改</div>'
      + '<div style="margin-top:10px;color:#b91c1c">只想删单个号：点那一行右边的「删」。</div>';
    showConfirm('', async () => {
      let ok = 0, fail = 0;
      for (const a of accs) {
        try {
          const r = await fetch('/api/accounts?uid=' + encodeURIComponent(a.uid), {method:'DELETE'});
          const d = await r.json();
          if (d && d.ok) { ok++; } else { fail++; }
        } catch(e) { fail++; }
      }
      setChanMsg('<span class="' + (fail ? 'err' : 'msg-assistant') + '">清空账号池：删掉 ' + ok + ' 个'
        + (fail ? '，失败 ' + fail + ' 个' : '') + '</span>');
      loadStatus(true);
    }, { title: '清空账号池？', okText: '全部删除', html: html });
  });
  // 单个账号「冻结 / 解冻」（2026-09-22 用户要求：每个号都要有这两个按钮）
  //   冻结 → POST /api/pool/freeze {on:true}   从选号里排除，配置全留
  //   解冻 → POST /api/pool/freeze {on:false}  清冷却 + 解冻 + 清错误计数
  // 平台封禁号解冻前再确认一次（老行为保留）
  root.querySelectorAll('.acc-thaw, .acc-freeze').forEach(b => b.onclick = () => {
    const uid = b.dataset.uid;
    const acc = ((poolStatus && poolStatus.accs) || []).find(a => String(a.uid) === String(uid));
    const freezing = b.dataset.on === '1';
    const send = async () => {
      let d = null;
      try {
        const r = await fetch('/api/pool/freeze', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({uid: uid, on: freezing})});
        d = await r.json();
      } catch(e) { setChanMsg('<span class="err">' + escHtml(e.message) + '</span>'); return; }
      setChanMsg('<span class="' + ((d && d.ok) ? 'msg-assistant' : 'err') + '">' + escHtml((d && (d.detail || d.error)) || '操作失败') + '</span>');
      loadStatus(true);
    };
    if (freezing) {
      showConfirm('', send, {
        title: '冻结这个账号？',
        okText: '冻结',
        html: '账号 <b>' + escHtml((acc && acc.nickname) || uid) + '</b>（uid ' + escHtml(uid) + '）'
          + '<div style="margin-top:8px">冻结后它<b>不再参与选号</b>；账号、登录态、备注全都留着，随时点「解冻」就回来。</div>',
      });
      return;
    }
    const doReset = send;
    if (acc && acc.disabled && isBannedAcc(acc)) {
      showConfirm('', doReset, {
        title: '这是平台封禁号，确定解冻？',
        okText: '仍要解冻',
        html: '账号 <b>' + escHtml(acc.nickname || uid) + '</b>（uid ' + escHtml(uid) + '）'
          + '<div style="margin-top:6px">上游原因：<span style="color:#b91c1c">' + escHtml(acc.reason || '(未提供)') + '</span></div>'
          + '<div style="margin-top:8px">解冻后它会重新参与选号，大概率还是失败再冷却。号没恢复就建议直接「删」。</div>',
      });
      return;
    }
    doReset();
  });
  // 立即续期：逐个打上游 /api/auth/refresh，40 个号约 5-10 秒（慢接口，按钮期间置灰防连点）
  root.querySelectorAll('.pool-keepalive').forEach(b => b.onclick = async () => {
    const old = b.innerText;
    b.disabled = true;
    b.innerText = '续期中...';
    try {
      const r = await fetch('/api/keepalive', {method:'POST'});
      const d = await r.json();
      if (d.ok) {
        let s = 'token 续期完成：成功 ' + (d.refreshed || 0) + ' 个';
        if (d.failed) s += '，失败 ' + d.failed + ' 个';
        if (d.banned) s += '，其中封禁 ' + d.banned + ' 个已自动禁用';
        setChanMsg('<span class="msg-assistant">' + s + '</span>');
      } else {
        setChanMsg('<span class="err">' + escHtml(d.error || '续期失败') + '</span>');
      }
    } catch (e) {
      setChanMsg('<span class="err">' + escHtml(e.message) + '</span>');
    }
      b.innerText = old;
      b.disabled = false;
      loadStatus(true);
    });
    // 立即签到：每个账号每天能领 100 积分（上游按天记账，重复点是安全的）
    root.querySelectorAll('.pool-checkin').forEach(b => b.onclick = async () => {
      const old = b.innerText;
      b.disabled = true;
      b.innerText = '签到中...';
      try {
        const r = await fetch('/api/checkin', {method:'POST'});
        const d = await r.json();
        if (d.ok) {
          const ICON = { claimed: '✅', already: '⏭️', no_activity: '➖', failed: '❌', banned: '🚫' };
          const LABEL = { claimed: '领到', already: '今天已领', no_activity: '上游无活动', failed: '失败', banned: '已封禁' };
          const rows = d.rows || [];
          let html = '<div style="margin-bottom:10px">共 <b>' + (d.total || 0) + '</b> 个号 · 本次领到 <b>' + (d.claimed || 0) + '</b> 个 · 合计 <b style="color:#15803d">+' + (d.credits || 0) + ' 积分</b></div>';
          if (rows.length) {
            html += '<div style="max-height:320px;overflow:auto;border:1px solid #eef0f2;border-radius:8px">';
            rows.forEach(r => {
              const ok = r.status === 'claimed';
              html += '<div style="display:flex;justify-content:space-between;gap:12px;padding:6px 10px;border-bottom:1px solid #f3f4f6">'
                + '<span>' + (ICON[r.status] || '?') + ' ' + escHtml(r.nick || r.uid) + '</span>'
                + '<span style="color:' + (ok ? '#15803d' : '#6b7280') + ';white-space:nowrap">'
                + (ok ? '+' + r.credits : escHtml(LABEL[r.status] || r.status))
                + (r.error ? '（' + escHtml(String(r.error).slice(0, 40)) + '）' : '')
                + '</span></div>';
            });
            html += '</div>';
          } else {
            html += '<div style="color:#6b7280">没有可参与的账号。</div>';
          }
          html += '<div style="margin-top:10px;color:#6b7280">每个号每天 100 积分（有效期 30 天）；同一天重复领不会重复加。</div>';
          showResult('每日积分礼 · 领取结果', html);
        } else {
          showResult('签到失败', '<span style="color:#dc2626">' + escHtml(d.error || '未知错误') + '</span>');
        }
      } catch (e) {
        showResult('签到失败', '<span style="color:#dc2626">' + escHtml(e.message) + '</span>');
      }
      b.innerText = old;
      b.disabled = false;
      loadStatus(true);
    });
    root.querySelectorAll('.ch-note').forEach(el => el.onclick = () => {
    const id = el.dataset.id;
    const inp = document.createElement('input');
    inp.className = 'note-input';
    inp.value = el.dataset.note || '';
    inp.placeholder = '备注（回车保存，清空即删除）';
    el.replaceWith(inp);
    inp.focus();
    let done = false;
    const save = async () => {
      if (done) return;
      done = true;
      try {
        await fetch('/api/channels/update', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id: id, note: inp.value.trim()})});
      } catch(e) {}
      loadChannels();
    };
    inp.onkeydown = e => {
      if (e.key === 'Enter') { e.preventDefault(); save(); }
      if (e.key === 'Escape') { done = true; loadChannels(); }
    };
    inp.onblur = save;
  });
  root.querySelectorAll('.up-setdef').forEach(b => b.onclick = async () => {
    await fetch('/api/channels/default', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({default: b.dataset.id})});
    loadChannels();
  });
  root.querySelectorAll('.ch2-test').forEach(b => b.onclick = async () => {
    if (b.disabled) return;
    // 跨按钮互斥：同时只让一个渠道在测。测试=真打一次上游 + 真发一单，多张卡并发点会白烧额度
    if (testingChanId) {
      const cur = (((chanData && chanData.channels) || []).find(x => x.id === testingChanId) || {}).name || testingChanId;
      setChanMsg('<span class="err">正在测试「' + escHtml(cur) + '」，等它跑完再点下一个（测试会真打上游）</span>');
      return;
    }
    testingChanId = b.dataset.id;
    b.disabled = true; b.textContent = '测试中';
    try {
      const rr = await fetch('/api/channels/test', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id: b.dataset.id})});
      const dd = await rr.json();
      const cn = (((chanData && chanData.channels) || []).find(x => x.id === b.dataset.id) || {}).name || '渠道';
      // ① 按钮就地变状态（用户点哪儿就在哪儿看到结果）
      if (dd.ok) { b.textContent = '通过(' + dd.models + ')'; b.style.color = '#059669'; b.style.borderColor = '#a7f3d0'; }
      else { b.textContent = '失败'; b.style.color = '#dc2626'; b.style.borderColor = '#fecaca'; }
      // ② 卡片内就地提示（含失败原因，不用再往上翻）
      const card = b.closest('.up-block');
      if (card) {
        // 卡片折叠时自动展开，否则结果被藏起来看不见
        if (card.classList.contains('collapsed')) {
          card.classList.remove('collapsed');
          try { localStorage.setItem('lb2a-fold-' + b.dataset.id, '0'); } catch(e) {}
        }
        let tip = card.querySelector('.ch-test-tip');
        if (!tip) {
          tip = document.createElement('div');
          tip.className = 'ch-test-tip';
          tip.style.cssText = 'font-size:12px;margin-top:6px';
          (card.querySelector('.ch-body') || card).appendChild(tip);
        }
        tip.innerHTML = dd.ok
          ? '<span style="color:#059669">测试通过：' + dd.models + ' 个模型 —— ' + escHtml((dd.models_sample || []).slice(0, 6).join(', ')) + '</span>'
          : '<span style="color:#dc2626">测试失败：' + escHtml(String(dd.error || '').slice(0, 150)) + '</span>';
      }
      // ③ 顶部提示区也写一份（保持原有行为）
      setChanMsg(dd.ok
        ? '<span class="msg-assistant">' + escHtml(cn) + ' 测试通过，' + dd.models + ' 个模型</span>'
        : '<span class="err">' + escHtml(cn) + ' 测试失败：' + escHtml(String(dd.error || '').slice(0, 150)) + '</span>'
      );
      if (dd.ok) renderModelPick(dd.models_sample);
    } catch(e) {
      b.textContent = '失败'; b.style.color = '#dc2626';
      setChanMsg('<span class="err">' + escHtml(e.message) + '</span>');
    }
    testingChanId = null;
    b.disabled = false;
    // 不立刻 loadChannels：那会把上面这些就地反馈冲掉。切页/手动刷新时卡片状态自然会更新。
  });
  // 「后台登录」按钮走全局事件委托（见 window.chanLoginClick）——
  // 以前是直接 forEach 绑 onclick，卡片整块 innerHTML 重建时有可能把绑定踩掉，
  // 用户反馈过"点一下没效果"。委托到 document，重建多少次都不会丢。
  // 停用/启用、一键接管：都走全局事件委托（window.chanToggleClick / window.chanTakeAllClick），
  // 这里不再直接绑 onclick，避免和委托重复触发。
  root.querySelectorAll('.ch2-edit').forEach(b => b.onclick = () => {
    const c = ((chanData && chanData.channels) || []).find(x => x.id === b.dataset.id);
    if (!c) return;
    // 卡片在哪个页面，就填那个页面上的表单。
    // 修 bug（2026-09-21）：原来不管在哪个页都调 openRelayForm()，它填的是「账号池」页的
    // #pool-chan-form —— 人站在「上游反代」页点编辑，表单却在另一个（隐藏的）页面里展开，
    // 表现就是"点编辑没反应"。
    if (b.closest('#ch-list')) setEditMode(c);
    else openRelayForm(c);
  });
  root.querySelectorAll('.ch2-del').forEach(b => b.onclick = () => {
    showConfirm('确定删除反代「' + b.dataset.name + '」吗？删除后走它的模型会按「失败回退」设置处理。', async () => {
      await fetch('/api/channels/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id: b.dataset.id})});
      // 顺手清掉该渠道在前端的残留：折叠状态记忆（避免 localStorage 留孤儿 key）
      try { localStorage.removeItem('lb2a-fold-' + b.dataset.id); } catch(e) {}
      loadChannels();
    });
  });
}

// 提示写到当前页能看见的地方
function setChanMsg(html) {
  const a = $('up-warn'), b = $('ch-msg');
  if (a) a.innerHTML = html;
  if (b) b.innerHTML = html;
}

// 顺序项显示名：lobster → 龙虾账号池，渠道 → 渠道名
function orderLabel(id) {
  if (id === 'lobster') return '龙虾账号池';
  return (((chanData && chanData.channels) || []).find(c => c.id === id) || {}).name || id;
}

// 提交新的路由顺序（ids 里含 "lobster" = 账号池的位置）
async function submitOrder(ids) {
  try {
    const r = await fetch('/api/channels/reorder', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ids: ids})});
    const d = await r.json();
    if (d && d.ok) {
      setChanMsg('<span class="msg-assistant">优先级已保存（从上往下）：' + escHtml(ids.map(orderLabel).join(' → ')) + '</span>');
    } else {
      setChanMsg('<span class="err">调序失败：' + escHtml((d && (d.error || d.message)) || '未知') + '</span>');
    }
  } catch(e) { setChanMsg('<span class="err">' + escHtml(e.message) + '</span>'); }
  loadChannels();
}

// 拖动排序：把任意卡片（含龙虾账号池）拖到新位置，松手就按新顺序提交
function bindDragOrder(box) {
  const blocks = Array.prototype.slice.call(box.querySelectorAll('.up-block[data-oid]'));
  if (blocks.length < 2) return;
  let dragEl = null;
  // 鼠标按在按钮/输入框/折叠箭头 上时不启动拖动（否则点按钮会变成拖卡片）
  const NO_DRAG = 'input,button,textarea,select,a,.up-fold,.no-drag';
  if (!window.__dragResetBound) {
    window.__dragResetBound = true;
    document.addEventListener('mouseup', () => {
      document.querySelectorAll('.up-block.dragging').forEach(b => b.classList.remove('dragging'));
    });
  }
  blocks.forEach(el => {
    // 整张卡片都能拖（不要求抓那个小符号），按钮/输入框上按下不算
    el.setAttribute('draggable', 'true');
    let blocked = false;
    el.addEventListener('mousedown', e => {
      blocked = !!(e.target && e.target.closest && e.target.closest(NO_DRAG));
    });
    el.addEventListener('dragstart', e => {
      if (blocked) { e.preventDefault(); return; }
      dragEl = el;
      el.classList.add('dragging');
      window.__dragging = true;
      try { e.dataTransfer.setData('text/plain', el.dataset.oid); e.dataTransfer.effectAllowed = 'move'; } catch(err) {}
    });
    el.addEventListener('dragend', () => {
      el.classList.remove('dragging');
      window.__dragging = false;
      if (!dragEl) return;
      dragEl = null;
      const ids = Array.prototype.slice.call(box.querySelectorAll('.up-block[data-oid]')).map(x => x.dataset.oid);
      submitOrder(ids);
    });
    el.addEventListener('dragover', e => {
      if (!dragEl || dragEl === el) return;
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const after = (e.clientY - r.top) > r.height / 2;
      el.parentNode.insertBefore(dragEl, after ? el.nextSibling : el);
    });
    el.addEventListener('drop', e => { e.preventDefault(); });
  });
  // 2026-09-22 §113 手机端调序：HTML5 拖放（dragstart/dragover）在触摸屏上根本不触发，
  // 所以卡片头上补了两个 ⬆⬇（只在「账号池」页那份列表里显示，手机才看得到）。
  function moveCard(el, dir) {
    const all = Array.prototype.slice.call(box.querySelectorAll('.up-block[data-oid]'));
    const i = all.indexOf(el), j = i + dir;
    if (i < 0 || j < 0 || j >= all.length) return;
    if (dir < 0) { box.insertBefore(el, all[j]); } else { box.insertBefore(all[j], el); }
    const ids = Array.prototype.slice.call(box.querySelectorAll('.up-block[data-oid]')).map(x => x.dataset.oid);
    submitOrder(ids);
  }
  box.querySelectorAll('.mv-btn').forEach(b => {
    b.onclick = (e) => {
      e.stopPropagation();
      if (e.preventDefault) e.preventDefault();
      const el = b.closest('.up-block[data-oid]');
      if (el) moveCard(el, b.dataset.dir === 'up' ? -1 : 1);
    };
  });
}

// 反代卡片列表（账号池页与上游反代页共用，保证两边完全一致）
function renderChannelCards(into, cur) {
  const box = $(into);
  if (!box) return;
  const chans = (chanData && chanData.channels) || [];
  const order = (chanData && chanData.order) || chans.map(c => c.id).concat(['lobster']);
  box.innerHTML = chans.length
    ? chans.map(c => {
        const oi = order.indexOf(c.id);
        return channelCardHtml(c, cur === c.id, oi >= 0 ? oi : 0);
      }).join('')
    : '<div class="hint" style="margin:0">还没有反代渠道。填上面表单点「添加反代」即可。</div>';
  bindChannelCard(box);
  // 修 bug（2026-09-22）：这里原来只调了 bindChannelCard，漏了 applyPoolFold →
  // 「上游反代」页每张卡片左边那个小三角点不动（账号池页有调，所以那边是好的）。
  applyPoolFold(box);
  fillChanAccounts();
}

// 反代的「后台账号」：配了后台登录态的卡片，把它的后台账号按账号池那种行样式列出来。
// 跟账号池那边 updateCreditCells 一个套路：先渲染、再就地打补丁（内容没变就不碰 DOM，30 秒轮询不会闪）。
function fmtMoney(b) {
  const sym = (String((b && b.currency) || '').toUpperCase() === 'USD') ? '$' : '¥';
  const r2 = n => Math.round(n * 100) / 100;
  if (b && typeof b.total === 'number') return sym + r2(b.total);
  if (b && typeof b.remaining === 'number') return sym + r2(b.remaining);
  return '';
}

function chanAcctRowHtml(c, b, index, meta) {
  meta = meta || {};
  // 状态牌：跟龙虾账号行一样的排法 —— 先是来源牌（龙虾那边是「龙虾池」），再是状态牌
  const srcPill = '<span class="pill gray">' + escHtml((c && c.name) || '反代') + '</span>';
  // 每个后台账号一个「删」（只删这条登录态，不影响转发用的 API Key）
  const delBtn = '<button class="del-x ch-acct-del" data-id="' + escHtml(c.id) + '" data-index="' + index + '" title="删掉这个后台账号（只删登录态，不影响转发）">删</button>';
  // 配这个号自己的 sk- 令牌 → 参与 A 模式轮询
  const keyBtn = (index >= 0)
    ? '<button class="gray mini ch-acct-key" data-id="' + escHtml(c.id) + '" data-index="' + index + '" title="给这个后台账号配它自己的 sk- 令牌；配了就参与轮询（请求轮流用）">' + (meta.has_key ? '换key' : '配key') + '</button>'
    : '';
  // 冻结 / 解冻这个后台账号（用户要求 2026-09-22：反代里每个账号也要能单独冻）
  //   冻结 = 这条不参与转发轮询（AccountKeys 直接跳过它）；登录态、令牌、备注全留着
  const frzBtn = (index >= 0)
    ? (meta.frozen
        ? '<button class="gray mini ch-acct-frz" data-id="' + escHtml(c.id) + '" data-index="' + index + '" data-on="0" title="解冻：让它重新参与转发">解冻</button>'
        : '<button class="gray mini ch-acct-frz" data-id="' + escHtml(c.id) + '" data-index="' + index + '" data-on="1" title="冻结：这条不参与转发（配置都留着，随时能解冻）">冻结</button>')
    : '';
  const frzPill = meta.frozen ? '<span class="pill gray" title="已冻结：不参与转发，点「解冻」恢复">已冻结</span>' : '';
  if (!b || !b.ok) {
    const why = (b && b.detail) || '读取失败';
    return '<div class="acc"><div><div class="name">账号 拿不到</div>'
      + '<div class="uid" style="color:#b45309">' + escHtml(String(why).slice(0, 160)) + '</div></div>'
      + '<div class="acc-credits">--</div>' + srcPill + '<span class="pill yellow">异常</span>' + frzPill + keyBtn + frzBtn + delBtn + '</div>';
  }
  const today = (window.__srcToday && window.__srcToday[c.id]) || null;
  // 账号名优先用手机号（tierflow 这类站的后台就是这么显示账号的，跟龙虾账号池 187****5260 同观感）
  const name = b.phone || b.username || '（后台没返回账号名）';
  // 第 2 行：uid / 用户名 / 角色 / 状态 / 2FA / 分组 / 注册时间（跟账号池那种"uid xxx · 备注"一个位置）
  const line2 = [];
  if (b.uid) line2.push('uid ' + escHtml(b.uid));
  if (b.phone && b.username) line2.push(escHtml(b.username));
  if (b.email) line2.push('邮箱 ' + escHtml(b.email));
  if (typeof b.role === 'number') {
    const rn = { 1: '普通用户', 10: '管理员', 100: '超级管理员' }[b.role] || ('角色 ' + b.role);
    line2.push(rn);
  }
  if (typeof b.status === 'number') line2.push(b.status === 1 ? '状态正常' : ('状态 ' + b.status + '（异常）'));
  line2.push('2FA ' + (b.twofa_enabled ? '已开' : '关闭'));
  if (b.group) line2.push('分组 ' + escHtml(b.group));
  if (b.created_time) {
    const d2 = new Date(b.created_time * 1000);
    if (!isNaN(d2.getTime())) line2.push('注册 ' + d2.toISOString().slice(0, 10));
  }
  // 第 3 行：用量 + 后台情况
  const line3 = [];
  if (typeof b.used === 'number') line3.push('已用 ' + fmtMoney({ currency: b.currency, total: b.used }));
  if (today) line3.push('今日 <b>' + fmtNum(today.requests || 0) + '</b> 单 · <b>' + fmtNum(today.total_tokens || 0) + '</b> token');
  if (typeof b.requests === 'number' && b.requests > 0) line3.push('后台请求 ' + b.requests + ' 次');
  if (b.auth) line3.push('登录态 ' + escHtml(b.auth));
  // 这号自己的转发令牌（配了才参与轮询）
  line3.push(meta.has_key
    ? ('令牌 <b>' + escHtml(meta.key_mask || 'sk-...') + '</b> 参与轮询')
    : '未配令牌（不参与转发）');
  const money = fmtMoney(b);
  // 状态牌：后台 status=1 且在线上 → 绿「在线」；status 异常 → 黄；余额读到了但状态怪 → 也标出来
  let statePill;
  if (typeof b.status === 'number' && b.status !== 1) statePill = '<span class="pill yellow">状态 ' + b.status + '</span>';
  else if (money) statePill = '<span class="pill green">在线</span>';
  else statePill = '<span class="pill gray">已连上</span>';
  return '<div class="acc">'
    + '<div><div class="name">' + escHtml(name) + '</div>'
    + '<div class="uid">' + (line2.join(' · ') || '后台账号') + '</div>'
    + (line3.length ? '<div class="uid" style="color:#6b7280">' + line3.join(' · ') + '</div>' : '')
    + '</div>'
    + '<div class="acc-credits">' + (money || '--') + '</div>'
    + srcPill + statePill + frzPill + keyBtn + frzBtn + delBtn
    + '</div>';
}

function chanAcctSig(c, b) {
  return JSON.stringify([
    b ? b.ok : null, b ? b.username : null, b ? b.phone : null, b ? b.uid : null,
    b ? b.total : null, b ? b.used : null, b ? b.requests : null, b ? b.status : null,
    b ? b.role : null, b ? b.group : null, b ? b.created_time : null, b ? b.twofa_enabled : null,
    (window.__srcToday && window.__srcToday[c.id]) || null,
  ]);
}

// 一个反代下可能挂着很多个后台账号 → 每个账号一行（跟龙虾账号池一样的列表形态）
function chanAcctRowsHtml(c, accounts) {
  if (!accounts || !accounts.length) {
    return chanAcctRowHtml(c, null, -1);
  }
  return accounts.map(function(a, i) {
    const b = a && a.balance ? a.balance : a;
    const idx = (a && typeof a.index === 'number') ? a.index : i;
    return chanAcctRowHtml(c, b, idx, {
      has_key: !!(a && a.has_key), key_mask: (a && a.key_mask) || '', frozen: !!(a && a.frozen),
    });
  }).join('');
}

function chanAcctListSig(c, accounts) {
  return JSON.stringify((accounts || []).map(a => {
    const b = (a && a.balance) ? a.balance : a;
    return chanAcctSig(c, b) + '|' + ((a && a.index) || 0) + '|' + (a && a.has_key ? 1 : 0) + '|' + ((a && a.key_mask) || '')
      + '|' + (a && a.frozen ? 1 : 0); // 冻结状态变了也要重绘（否则点了冻结界面不刷新）
  }));
}

// 每次都拉一次（后端 60 秒缓存，不会真打上游），但**内容没变就不碰 DOM** —— 30 秒轮询不会闪。
async function fillChanAccounts() {
  const boxes = Array.prototype.slice.call(document.querySelectorAll('.ch-acctbox[data-id]'));
  for (const box of boxes) {
    const id = box.dataset.id;
    const ch = ((typeof chanData !== 'undefined' && chanData && chanData.channels) || []).find(x => x.id === id) || { id: id };
    try {
      const d = await fetchJSON('/api/channelbalance?id=' + encodeURIComponent(id));
      const list = (d && d.accounts && d.accounts.length) ? d.accounts : ((d && d.balance) ? [{ index: 0, balance: d.balance }] : []);
      const sig = chanAcctListSig(ch, list);
      if (box.dataset.sig === sig) continue;
      box.dataset.sig = sig;
      box.innerHTML = chanAcctRowsHtml(ch, list);
      // 同一个渠道在「账号池」和「上游反代」两页各渲染了一份 → 两处的表头小牌都要更新
      const okList = list.filter(a => ((a.balance) ? a.balance.ok : a.ok));
      let txt;
      if (!list.length) txt = '余额 未配置';
      else if (!okList.length) txt = '余额 拿不到';
      else {
        // 多个账号就显示合计（同币种才相加，否则显示"N 个账号"）
        const cur = (okList[0].balance || okList[0]).currency;
        const sameCur = okList.every(a => ((a.balance || a).currency === cur));
        if (sameCur) {
          const sum = okList.reduce((s, a) => { const b = a.balance || a; return s + (typeof b.total === 'number' ? b.total : (typeof b.remaining === 'number' ? b.remaining : 0)); }, 0);
          const sym = (String(cur || '').toUpperCase() === 'USD') ? '$' : '¥';
          txt = '余额 ' + sym + (Math.round(sum * 100) / 100) + (list.length > 1 ? ('（' + list.length + ' 个号）') : '');
        } else {
          txt = list.length + ' 个后台账号';
        }
      }
      document.querySelectorAll('.ch-acctpill[data-id="' + id + '"]').forEach(el => el.textContent = txt);
      // 有几个令牌在参与轮询（A 模式）→ 挪到悬停提示里，不占表头
      const keyN = list.filter(a => a && a.has_key).length;
      document.querySelectorAll('.ch-acctpill[data-id="' + id + '"]').forEach(el => {
        el.title = keyN > 0
          ? ('A 模式轮询中：' + keyN + ' 个令牌轮流用（请求依次打到各号上）')
          : ('还没配转发令牌 —— 现在只用渠道卡片上那一个 key；给每个号「配key」后才会轮询');
      });
    } catch (e) {
      if (box.dataset.sig === 'err') continue;
      box.dataset.sig = 'err';
      box.innerHTML = '<div class="acc"><div><div class="name">账号 读取失败</div></div>'
        + '<div class="acc-credits">--</div><span class="pill gray">' + escHtml((ch && ch.name) || '反代') + '</span>'
        + '<span class="pill yellow">异常</span></div>';
      document.querySelectorAll('.ch-acctpill[data-id="' + id + '"]').forEach(el => el.textContent = '余额 拿不到');
    }
  }
}

async function loadModels() {
  try {
    const d = await fetchJSON('/api/models');
    if (!d) return;
    const m = (d.data||[]).map(x => x.id);
    window.__allModelIds = m;
    // 账号池自己的模型表（不含反代接管的）：模型测试按它把「龙虾账号池」那一组跟反代分组分开，
    // 这样同一个模型名只会在一个组里出现，分组数量 = 账号池页优先级表的行数。
    try {
      const pd = await fetchJSON('/api/poolmodels');
      window.__poolModelIds = (pd && pd.data) ? pd.data.map(x => x.id) : null;
    } catch(e) { window.__poolModelIds = null; }
    // 带斜杠的 id（渠道名/模型 点名写法、上游原生斜杠名）一律不上标签墙 —— 2026-09-22 用户要求彻底删掉
    const shownIds = m.filter(function(x) { return String(x).indexOf('/') < 0; });
    $('modelcount').textContent = shownIds.length;
    $('models').innerHTML = shownIds.map(x => '<span class="model-tag pick-model" data-m="' + escHtml(x) + '" title="点一下填进下拉框">' + escHtml(x) + '</span>').join('') || '无';
    // 点模型名 → 直接选中上面的下拉框
    $('models').querySelectorAll('.pick-model').forEach(t => t.onclick = () => {
      const sel = $('model');
      if (!sel) return;
      if (!Array.prototype.some.call(sel.options, o => o.value === t.dataset.m)) {
        const op = document.createElement('option');
        op.value = t.dataset.m;
        op.textContent = t.dataset.m;
        sel.appendChild(op);
      }
      sel.value = t.dataset.m;
      const p = $('prompt');
      if (p) p.focus();
    });
    // 模型测试下拉 + 「接管模型」下拉：都按来源分组
    window.__allModelIds = m;
    fillTestModelSelect();
    fillModelPickers();
  } catch(e) {
    $('models').innerHTML = '<span class="err">模型列表加载失败</span>';
  }
}

// 组装「按来源分组」的 option/optgroup HTML
// 渠道对外可见的模型 = 接管声明 ∪ 最近一次「测试」实测到的（与服务端 channelModelList 同口径）
function chanModelList(c) {
  const out = [], seen = {};
  ((c && c.models) || []).concat((c && c.seen_models) || []).forEach(function(m) {
    m = String(m || '').trim();
    if (!m || m === '*') return;
    const k = m.toLowerCase();
    if (seen[k]) return;
    seen[k] = true;
    out.push(m);
  });
  return out;
}

//   allIds       : /api/models 返回的完整 id 列表
function buildModelOptionsHtml(allIds) {
  const ids = allIds || [];
  let html = '';
  const chs = (typeof chanData !== 'undefined' && chanData && chanData.channels) || [];
  const byId = {};
  chs.forEach(function(c) { byId[c.id] = c; });
  // 顺序表 = 面板「账号池」页那张优先级表（含 lobster = 账号池的位置）。
  // 下面严格按它的顺序、一个一个地生成分组，序号也对齐 —— 账号池页有几行，这里就有几组，一个都不漏。
  const order = (typeof chanData !== 'undefined' && chanData && chanData.order)
    || chs.map(function(c) { return c.id; }).concat(['lobster']);
  const optOf = function(x) { return '<option value="' + escHtml(x) + '">' + escHtml(x) + '</option>'; };
  const poolIds = (window.__poolModelIds && window.__poolModelIds.length)
    ? window.__poolModelIds
    : ids.filter(function(x) { return x.indexOf('/') < 0; });
  const shown = {};
  const markShown = function(list) { (list || []).forEach(function(x) { shown[String(x).toLowerCase()] = true; }); };
  order.forEach(function(id, i) {
    const no = i + 1;
    if (id === 'lobster') {
      if (!poolIds.length) return;
      markShown(poolIds);
      html += '<optgroup label="' + no + '. 龙虾账号池（优先 ' + no + '）">'
        + poolIds.map(optOf).join('') + '</optgroup>';
      return;
    }
    const c = byId[id];
    if (!c) return;
    const ms = chanModelList(c);
    markShown(ms);
    if (!ms.length) {
      // 没声明、也没测试过的反代也要出现在列表里 —— 否则用户会以为"我加的反代不见了"
      html += '<optgroup label="' + no + '. ' + escHtml(c.name) + '（反代 · 未声明模型）">'
        + '<option value="" disabled>点「上游反代」页的「测试」按钮获取它的模型</option>'
        + '</optgroup>';
      return;
    }
    // 标签只写渠道名，不加"已停用"之类的后缀（用户要求：别加这个）
    html += '<optgroup label="' + no + '. ' + escHtml(c.name) + '（反代）">'
      + ms.map(optOf).join('') + '</optgroup>';
  });
  // 兜底：优先级表里没有的渠道也照列（正常不会有，但保证"一个都不漏"）
  chs.forEach(function(c) {
    if (order.indexOf(c.id) >= 0) return;
    const ms = chanModelList(c);
    markShown(ms);
    html += '<optgroup label="' + escHtml(c.name) + '（反代 · 不在优先级表里）">'
      + (ms.length ? ms.map(optOf).join('')
                   : '<option value="" disabled>未声明模型，点「测试」获取</option>')
      + '</optgroup>';
  });
  // 还剩下的（既不在账号池动态表、也不在任何反代里）也放一组，免得"明明有却选不到"。
  // 带斜杠的 id（渠道名/模型 点名写法、上游原生斜杠名）全部排除 —— 已按用户要求彻底删掉这两组。
  const left = ids.filter(function(x) {
    return String(x).indexOf('/') < 0 && !shown[String(x).toLowerCase()];
  });
  if (left.length) {
    html += '<optgroup label="其它模型">' + left.map(optOf).join('') + '</optgroup>';
  }
  return html;
}

// 模型测试页的下拉：按优先级表分组（账号池 + 各反代），不带任何前缀写法
function fillTestModelSelect() {
  const sel = $('model');
  if (!sel) return;
  const keep = sel.value;
  sel.innerHTML = buildModelOptionsHtml(window.__allModelIds || []);
  if (keep) sel.value = keep;
}

// 「接管模型」下拉：按来源分组，选一次往输入框追加一个（只列纯模型名，不带渠道前缀）
function fillModelPickers() {
  const opts = buildModelOptionsHtml(window.__allModelIds || []);
  [['p-models-pick', 'p-models'], ['ch-models-pick', 'ch-models']].forEach(function(pair) {
    const s = $(pair[0]), inp = $(pair[1]);
    if (!s || !inp) return;
    s.innerHTML = '<option value="">— 从下面按来源选（选一次加一个，也可以直接手输） —</option>' + opts;
    s.onchange = function() {
      const v = s.value;
      if (!v) return;
      const arr = inp.value.split(/[,，]/).map(function(t) { return t.trim(); }).filter(Boolean);
      if (!arr.some(function(t) { return t.toLowerCase() === v.toLowerCase(); })) arr.push(v);
      inp.value = arr.join(', ');
      s.value = '';
    };
  });
}

function appendMsg(role, text) {
  const log = $('chatlog');
  const cls = role === 'user' ? 'msg-user' : role === 'err' ? 'err' : 'msg-assistant';
  const label = role === 'user' ? '你' : role === 'err' ? '错误' : '助手';
  const div = document.createElement('div');
  div.innerHTML = '<b>[' + label + ']</b> ';
  const span = document.createElement('span');
  span.className = cls;
  span.textContent = text;
  div.appendChild(span);
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

// 累计统计
let cum = { req:0, hit:0, miss:0, out:0, reason:0 };

function usageLine(u) {
  if (!u) return null;
  const hit = (u.prompt_cache_hit_tokens !== undefined ? u.prompt_cache_hit_tokens : (u.prompt_tokens_details && u.prompt_tokens_details.cached_tokens)) || 0;
  const miss = (u.prompt_cache_miss_tokens !== undefined ? u.prompt_cache_miss_tokens : ((u.prompt_tokens||0) - hit));
  const out = u.completion_tokens || 0;
  const reason = (u.completion_tokens_details && u.completion_tokens_details.reasoning_tokens) || 0;
  const input = u.prompt_tokens || (hit+miss);
  const rate = input > 0 ? Math.round(hit/input*100) : 0;
  return { hit, miss, out, reason, input, rate, total: u.total_tokens||0 };
}

function appendUsage(us) {
  const log = $('chatlog');
  const div = document.createElement('div');
  div.className = 'usage';
  div.innerHTML = 'in <b>' + us.input + '</b> · 缓存命中 <span class="hit">' + us.hit + '</span> · 未命中 <span class="miss">' + us.miss + '</span>'
    + ' (命中率 <b>' + us.rate + '%</b>) · out <b>' + us.out + '</b> · 思考 <b>' + us.reason + '</b>';
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

function updateStats(us) {
  $('stats').style.display = 'grid';
  cum.req++; cum.hit += us.hit; cum.miss += us.miss; cum.out += us.out; cum.reason += us.reason;
  $('s-hit').textContent = us.hit;
  $('s-miss').textContent = us.miss;
  $('s-out').textContent = us.out;
  $('s-reason').textContent = us.reason;
  $('s-req').textContent = cum.req;
  const allIn = cum.hit + cum.miss;
  $('s-rate').textContent = (allIn > 0 ? Math.round(cum.hit/allIn*100) : 0) + '%';
}

async function delAcc(uid) {
  showConfirm('确定删除账号 uid=' + uid + ' 吗？删除后该账号移出池子（Token 不受影响）。', async () => {
    try {
      const r = await fetch('/api/accounts?uid=' + uid, {method:'DELETE'});
      const d = await r.json();
      $('accmsg').innerHTML = '<span class="' + (d.ok ? 'msg-assistant' : 'err') + '">' + escHtml(JSON.stringify(d)) + '</span>';
      loadStatus();
    } catch(e) { $('accmsg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>'; }
  });
}

// 页面内确认弹窗
let cmCallback = null;
let cmKeep = false; // showConfirm 的 keep 选项：点确定后不自动关弹窗

// 是不是"平台封禁号"：账号被上游终止性封了，解禁也救不回来（解了只是白撞）
function isBannedAcc(a) {
  if (!a) return false;
  return !!a.disabled && /40302|封禁|封号|banned/i.test(String(a.reason || ''));
}

function showConfirm(text, cb, opts) {
  opts = opts || {};
  $('cm-cancel').style.display = '';
  // keep=true：点「确定」后**不自动关**，留给回调自己决定（校验失败时还能接着改）。
  // 2026-09-22 补上：以前这个参数被忽略，导致"点确定就先关，校验再报错"很别扭。
  cmKeep = !!opts.keep;
  const ok = $('cm-ok');
  ok.style.background = '#dc2626';
  ok.style.borderColor = '#dc2626';
  $('cm-title').textContent = opts.title || '确认删除';
  const body = $('cm-text');
  if (opts.html) body.innerHTML = opts.html;
  else body.textContent = text;
  $('cm-ok').textContent = opts.okText || '确认删除';
  cmCallback = cb;
  $('confirm-modal').style.display = 'flex';
}
$('cm-cancel').onclick = () => {
  $('confirm-modal').style.display = 'none';
  cmCallback = null;
  cmKeep = false;
};

// 只读结果弹窗：复用确认弹窗，但只留一个「知道了」（签完到弹结果用）
function showResult(title, html) {
  $('cm-title').textContent = title;
  $('cm-text').innerHTML = html;
  cmKeep = false; // 只读结果弹窗：点「知道了」就该关
  const ok = $('cm-ok');
  ok.textContent = '知道了';
  ok.style.background = '#2563eb';   // 只读结果用蓝色，别跟"危险操作确认"的红混在一起
  ok.style.borderColor = '#2563eb';
  $('cm-cancel').style.display = 'none';
  cmCallback = () => { $('cm-cancel').style.display = ''; };
  $('confirm-modal').style.display = 'flex';
}
$('cm-ok').onclick = () => {
  const cb = cmCallback;
  if (!cmKeep) {
    $('confirm-modal').style.display = 'none';
    cmCallback = null;
  }
  if (cb) cb();
};

// ===== API Key 管理（列表，历史全保留） =====
let keyCache = [];
let keyUsage = {};
let keyLimits = {};
let keyDaily = {};   // 每 Key 每日单数上限（0 = 不限）
let keyStrict = {};  // true = 每分钟限速超了直接 429，不排队
let keyUsed = {};    // 今日已用单数

function relTime(iso) {
  if (!iso) return '从未';
  const t = new Date(iso).getTime();
  if (!t || t < 0) return '从未';
  const diff = Math.max(0, Date.now() - t);
  const s = Math.floor(diff/1000);
  if (s < 60) return s + ' 秒前';
  const m = Math.floor(s/60);
  if (m < 60) return m + ' 分钟前';
  const h = Math.floor(m/60);
  if (h < 24) return h + ' 小时前';
  return Math.floor(h/24) + ' 天前';
}

// 概览页那张「API 密钥」卡片：总数 + 「N 启用」。
//   总数：只写数字（单位「个」在下面那行，按用户要求）
//   启用：有过调用记录的密钥数（keyUsage[k].count > 0）—— 从没用过的算"未使用"
function setKeyCount() {
  const keys = keyCache || [];
  const el = $('key-count');
  if (el) el.textContent = String(keys.length);
  const on = $('key-on');
  if (on) {
    const usedN = keys.filter(k => keyUsage && keyUsage[k] && keyUsage[k].count > 0).length;
    on.textContent = String(usedN);
  }
}

async function loadKeys() {
  try {
    const d = await fetchJSON('/api/apikey');
    if (!d) return;
    keyCache = d.keys || [];
    try {
      const lr = await fetch('/api/keylimits');
      const ld = await lr.json();
      keyLimits = ld.limits || {};
      keyDaily = ld.daily || {};
      keyStrict = ld.strict || {};
      keyUsed = ld.used || {};
    } catch(e) { keyLimits = {}; }
    // 拉取每个 key 的使用统计
    try {
      const ur = await fetch('/api/keyusage');
      const ud = await ur.json();
      keyUsage = ud.keys || {};
    } catch(e) { keyUsage = {}; }
    // 概览页那张「API 密钥」卡片：总数 + N 启用（要在 keyUsage 拉回来之后算，
    // 否则「启用」会算成 0）。loadKeys 每 30 秒跑一次，不额外发请求。
    setKeyCount();
    renderKeys();
  } catch(e) {
    $('keylist').innerHTML = '<span class="err">Key 列表加载失败: ' + escHtml(e.message) + '</span>';
  }
}

function renderKeys() {
  if ($('keycount')) $('keycount').textContent = keyCache.length ? ' · 共 ' + keyCache.length + ' 个（# 为添加顺序）' : '';
  if (!keyCache.length) {
    $('keylist').innerHTML = '<div class="usage">(无 Key，服务不鉴权)</div>';
    return;
  }
  const now = Date.now();
  $('keylist').innerHTML = keyCache.map((k, i) => {
    const u = keyUsage[k];
    let status, statusTitle;
    if (!u || !u.count) {
      status = '<span class="pill gray">未使用</span>';
      statusTitle = '从未被调用';
    } else {
      const recent = (u.recent || []).map(t => new Date(t).getTime()).filter(t => t > 0);
      const in2 = recent.filter(t => now - t < 2 * 60 * 1000).length;   // 近 2 分钟
      const in5 = recent.filter(t => now - t < 5 * 60 * 1000).length;   // 近 5 分钟
      if (in2 >= 2) {
        status = '<span class="pill green">使用中</span>';
        statusTitle = '近2分钟 ' + in2 + ' 次 · 累计 ' + u.count + ' 次';
      } else if (in5 >= 1) {
        status = '<span class="pill yellow">偶发</span>';
        statusTitle = relTime(u.last_used) + ' · 近5分钟 ' + in5 + ' 次 · 累计 ' + u.count + ' 次';
      } else {
        status = '<span class="pill gray">空闲</span>';
        statusTitle = relTime(u.last_used) + ' · 累计 ' + u.count + ' 次';
      }
    }
    // 这一行的主体 +（默认收起的）「在干什么」明细块
    return '<div class="key-row">'
      + '<span class="keyno" title="第 ' + (i + 1) + ' 个生成（按添加顺序）">#' + (i + 1) + '</span>'
      + '<code style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; color:#15803d; padding:6px 10px; font-size:13px; flex:1; min-width:220px; word-break:break-all">' + k + '</code>'
      + '<span class="usage" style="margin:0; white-space:nowrap">' + statusTitle + '</span>'
      + status
      + '<select class="keylimit" data-key="' + escHtml(k) + '" title="该 Key 每分钟最多几次请求">' + limitOptions(keyLimits[k] || 0) + '</select>'
      + '<select class="keydaily" data-key="' + escHtml(k) + '" title="该 Key 每天最多几单：超了直接 429（不排队），第二天自动重置">' + dailyOptions(keyDaily[k] || 0) + '</select>'
      + '<span class="usage" style="margin:0;white-space:nowrap;min-width:96px;text-align:right" title="今日已用 / 每日上限">' + (keyDaily[k] ? '今日 ' + (keyUsed[k] || 0) + '/' + keyDaily[k] : '') + '</span>'
      + '<label class="usage" style="margin:0;white-space:nowrap;display:inline-flex;align-items:center;gap:3px;cursor:pointer" title="勾上 = 每分钟限速超了直接 429（默认排队等待，客户端只是变慢）"><input type="checkbox" class="keystrict" data-key="' + escHtml(k) + '"' + (keyStrict[k] ? ' checked' : '') + ' style="margin:0">严格</label>'
      + '<button class="gray mini key-what-btn" data-i="' + i + '" title="这个 Key 的调用记录：今日单数 / token、常用模型、最近 30 条明细（时间 · 模型 · 走哪条上游 · token · 成败）">记录</button>'
      + '<button class="gray mini" onclick="copyKey(' + i + ')">复制</button>'
      + '<button class="del" onclick="delKey(' + i + ')">删</button>'
      + '</div>'
      + '<div class="key-what" id="kw-' + i + '" style="display:none"></div>';
  }).join('');
  $('keylist').querySelectorAll('.keylimit').forEach(sel => sel.onchange = async () => {
    try {
      await fetch('/api/keylimits/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({key: sel.dataset.key, rpm: parseInt(sel.value, 10)})});
    } catch(e) {}
    loadKeys();
  });
  // 每日额度：超了服务端直接 429（硬闸门）
  $('keylist').querySelectorAll('.keydaily').forEach(sel => sel.onchange = async () => {
    try {
      await fetch('/api/keylimits/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({key: sel.dataset.key, daily: parseInt(sel.value, 10)})});
    } catch(e) {}
    loadKeys();
  });
  // 严格模式：每分钟限速超了直接 429，不排队
  $('keylist').querySelectorAll('.keystrict').forEach(cb => cb.onchange = async () => {
    try {
      await fetch('/api/keylimits/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({key: cb.dataset.key, strict: cb.checked})});
    } catch(e) {}
    loadKeys();
  });
  // 「记录」展开/收起：展开时去拉这个 key 的调用流水（/api/callslog → 主服务 /calls/log）
  $('keylist').querySelectorAll('.key-what-btn').forEach(b => b.onclick = () => {
    const box = $('kw-' + b.dataset.i);
    if (!box) return;
    const show = box.style.display === 'none';
    box.style.display = show ? 'block' : 'none';
    b.textContent = show ? '收起' : '记录';
    if (show) loadKeyLog(parseInt(b.dataset.i, 10));
  });
}

// 拉某个 key 的调用流水并渲染成表格。
// 数据源：主服务 GET /calls/log?key=&from=&to=&limit=（JSONL 落盘，主服务重启也不丢）
async function loadKeyLog(i, from, to, page) {
  const k = keyCache[i];
  const box = $('kw-' + i);
  if (!box || !k) return;
  if (typeof from !== 'string') from = ($('kw-from-' + i) || {}).value || '';
  if (typeof to !== 'string') to = ($('kw-to-' + i) || {}).value || '';
  let size = parseInt(box.dataset.size || '50', 10);
  if (!(size > 0)) size = 50;
  let pg = (typeof page === 'number' && page > 0) ? page : parseInt(box.dataset.page || '1', 10);
  if (!(pg > 0)) pg = 1;
  box.innerHTML = '<div class="hint">读取中…</div>';
  let d = null;
  try {
    const r = await fetch('/api/callslog?key=' + encodeURIComponent(k)
      + '&from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to)
      + '&limit=' + size + '&offset=' + ((pg - 1) * size));
    d = await r.json();
  } catch(e) {
    box.innerHTML = '<div class="err">读取失败：' + escHtml(e.message) + '</div>';
    return;
  }
  const total = (d && d.total) || 0;
  const pages = Math.max(1, Math.ceil(total / size));
  if (pg > pages) pg = pages; // 翻过头（比如删了记录）→ 回到最后一页
  box.dataset.page = pg;
  box.dataset.size = size;
  box.innerHTML = keyWhatHtml(keyUsage[k]) + keyLogTableHtml(d, i, from, to, pg, size);
  // 分页 + 筛选的绑定
  const go = p => loadKeyLog(i, undefined, undefined, p);
  const q = $('kw-query-' + i), c = $('kw-clear-' + i);
  if (q) q.onclick = () => go(1);
  if (c) c.onclick = () => {
    if ($('kw-from-' + i)) $('kw-from-' + i).value = '';
    if ($('kw-to-' + i)) $('kw-to-' + i).value = '';
    loadKeyLog(i, '', '', 1);
  };
  [['kw-first-' + i, 1], ['kw-prev-' + i, pg - 1], ['kw-next-' + i, pg + 1], ['kw-last-' + i, pages]].forEach(function(pair) {
    const el = $(pair[0]);
    if (el) el.onclick = () => go(pair[1]);
  });
  const sz = $('kw-size-' + i);
  if (sz) sz.onchange = () => { box.dataset.size = sz.value; go(1); };
  const jp = $('kw-goto-' + i);
  if (jp) jp.onkeydown = ev => { if (ev.key === 'Enter') go(parseInt(jp.value, 10) || 1); };
  ['kw-from-' + i, 'kw-to-' + i].forEach(id => {
    const el = $(id);
    if (el) el.onkeydown = ev => { if (ev.key === 'Enter') go(1); };
  });
}

// 调用流水表格（时间 / 消息 / 模型 / 输入 / 缓存命中 / 输出 / 消耗积分估算 / 费用估算 / 上游·结果）
function keyLogTableHtml(d, i, from, to, page, size) {
  const rows = (d && d.rows) || [];
  const total = (d && d.total) || 0;
  page = page || 1;
  size = size || 50;
  const pages = Math.max(1, Math.ceil(total / size));
  const accts = {};
  ((typeof poolStatus !== 'undefined' && poolStatus && poolStatus.accs) || []).forEach(a => { accts[a.uid] = a; });
  // 全池平均"积分/token"：某个账号还没攒够今日数据时兜底用它
  let poolBurn = 0, poolTok = 0;
  Object.keys(accts).forEach(uid => {
    poolBurn += (accts[uid].today_burn || 0);
    poolTok += (accts[uid].today_tokens || 0);
  });
  const chans = (typeof chanData !== 'undefined' && chanData && chanData.channels) || [];
  const bar = '<div class="kw-bar">'
    + '<span class="kw-lab">日期范围</span>'
    + '<input type="date" class="kw-date" id="kw-from-' + i + '" value="' + escHtml(from || '') + '">'
    + '<span class="kw-arrow">→</span>'
    + '<input type="date" class="kw-date" id="kw-to-' + i + '" value="' + escHtml(to || '') + '">'
    + '<button class="gray mini" id="kw-query-' + i + '">查询</button>'
    + '<button class="gray mini" id="kw-clear-' + i + '">清空</button>'
    + '<span class="kw-total">共 ' + total + ' 条调用</span>'
    + '</div>';
  if (!rows.length) {
    return bar + '<div class="hint" style="margin:6px 0 0">这个区间没有调用记录。</div>';
  }
  const trs = rows.map(r => {
    const t = String(r.at || '').replace('T', ' ').slice(0, 16);
    const src = !r.src ? '未转发'
      : (r.src === 'lobster' ? '账号池' : (((chans.find(c => c.id === r.src) || {}).name) || r.src));
    const acct = r.acct ? (accts[r.acct] || null) : null;
    // 消耗积分估算：拿这个账号**实测**的"今日积分消耗 ÷ 今日 token"折算本单。
    // 上游 usage 里没有积分字段（响应头也没有），所以这是估算，不是有道后台那个准数。
    let credits = '-';
    if (r.total) {
      let ratio = 0;
      if (acct && acct.today_tokens > 0) ratio = (acct.today_burn || 0) / acct.today_tokens;
      else if (poolTok > 0) ratio = poolBurn / poolTok; // 该账号今天还没数据 → 用全池平均折算
      if (ratio > 0) credits = (Math.round(r.total * ratio * 100) / 100).toFixed(2);
    }
    const miss = Math.max(0, (r.in || 0) - (r.cached || 0));
    const usd = (miss / 1e6) * priceIn + ((r.cached || 0) / 1e6) * priceCache + ((r.out || 0) / 1e6) * priceOut;
    const money = usd > 0
      ? (cnyRate > 0
          ? '¥' + (usd * cnyRate >= 0.01 ? (usd * cnyRate).toFixed(2) : (usd * cnyRate).toFixed(4))
          : '$' + (usd >= 0.01 ? usd.toFixed(2) : usd.toFixed(5)))
      : '-';
    return '<tr class="' + (r.ok ? '' : 'kw-badrow') + '">'
      + '<td class="kw-c-time">' + escHtml(t) + '</td>'
      + '<td class="kw-c-msg" title="' + escHtml(r.msg || '') + '">' + escHtml(r.msg || '-') + '</td>'
      + '<td>' + escHtml(r.model || '-') + '</td>'
      + '<td class="kw-num">' + fmtNum(r.in || 0) + '</td>'
      + '<td class="kw-num">' + fmtNum(r.cached || 0) + '</td>'
      + '<td class="kw-num">' + fmtNum(r.out || 0) + '</td>'
      + '<td class="kw-num">' + credits + '</td>'
      + '<td class="kw-num">' + money + '</td>'
      + '<td>' + escHtml(src) + (r.stream ? ' · 流' : '')
        + (r.ok ? '' : ' <span class="kw-bad">❌ ' + escHtml(String(r.note || '失败').slice(0, 40)) + '</span>') + '</td>'
      + '</tr>';
  }).join('');
  return bar
    + '<div class="kw-tablewrap"><table class="kw-table">'
    + '<thead><tr><th>时间</th><th>消息</th><th>模型</th><th>输入 TOKENS</th><th>缓存命中 TOKENS</th><th>输出 TOKENS</th>'
    + '<th>消耗(积分估算)</th><th>费用估算</th><th>上游 / 结果</th></tr></thead>'
    + '<tbody>' + trs + '</tbody></table></div>'
    + '<div class="kw-pager">'
    + '<span class="kw-lab">共 ' + total + ' 条 · 第 ' + page + ' / ' + pages + ' 页</span>'
    + '<button class="gray mini" id="kw-first-' + i + '"' + (page <= 1 ? ' disabled' : '') + '>首页</button>'
    + '<button class="gray mini" id="kw-prev-' + i + '"' + (page <= 1 ? ' disabled' : '') + '>上一页</button>'
    + '<button class="gray mini" id="kw-next-' + i + '"' + (page >= pages ? ' disabled' : '') + '>下一页</button>'
    + '<button class="gray mini" id="kw-last-' + i + '"' + (page >= pages ? ' disabled' : '') + '>末页</button>'
    + '<span class="kw-lab">每页</span>'
    + '<select class="kw-size" id="kw-size-' + i + '">'
      + [20, 50, 100, 200].map(function(n) { return '<option value="' + n + '"' + (n === size ? ' selected' : '') + '>' + n + '</option>'; }).join('')
    + '</select>'
    + '<span class="kw-lab">跳到</span>'
    + '<input class="kw-goto" id="kw-goto-' + i + '" type="number" min="1" value="' + page + '">'
    + '<span class="kw-lab">页</span>'
    + '</div>'
    + '<div class="hint" style="margin:4px 0 0">'
    + '积分估算 = 本单 token ×「该账号今日积分消耗 ÷ 今日 token」；费用估算按「数据概览 → 改价」里设的单价换算。</div>';
}

// 「这个 token 在干什么」的明细块。
// 只输出一行摘要：今日单数/token、累计单数/token、最后调用时间。
// 数据源：/api/keyusage（主服务 keyStats）的 today_calls / today_tokens / total_tokens / count / last_used。
// 调用明细不在这里 —— 见 loadKeyLog 拉的流水表（/api/callslog）。
function keyWhatHtml(u) {
  if (!u) return '';
  // 2026-09-22 用户要求「先去掉」：原来这里还挂了「常用模型 ×N」和「最近 12 条明细」，
  // 但那两样下面那张流水表已经全有了（而且更全、还能按天筛），重复一遍纯刷屏。
  // 只留这一行摘要（今日/累计/最后调用），够用不占地方。
  return '<div class="hint" style="margin:4px 0 6px">'
      + '今日 <b>' + (u.today_calls || 0) + '</b> 单 · <b>' + fmtNum(u.today_tokens || 0) + '</b> token'
      + '　｜　累计 <b>' + (u.count || 0) + '</b> 单 · token <b>' + fmtNum(u.total_tokens || 0) + '</b>（新版本起计）'
      + (u.last_used ? '　｜　最后调用 ' + escHtml(relTime(u.last_used)) : '')
      + '</div>';
}

// Key 限速档位
function limitOptions(cur) {
  const tiers = [0, 1, 3, 5, 10, 20, 30, 60, 120, 600];
  let out = tiers.map(v => '<option value="' + v + '"' + (v === cur ? ' selected' : '') + '>' + (v === 0 ? '不限速' : v + '/分') + '</option>').join('');
  if (cur && tiers.indexOf(cur) < 0) out += '<option value="' + cur + '" selected>' + cur + '/分</option>';
  return out;
}

// Key 每日额度档位（这是"套餐额度"：超了直接 429，不排队）
function dailyOptions(cur) {
  const tiers = [0, 100, 300, 1000, 3000, 10000];
  let out = tiers.map(v => '<option value="' + v + '"' + (v === cur ? ' selected' : '') + '>' + (v === 0 ? '不限量' : v + ' 单/天') + '</option>').join('');
  if (cur && tiers.indexOf(cur) < 0) out += '<option value="' + cur + '" selected>' + cur + ' 单/天</option>';
  return out;
}

// 复制工具：面板对外是 http://IP:8368（非安全上下文），navigator.clipboard 在
// 这种情况下是 undefined/被拒，公网点「复制」必挂。所以必须带 execCommand 兜底。
async function copyText(text, el) {
  // ① 安全上下文（https / localhost）优先走新 API
  if (navigator.clipboard && window.isSecureContext) {
    try { await navigator.clipboard.writeText(text); return true; } catch(e) {}
  }
  // ② 兼容兜底：临时 textarea + execCommand（http 下唯一能用的方式）
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;padding:0;border:0;opacity:0;';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    ta.setSelectionRange(0, text.length);
    const ok = document.execCommand('copy');
    document.body.removeChild(ta);
    if (ok) return true;
  } catch(e) {}
  // ③ 还不行就把文本选中，让用户自己 Ctrl+C
  if (el) {
    try {
      const r = document.createRange();
      r.selectNodeContents(el);
      const s = document.getSelection();
      s.removeAllRanges();
      s.addRange(r);
    } catch(e) {}
  }
  return false;
}

async function copyKey(i) {
  const k = keyCache[i];
  const rows = $('keylist').querySelectorAll(':scope > div');
  const el = rows[i] ? rows[i].querySelector('code') : null;
  const ok = await copyText(k, el);
  $('keymsg').innerHTML = ok
    ? '<span class="msg-assistant">已复制: ' + k + '</span>'
    : '<span class="err">复制被浏览器拦了，已帮你选中文本，按 Ctrl+C 即可</span>';
}

async function delKey(i) {
  const k = keyCache[i];
  showConfirm('确定删除 Key：' + k + ' ？删除后该 Key 立即失效，使用它的客户端会报错。', async () => {
    try {
      const r = await fetch('/api/apikey?key=' + encodeURIComponent(k), {method:'DELETE'});
      const d = await r.json();
      $('keymsg').innerHTML = '<span class="' + (d.ok ? 'msg-assistant' : 'err') + '">' + escHtml(JSON.stringify(d)) + '</span>';
      if (d.ok) { keyCache = d.keys || []; renderKeys(); setKeyCount(); }
    } catch(e) { $('keymsg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>'; }
  });
}

$('keygen').onclick = async () => {
  const custom = $('keycustom').value.trim();
  const msg = custom ? '添加自定义 Key: ' + custom + ' ？' : '生成新的随机 Key？';
  if (!confirm(msg + '（老 Key 继续有效）')) return;
  const btn = $('keygen'); btn.disabled = true;
    $('keymsg').innerHTML = '<span style="color:#2563eb">提交中...</span>';
  try {
    const r = await fetch('/api/apikey', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({key: custom || '__generate__'})
    });
    const d = await r.json();
    $('keymsg').innerHTML = '<span class="' + (d.ok ? 'msg-assistant' : 'err') + '">' + escHtml(JSON.stringify(d)) + '</span>';
      if (d.ok) { keyCache = d.keys || []; renderKeys(); setKeyCount(); $('keycustom').value = ''; }
  } catch(e) { $('keymsg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>'; }
  btn.disabled = false;
};

// ===== 手机验证码登录（服务器浏览器自动填写） =====
let scanqTimer = null;

async function ensureBrowser() {
  // 确保浏览器已启动并打开登录页；若已启动则复用
  try {
    const st = await (await fetch('/api/scanstate')).json();
    if (st.state) return true; // 已有会话
  } catch (e) {}
  const r = await fetch('/api/scanlogin', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ invitationCode: $('scan-invite').value.trim() })
  });
  const d = await r.json();
  return !!(d && d.ok);
}

$('scanp-send').onclick = async () => {
  const phone = $('scanp-phone').value.trim();
  if (!/^1\d{10}$/.test(phone)) {
    $('scanp-msg').innerHTML = '<span class="err">请输入 11 位手机号</span>';
    return;
  }
  const btn = $('scanp-send'); btn.disabled = true;
  $('scanp-msg').innerHTML = '<span style="color:#2563eb">启动服务器浏览器并发送验证码...</span>';
  try {
    const okB = await ensureBrowser();
    if (!okB) throw new Error('浏览器启动失败');
    const r = await fetch('/api/scanphone', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phone })
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '发送失败');
    $('scanp-msg').innerHTML = '<span class="msg-assistant">✅ 验证码已发送到 ' + phone + '，请输入短信验证码并点「提交登录」</span>';
  } catch (e) {
    $('scanp-msg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>';
  }
  btn.disabled = false;
};

$('scanp-login').onclick = async () => {
  const code = $('scanp-sms').value.trim();
  if (!code) {
    $('scanp-msg').innerHTML = '<span class="err">请输入短信验证码</span>';
    return;
  }
  const btn = $('scanp-login'); btn.disabled = true;
  $('scanp-msg').innerHTML = '<span style="color:#2563eb">提交登录...</span>';
  try {
    const r = await fetch('/api/scansms', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code })
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '提交失败');
    $('scanp-msg').innerHTML = '<span style="color:#2563eb">登录提交成功，等待回调自动入池...</span>';
    // 轮询完成状态
    clearInterval(scanqTimer);
    scanqTimer = setInterval(async () => {
      try {
        const sr = await fetch('/api/scanstate');
        const sd = await sr.json();
        if (sd.done) {
          clearInterval(scanqTimer);
          $('scanp-msg').innerHTML = '<span class="msg-assistant">✅ 登录成功！账号已加入池子</span>';
          loadStatus(true);
          btn.disabled = false;
        }
      } catch (e) {}
    }, 2000);
  } catch (e) {
    $('scanp-msg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>';
    btn.disabled = false;
  }
};

$('send').onclick = async () => {
  const p = $('prompt').value.trim();
  if (!p) return;
  const btn = $('send');
  btn.disabled = true;
  appendMsg('user', p);
  try {
    const r = await fetch('/api/chat', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({model: $('model').value, messages:[{role:'user', content:p}], stream:false})
    });
    const d = await r.json();
    if (d.error) { appendMsg('err', JSON.stringify(d.error)); }
    else {
      appendMsg('assistant', (d.choices&&d.choices[0]&&d.choices[0].message&&d.choices[0].message.content)||'(空回复)');
      const us = usageLine(d.usage);
      if (us) { appendUsage(us); updateStats(us); }
    }
  } catch(e) {
    appendMsg('err', e.message);
  }
  btn.disabled = false;
};

$('clear').onclick = () => { $('chatlog').innerHTML = ''; $('prompt').value = ''; };

$('addacc').onclick = async () => {
  const btn = $('addacc');
  const j = $('authjson').value.trim();
  if (!j) { $('accmsg').innerHTML = '<span class="err">请先粘贴完整 auth JSON</span>'; return; }
  let payload;
  try {
    payload = JSON.parse(j);
  } catch(e) {
    $('accmsg').innerHTML = '<span class="err">auth JSON 解析失败: ' + escHtml(e.message) + '</span>';
    return;
  }
  btn.disabled = true;
  $('accmsg').innerHTML = '<span style="color:#2563eb">提交中...</span>';
  // 兼容嵌套形 {"auth":{...},"account":{...}}：token 在 payload.auth 里
  const tok = payload.auth ? payload.auth.accessToken : payload.accessToken;
  const rtok = payload.auth ? payload.auth.refreshToken : payload.refreshToken;
  if (!tok || !rtok) {
    $('accmsg').innerHTML = '<span class="err">JSON 里必须有 accessToken 和 refreshToken</span>';
    btn.disabled = false; return;
  }
  try {
    const r = await fetch('/api/accounts', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const d = await r.json();
    $('accmsg').innerHTML = '<span class="' + (d.ok ? 'msg-assistant' : 'err') + '">' + escHtml(JSON.stringify(d)) + '</span>';
    if (d.ok) { waitBackendThenReload(); }
  } catch(e) {
    $('accmsg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>';
  }
  btn.disabled = false;
};

// ===== 上游反代（多渠道） =====
function escHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
}

async function loadChannels() {
  try {
    const d = await fetchJSON('/api/channels');
    if (!d) return;
    chanData = d;
    renderUpstreamPick(d);
    renderPrefixHint();
    renderAccounts();
    // 上游反代页展示与账号池完全一致的卡片
    renderChannelCards('ch-list', d.default);
    loadUsage();
    fillTestModelSelect();
    fillModelPickers();
    fillCreditSrc();   // 积分卡的来源下拉（池子 + 各反代）
  } catch(e) {}
}

// 模型映射输入框文本 → 对象。
// 写法：「客户端名=上游真名」，多条用逗号（中英文都行）分隔，例如
//   deepseek-flash=DeepSeek-V4.1-Flash, glm-5.2=GLM-5.3
// 只写名字不写 =（没有上游真名）的那种行直接忽略；返回空对象表示「没有任何映射」，
// 前端的空对象是有意义的：后端用 nil 判断"这次没传这个字段，别动"，
// 传 {} 就是"清空已有映射"。
function parseModelMap(text) {
  const out = {};
  String(text || '').split(/[,，\n]/).forEach(function(seg) {
    const s = seg.trim();
    if (!s) return;
    const i = s.indexOf('=');
    if (i <= 0) return;
    const k = s.slice(0, i).trim();
    const v = s.slice(i + 1).trim();
    if (k && v) out[k] = v;
  });
  return out;
}

$('ch-add').onclick = async () => {
  const btn = $('ch-add');
  const name = $('ch-name').value.trim();
  const url = $('ch-url').value.trim();
  const key = $('ch-key').value.trim();
const models = $('ch-models').value.split(/[,，]/).map(s => s.trim()).filter(Boolean);
  const noteVal = ($('ch-noteinput') ? $('ch-noteinput').value.trim() : '');
  const editing = editingId;
  if (!url || (!key && !editing)) { $('ch-msg').innerHTML = '<span class="err">地址和 Token 必填</span>'; return; }
  btn.disabled = true; btn.textContent = editing ? '保存中...' : '添加中...';
  try {
  // model_map 一定带上（哪怕 {}）——后端靠"字段在不在"区分"不改"和"清空"
  const modelMap = parseModelMap($('ch-modelmap') ? $('ch-modelmap').value : '');
  const payload = {name, base_url: url, api_key: key, models, note: noteVal, model_map: modelMap};
    if (editing) payload.id = editing;
    const r = await fetch(editing ? '/api/channels/update' : '/api/channels/add', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
    const d = await r.json();
    if (d.ok) {
      const sample = (d.models_sample && d.models_sample.length) ? '，样例：' + escHtml(d.models_sample.slice(0, 8).join(', ')) : '';
      $('ch-msg').innerHTML = '<span class="msg-assistant">' + (editing ? '修改已保存' : '添加成功') + '，连通测试通过' + sample + '</span>';
      setEditMode(null);
    } else {
      $('ch-msg').innerHTML = '<span class="err">' + (editing ? '已保存' : '已添加') + '，但连通测试失败：' + escHtml(d.error || '') + '</span>';
      if (editing) setEditMode(null);
    }
  } catch(e) {
    $('ch-msg').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>';
  }
  btn.disabled = false; btn.textContent = '添加反代（自动测连通）';
  loadChannels();
};

// 底部提示里的前缀示例：跟着实际渠道动态生成（以后加渠道自动出现）
function renderPrefixHint() {
  const el = $('prefix-hint');
  if (!el) return;
  const chans = ((chanData && chanData.channels) || []).filter(c => c.enabled);
  const parts = chans.map(c => '<code>' + escHtml(c.name) + '/xxx</code>');
  parts.push('<code>lobster/xxx</code>');
  el.innerHTML = parts.join('、');
  const el2 = $('prefix-hint2');
  if (el2) {
    const c0 = chans[0];
    el2.innerHTML = c0 ? ('<code>' + escHtml(c0.name) + '/模型名</code>') : '渠道名/模型';
  }
}

// 账号池卡片顶部的「默认上游」选择：自动 / 龙虾账号池 / 任意反代
function renderUpstreamPick(d) {
  const chans = (d.channels || []).filter(c => c.enabled);
  const items = [{id:'', name:'自动'}, {id:'lobster', name:'龙虾账号池'}];
  chans.forEach(c => items.push({id: c.id, name: c.name}));
  const cur = d.default || '';
  const fbCur = d.fallback_target || '';
  // 「自动」挪到紧跟「失败回退」标签的位置（用户要求：自动要挨着失败回退）
  const fbItems = [{id:'auto', name:'自动'}, {id:'', name:'关'}, {id:'lobster', name:'龙虾账号池'}];
  chans.forEach(c => fbItems.push({id: c.id, name: c.name}));
  $('up-pick').innerHTML = '<span class="up-label def">默认上游</span>'
    + items.map(it => '<span class="up-chip' + (it.id === cur ? ' on' : '') + '" data-def="' + it.id + '">' + escHtml(it.name) + '</span>').join('')
    + '<span class="up-sep"></span><span class="up-label fb" title="反代失败后把这单交给谁">失败回退</span>'
    + fbItems.map(it => '<span class="up-chip' + (it.id === fbCur ? ' on' : '') + '" data-fb="' + it.id + '">' + escHtml(it.name) + '</span>').join('')
    + '<span class="up-sep"></span><span class="up-chip" id="ch-addchip">+ 添加反代</span>'
    + '<span class="up-sep"></span><span class="up-hint" title="鼠标按住卡片标题栏直接拖到新位置，松手自动保存（龙虾账号池也能拖）。顺序从上往下试，数字小的先试 —— 账号池排第几就第几个轮到他">优先级：拖动卡片排序（含龙虾账号池）</span>';
  const warn = $('up-warn');
  if (warn) {
    const fbName = (fbItems.find(x => x.id === fbCur) || {}).name || '未知';
    if (cur && cur !== 'lobster' && fbCur === 'lobster') {
      warn.innerHTML = '<span style="color:#b45309">⚠ 反代失败会回退龙虾账号池（消耗积分），把上面「失败回退」改成「关」就不消耗</span>';
    } else if (cur && cur !== 'lobster' && fbCur === '') {
      warn.innerHTML = '<span style="color:#15803d">已关闭回退：反代失败直接报错，不消耗龙虾积分</span>';
    } else if (cur && cur !== 'lobster') {
      // 默认上游和失败回退是同一个渠道 → 说点实话：它失败时不可能"回退到它自己"
      if (fbCur === cur) {
        warn.innerHTML = '<span style="color:#b45309">⚠ 默认上游和失败回退<b>是同一个</b>（都是「' + escHtml(fbName) + '」）：它失败时<b>不会回退到它自己</b>，'
          + '会自动去扫其它反代；其它反代也不行就<b>直接报错</b>（不会落龙虾池）。'
          + '想真的兜底，把「失败回退」改成别的反代或「龙虾账号池」。</span>';
      } else {
        warn.innerHTML = '<span style="color:#15803d">反代失败会回退到「' + escHtml(fbName) + '」</span>';
      }
    } else if (cur === 'lobster') {
      warn.textContent = '全部请求强制走龙虾账号池';
    } else {
      warn.textContent = '按模型接管：被反代接管的模型走反代，其余走龙虾账号池';
    }
  }
  $('up-pick').querySelectorAll('.up-chip[data-def]').forEach(el => el.onclick = async () => {
    await fetch('/api/channels/default', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({default: el.dataset.def})});
    loadChannels();
  });
  $('up-pick').querySelectorAll('.up-chip[data-fb]').forEach(el => el.onclick = async () => {
    await fetch('/api/channels/fallback', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({target: el.dataset.fb})});
    loadChannels();
  });
  const addChip = $('ch-addchip');
  if (addChip) addChip.onclick = () => openRelayForm(null);
}

// 账号池页里的反代增改表单
let editingChanId = null;
function openRelayForm(c) {
  editingChanId = c ? c.id : null;
  $('p-name').value = c ? c.name : '';
  $('p-url').value = c ? c.base_url : '';
  $('p-key').value = '';
  $('p-key').placeholder = c ? ('留空 = 不改（当前 ' + c.api_key + '）') : 'sk-...';
  $('p-models').value = (c && c.models) ? c.models.join(', ') : '';
  $('p-modelmap').value = (c && c.model_map)
    ? Object.keys(c.model_map).map(function(k) { return k + '=' + c.model_map[k]; }).join(', ')
    : '';
  $('p-note').value = (c && c.note) ? c.note : '';
  $('p-save').textContent = c ? '保存修改' : '添加反代';
  $('pool-chan-form').style.display = 'grid';
  $('p-name').focus();
}
function closeRelayForm() {
  $('pool-chan-form').style.display = 'none';
  editingChanId = null;
}

// 编辑模式：把渠道填回表单，改完点保存
let editingId = null;
function setEditMode(c) {
  editingId = c ? c.id : null;
  $('ch-name').value = c ? c.name : '';
  $('ch-url').value = c ? c.base_url : '';
  $('ch-key').value = '';
  $('ch-key').placeholder = c ? ('留空 = 不改（当前 ' + c.api_key + '）') : 'sk-...';
  $('ch-models').value = (c && c.models) ? c.models.join(', ') : '';
  // 模型映射：对象 → "客户端名=上游真名, ..." 文本
  $('ch-modelmap').value = (c && c.model_map)
    ? Object.keys(c.model_map).map(function(k) { return k + '=' + c.model_map[k]; }).join(', ')
    : '';
  $('ch-noteinput').value = (c && c.note) ? c.note : '';
  $('ch-add').textContent = c ? '保存修改' : '添加反代（自动测连通）';
  $('ch-cancel').style.display = c ? 'inline-block' : 'none';
  if (c) $('ch-msg').innerHTML = '<span class="msg-assistant">正在编辑「' + escHtml(c.name) + '」——模型留空表示不接管任何请求，填 * 表示全部接管</span>';
  else $('ch-msg').innerHTML = '';
}

$('ch-cancel').onclick = () => { setEditMode(null); };
// §113 手机端「使用说明」折叠（桌面端这个按钮是 display:none，说明照旧常显）
(function() {
  const btn = $('ch-help-btn'), box = $('ch-help');
  if (!btn || !box) return;
  btn.onclick = () => {
    const open = box.classList.toggle('open');
    btn.textContent = open ? '收起说明 ▴' : '使用说明 ▾';
  };
})();

$('p-cancel').onclick = () => closeRelayForm();
$('p-save').onclick = async () => {
  const btn = $('p-save');
  const name = $('p-name').value.trim();
  const url = $('p-url').value.trim();
  const key = $('p-key').value.trim();
  const models = $('p-models').value.split(/[,，]/).map(s => s.trim()).filter(Boolean);
  if (!url || (!key && !editingChanId)) { $('up-warn').innerHTML = '<span class="err">地址和 Token 必填</span>'; return; }
  const editing = editingChanId;
  btn.disabled = true; btn.textContent = '保存中...';
  try {
    const payload = {
      name, base_url: url, api_key: key, models,
      note: $('p-note').value.trim(),
      model_map: parseModelMap($('p-modelmap') ? $('p-modelmap').value : '')
    };
    if (editing) payload.id = editing;
    const r = await fetch(editing ? '/api/channels/update' : '/api/channels/add', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
    const dd = await r.json();
    $('up-warn').innerHTML = dd.ok
      ? '<span class="msg-assistant">' + (editing ? '已保存' : '已添加') + '，连通测试通过' + (dd.models ? '（' + dd.models + ' 个模型）' : '') + '</span>'
      : '<span class="err">' + (editing ? '已保存' : '已添加') + '，但连通测试失败：' + escHtml(dd.error || '') + '</span>';
    closeRelayForm();
  } catch(e) {
    $('up-warn').innerHTML = '<span class="err">' + escHtml(e.message) + '</span>';
  }
  btn.disabled = false; btn.textContent = '保存反代';
  loadChannels();
};

// 测试结果里的模型名可点选，直接写进「接管模型」
function renderModelPick(ids) {
  if (!ids || !ids.length) return;
  const box = document.createElement('div');
  box.className = 'models';
  box.style.marginTop = '8px';
  box.innerHTML = ids.map(m => '<span class="model-tag pick-model" data-m="' + escHtml(m) + '">' + escHtml(m) + '</span>').join('');
  $('ch-msg').appendChild(box);
  const tip = document.createElement('div');
  tip.className = 'hint';
  tip.style.marginTop = '4px';
  tip.textContent = '点上面的模型名，加入 / 移出「接管模型」输入框';
  $('ch-msg').appendChild(tip);
  box.querySelectorAll('.pick-model').forEach(t => {
    t.onclick = () => {
      const inp = $('ch-models');
      const arr = inp.value.split(/[,，]/).map(s => s.trim()).filter(Boolean);
      const m = t.dataset.m;
      const i = arr.findIndex(x => x.toLowerCase() === m.toLowerCase());
      if (i >= 0) {
        arr.splice(i, 1);
        t.style.background = '#f3f4f6'; t.style.borderColor = '#e5e7eb';
      } else {
        arr.push(m);
        t.style.background = '#dbeafe'; t.style.borderColor = '#93c5fd';
      }
      inp.value = arr.join(', ');
    };
  });
  // 本次测出的模型也加进「接管模型」下拉，方便直接选
  const sel = $('ch-models-pick');
  if (sel) {
    const have = new Set(Array.prototype.map.call(sel.options, o => o.value));
    ids.forEach(m => {
      if (!have.has(m)) {
        const op = document.createElement('option');
        op.value = m; op.textContent = m;
        sel.appendChild(op);
      }
    });
  }
}

// ===== 区块折叠 =====
function initCollapsible() {
  document.querySelectorAll('.section, .foldable-card').forEach(sec => {
    const h2 = sec.querySelector('h2');
    if (!h2) return;
    // 用标题文本做 key（去掉数字/空格/括号），顺序变化不影响
    const storeKey = 'lb2a-collapse2-' + h2.textContent.trim().replace(/[0-9\s()（）]/g, '');
    // 把 h2 之后的内容包进 .sec-body
    const body = document.createElement('div');
    body.className = 'sec-body';
    while (h2.nextSibling) body.appendChild(h2.nextSibling);
    sec.appendChild(body);
    // 默认全部展开（切页后能直接看到内容），只有用户手动收起过才记住
    try {
      if (localStorage.getItem(storeKey) === '1') sec.classList.add('collapsed');
    } catch(e) {}
    // 点击标题切换
    h2.onclick = () => {
      sec.classList.toggle('collapsed');
      try {
        localStorage.setItem(storeKey, sec.classList.contains('collapsed') ? '1' : '0');
      } catch(e) {}
    };
  });
}

initCollapsible();

// ===== 接口地址（Base URL）=====
// 口径改成「面板自己这个地址 + /v1」（面板已经把 /v1 透明转发给核心了，见 Go 侧 v1proxy）：
//   · 从 IP 打开   → http://124.221.39.166:8368/v1
//   · 从隧道域名打开 → https://xxx.trycloudflare.com/v1   ← 不再是数字
// 好处：隧道/域名一换，这个地址自动跟着走，不用手改；客户端只填面板地址 + /v1。
(function initApiBase() {
  const origin = (location.origin && location.origin !== 'null')
    ? location.origin
    : ('http://' + (location.hostname || '127.0.0.1') + ':8368');
  const base = origin + '/v1';
  const el = $('api-base');
  if (el) el.textContent = base;
  const full = $('api-full');
  if (full) full.textContent = '对话 ' + base + '/chat/completions · 模型列表 ' + base + '/models';
})();
$('copy-base').onclick = async () => {
  const base = $('api-base').textContent;
  const ok = await copyText(base, $('api-base'));
  $('keymsg').innerHTML = ok
    ? '<span class="msg-assistant">已复制接口地址: ' + base + '</span>'
    : '<span class="err">复制被浏览器拦了，已帮你选中地址，按 Ctrl+C 即可</span>';
};
$('test-base').onclick = async () => {
  $('keymsg').innerHTML = '<span style="color:#2563eb">测试中...</span>';
  try {
    // 浏览器跨端口会被 CORS 拦，所以走面板自己的代理探测后端
    const r = await fetch('/api/models');
    const d = await r.json();
    const n = (d.data || []).length;
    $('keymsg').innerHTML = n
      ? '<span class="msg-assistant">后端正常，模型列表返回 ' + n + ' 个</span>'
      : '<span class="err">返回异常: ' + escHtml(JSON.stringify(d).slice(0, 160)) + '</span>';
  } catch(e) {
    $('keymsg').innerHTML = '<span class="err">探测失败: ' + escHtml(e.message) + '</span>';
  }
};

// ===== 侧边栏切换：点哪个模块，下面就显示哪个 =====
// 页头标题 + 副标题（2026-09-22 照参考站样式做：左边大标题，右边状态与用户牌）
const PAGE_HEAD = {
  overview: ['数据概览', '欢迎回来！这是您账户的概览。'],
  pool:     ['账号池',   '龙虾账号自动轮换选号，冻结/解冻在这里。'],
  addacc:   ['添加账号', '粘贴 auth JSON 即可加入号池，热生效不重启。'],
  relay:    ['上游反代', '粘贴别人的中转，按模型名接管流量。'],
  phone:    ['手机登录', '扫码或短信登录上游账号。'],
  apikey:   ['API Key',  '发给客户用的 key、限速与额度都在这里。'],
  chat:     ['模型测试', '试一下某个模型通不通。'],
  visitors: ['访问 IP',  '谁在访问这个面板，可禁用或删除。'],
  admin:    ['后台管理', '面板自己的门锁：登录、验证码、账号密码、登录记录。']
};
function setPageHead(id) {
  const h = PAGE_HEAD[id] || PAGE_HEAD.overview;
  const t = $('page-title'), s = $('page-sub');
  if (t) t.textContent = h[0];
  if (s) s.textContent = h[1];
}
function showPage(id) {
  setPageHead(id);
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('on', p.dataset.page === id));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('on', n.dataset.page === id));
  // 2026-09-22 §113 手机端：导航是横滑条，切页后把当前项滑进视野（桌面端不是横滑，这里什么都不做）
  try {
    const bar = document.querySelector('.sidebar'), el = document.querySelector('.nav-item.on');
    if (bar && el && bar.scrollWidth > bar.clientWidth + 4) {
      const r = el.getBoundingClientRect(), b = bar.getBoundingClientRect();
      bar.scrollBy({ left: (r.left + r.width / 2) - (b.left + b.width / 2), behavior: 'smooth' });
    }
  } catch(e) {}
  try { localStorage.setItem('lb2a-page', id); } catch(e) {}
}
  document.querySelectorAll('.nav-item').forEach(n => n.onclick = () => {
    showPage(n.dataset.page);
    if (n.dataset.page === 'visitors') loadVisitors();
    if (n.dataset.page === 'admin') loadAdmin();
    window.scrollTo(0, 0);
  });
  // 左侧导航支持随意拖动排序（顺序记在 localStorage，刷新后保持）
  bindNavDrag();
let startPage = 'overview';
try {
  const sp = localStorage.getItem('lb2a-page');
  if (sp && document.querySelector('.page[data-page="' + sp + '"]')) startPage = sp;
} catch(e) {}
showPage(startPage);

// ===== 按天明细折叠（默认收起，状态记住）=====
// 按天明细改成常驻显示，不再需要折叠逻辑

  $('fb-head').onclick = () => {
    const el = $('fb-list');
    const open = el.style.display === 'none';
    el.style.display = open ? 'block' : 'none';
    $('fb-tri').innerHTML = open ? '&#9662;' : '&#9656;';
  };

  // 「最近访问明细」折叠：默认展开，收起状态记在本地（跟"最近回退明细"一个交互）
  const vlHead = $('vis-log-head');
  if (vlHead) {
    let vlFolded = false;
    try { vlFolded = localStorage.getItem('lb2a-vislog-folded') === '1'; } catch(e) {}
    const applyVlFold = () => {
      const el = $('vis-log');
      if (el) el.style.display = vlFolded ? 'none' : 'block';
      const tri = $('vis-log-tri');
      if (tri) tri.innerHTML = vlFolded ? '&#9656;' : '&#9662;';
    };
    vlHead.onclick = () => {
      vlFolded = !vlFolded;
      try { localStorage.setItem('lb2a-vislog-folded', vlFolded ? '1' : '0'); } catch(e) {}
      applyVlFold();
    };
    applyVlFold();
  }

  loadStatus(); loadModels(); loadKeys(); loadUsage(); loadChannels(); loadVisitors();
  // 页签在后台就别轮询了（手机切走/锁屏时省流量也省电）
  setInterval(() => { if (!document.hidden) loadStatus(); }, 30000);
  // 反代卡片的账号/余额/今日用量也跟着刷新（后端 60 秒缓存，内容没变不碰 DOM）
  setInterval(() => { if (!document.hidden) fillChanAccounts(); }, 30000);
  setInterval(() => { if (!document.hidden) loadKeys(); }, 30000);
  setInterval(() => { if (!document.hidden) loadUsage(); }, 30000);
  setInterval(() => { if (!document.hidden) loadVisitors(); }, 30000);
  // 从后台切回来立刻补一次，别等下一个 30 秒
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { loadStatus(); loadUsage(); } });
  const visRefresh = $('vis-refresh');
  if (visRefresh) visRefresh.onclick = () => loadVisitors();

// 手动刷新积分（强制回源）
$('refresh-credits').onclick = async () => {
  const btn = $('refresh-credits');
  btn.disabled = true;
  btn.textContent = '刷新中...';
  await loadStatus(true);
  await renderCreditCard(true);   // 选的是反代余额时，这里才是真正回源的那一步
  btn.disabled = false;
  btn.textContent = '刷新';
};

// ===== 登录态相关（2026-09-22 加面板登录）=====
// ===== 后台管理页（面板自己的门锁：登录开关 / 验证码开关 / 账号密码 / 解锁 IP / 登录记录）=====
function admResultName(r) {
  return ({ ok: '✅ 登录成功', bad_pass: '❌ 密码不对', bad_captcha: '❌ 验证码不对',
            locked: '🚫 已被锁定', rate: '🚫 太频繁' })[r] || r;
}
let admState = null;
async function loadAdmin() {
  const box = $('adm-body');
  if (!box) return;
  box.innerHTML = '<div class="meta">加载中...</div>';
  let d = null;
  try { d = await (await fetch('/api/admin/state')).json(); } catch(e) {}
  if (!d || !d.ok) { box.innerHTML = '<div class="err">读取失败（会话可能过期，刷新页面重新登录）</div>'; return; }
  admState = d;
  const lim = d.limits || {};
  const locked = d.locked || [];
  const logins = d.logins || [];
  const sw = function(id, on, label, tip) {
    return '<div style="display:flex;align-items:center;gap:8px;margin:6px 0;flex-wrap:wrap">'
      + '<button class="gray mini" id="' + id + '">' + (on ? '关闭' : '开启') + '</button>'
      + '<span><b>' + label + '</b>：' + (on ? '<span class="pill green">已开启</span>' : '<span class="pill gray">已关闭</span>') + '</span>'
      + '<span class="hint" style="margin:0">' + tip + '</span></div>';
  };
  box.innerHTML = ''
    + sw('adm-login', !!d.login_enabled, '面板登录', '关掉之后谁打开面板都不用密码（慎用）')
    + sw('adm-cap', !!d.captcha_enabled, '数字验证码', '关掉之后登录页不显示那 4 位数字')
    + '<div class="ch-line" style="margin-top:10px">当前账号：<b>' + escHtml(d.user) + '</b>'
      + ' <span class="pill gray" title="按建号顺序排：第一个账号是 1，右上角牌子上显示的也是它">注册序号 第 ' + (d.serial || 1) + ' 个</span>'
      + (d.changed ? ' <span class="pill gray">已改过密码</span>' : ' <span class="pill yellow">还在用初始密码</span>') + '</div>'
    + '<div class="add-form" style="margin-top:10px; max-width:680px">'
      + '<div><label>改账号（留空 = 不改）</label><input id="adm-user" placeholder="新账号名"></div>'
      + '<div><label>旧密码（改账号/改密码必填）</label><input id="adm-old" type="password"></div>'
      + '<div><label>新密码（至少 6 位，留空 = 不改）</label><input id="adm-new1" type="password"></div>'
      + '<div><label>再输一遍新密码</label><input id="adm-new2" type="password"></div>'
      + '<div class="full"><button id="adm-save">保存账号 / 密码</button></div>'
    + '</div>'
    + '<div class="ch-line" style="margin-top:14px">防护阈值（当前生效值；要改就设环境变量后重启面板）：'
      + '登录 <b>' + lim.login_rpm + '</b> 次/分 · 发码 <b>' + lim.captcha_rpm + '</b> 次/分 · 连错 <b>' + lim.fails
      + '</b> 次锁 <b>' + lim.lock_min + '</b> 分钟</div>'
    + '<div style="margin-top:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">'
      + '<button class="gray mini" id="adm-unlock">解锁所有 IP</button>'
      + '<span class="hint" style="margin:0">' + (locked.length
          ? ('当前被锁：' + locked.map(function(x) { return escHtml(x.ip) + '（剩 ' + Math.round((x.remain_sec || 0) / 60) + ' 分钟）'; }).join('、'))
          : '当前没有被锁的 IP') + '</span></div>'
    + '<div style="margin-top:16px"><div class="meta">最近登录记录（新的在上，最多 50 条；记在 data/panel-logins.json）</div>'
      + '<div class="kw-tablewrap" style="margin-top:6px"><table class="kw-table"><thead><tr>'
      + '<th>时间</th><th>IP</th><th>结果</th><th>UA</th></tr></thead><tbody>'
      + (logins.length ? logins.map(function(r) {
          return '<tr><td class="kw-c-time">' + escHtml(String(r.at || '').replace('T', ' ').slice(0, 19)) + '</td>'
            + '<td>' + escHtml(r.ip || '') + '</td>'
            + '<td>' + escHtml(admResultName(r.result)) + '</td>'
            + '<td class="kw-c-msg" title="' + escHtml(r.ua || '') + '">' + escHtml(String(r.ua || '').slice(0, 60)) + '</td></tr>';
        }).join('') : '<tr><td colspan="4" class="kw-c-time">还没有记录</td></tr>')
      + '</tbody></table></div></div>';

  const say = function(html) { const m = $('adm-msg'); if (m) m.innerHTML = html; };
  const setOpt = async function(body) {
    let r = null;
    try {
      r = await (await fetch('/api/admin/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)})).json();
    } catch(e) { say('<span class="err">' + escHtml(e.message) + '</span>'); return; }
    say('<span class="' + ((r && r.ok) ? 'msg-assistant' : 'err') + '">' + escHtml((r && (r.message || r.error)) || '操作失败') + '</span>');
    if (r && r.relogin) { setTimeout(function() { location.href = '/login'; }, 1200); return; }
    loadAdmin();
  };
  if ($('adm-login')) $('adm-login').onclick = function() { setOpt({login_enabled: !d.login_enabled}); };
  if ($('adm-cap')) $('adm-cap').onclick = function() { setOpt({captcha_enabled: !d.captcha_enabled}); };
  if ($('adm-unlock')) $('adm-unlock').onclick = async function() {
    let r = null;
    try { r = await (await fetch('/api/admin/unlock', {method:'POST'})).json(); } catch(e) {}
    say('<span class="' + ((r && r.ok) ? 'msg-assistant' : 'err') + '">' + escHtml((r && r.message) || '操作失败') + '</span>');
    loadAdmin();
  };
  if ($('adm-save')) $('adm-save').onclick = function() {
    const u = ($('adm-user').value || '').trim();
    const o = $('adm-old').value || '';
    const n1 = $('adm-new1').value || '';
    const n2 = $('adm-new2').value || '';
    if (!u && !n1) { say('<span class="err">没有要保存的改动（账号和新密码都空着）</span>'); return; }
    if (n1 && n1.length < 6) { say('<span class="err">新密码至少 6 位</span>'); return; }
    if (n1 !== n2) { say('<span class="err">两次输入的新密码不一样</span>'); return; }
    if (!o) { say('<span class="err">改账号 / 改密码都要先填旧密码</span>'); return; }
    const body = {user: u, old_pass: o, new_pass: n1};
    showConfirm('', function() { setOpt(body); }, {
      title: '确认修改登录凭据？', okText: '确认修改',
      html: (u ? ('账号改成 <b>' + escHtml(u) + '</b><br>') : '')
        + (n1 ? '密码也一起改<br>' : '')
        + '<div class="hint" style="margin-top:6px">改完所有登录状态失效，需要用新凭据重新登录。</div>',
    });
  };
}
if ($('adm-refresh')) $('adm-refresh').onclick = function() { loadAdmin(); };
// 右上角用户牌（2026-09-22 用户要求）：绿圆显示"第几个用户"，右边账号名，点开有菜单
(async function loadMe() {
  const box = $('me-box');
  if (!box) return;
  try {
    const d = await (await fetch('/api/me')).json();
    if (!d || !d.ok) return;          // 没登录 / 登录已关 → 不显示
    $('me-serial').textContent = d.serial || 1;
    $('me-name').textContent = d.user || 'admin';
    box.style.display = 'flex';
  } catch(e) { return; }
  box.onclick = function(ev) {
    if (ev.target && ev.target.closest && ev.target.closest('.me-item')) return;
    $('me-menu').classList.toggle('on');
  };
  document.addEventListener('click', function(ev) {
    if (!box.contains(ev.target)) $('me-menu').classList.remove('on');
  });
  $('me-menu').querySelectorAll('.me-item').forEach(function(it) {
    it.onclick = function() {
      const act = it.dataset.go;
      $('me-menu').classList.remove('on');
      // 2026-09-22 §110：侧栏那两个按钮删了，菜单直接调函数
      if (act === 'passwd') { doPasswd(); return; }
      if (act === 'logout') { doLogout(); return; }
    };
  });
})();
// 登录被关掉时（data/panel-auth.json 里 disabled=true 或 LB2A_PANEL_AUTH=off）
// 就把「改密码 / 退出登录」藏起来 —— 点了也没意义
(async function() {
  try {
    const r = await fetch('/api/authstate');
    const d = await r.json();
    if (d && d.enabled === false) {
      ['btn-passwd', 'btn-logout'].forEach(function(id) {
        const el = $(id);
        if (el && el.parentElement) el.parentElement.style.display = 'none';
      });
    }
  } catch(e) {}
})();
// 退出登录（2026-09-22 §110：改成具名函数，右上角用户牌菜单直接调）
async function doLogout() {
  try { await fetch('/api/logout', {method:'POST'}); } catch(e) {}
  location.href = '/login';
}
// 改密码（旧密码 + 新密码两遍；改完全部会话失效）
function doPasswd() {
  const inp = 'width:100%;margin-top:6px;background:#ffffff;border:1px solid #d1d5db;border-radius:8px;color:#111827;padding:9px 12px;font-size:13px';
  showConfirm('', async () => {
    const o = $('pw-old') ? $('pw-old').value : '';
    const n1 = $('pw-new') ? $('pw-new').value : '';
    const n2 = $('pw-new2') ? $('pw-new2').value : '';
    if (n1.length < 6) { alert('新密码至少 6 位'); return; }
    if (n1 !== n2) { alert('两次输入的新密码不一样'); return; }
    let d = null;
    try {
      const r = await fetch('/api/passwd', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({old: o, new: n1})});
      d = await r.json();
    } catch(e) { alert('请求失败：' + e.message); return; }
    if (d && d.ok) {
      alert(d.message || '密码已改，请重新登录');
      location.href = '/login';
      return;
    }
    alert((d && (d.message || d.error)) || '改密码失败');
  }, {
    title: '改密码',
    okText: '保存',
    keep: true,
    html: '<div style="text-align:left;font-size:13px">'
      + '<div>旧密码</div><input id="pw-old" type="password" style="' + inp + '">'
      + '<div style="margin-top:8px">新密码（至少 6 位）</div><input id="pw-new" type="password" style="' + inp + '">'
      + '<div style="margin-top:8px">再输一遍新密码</div><input id="pw-new2" type="password" style="' + inp + '">'
      + '<div class="hint" style="margin-top:8px">改完所有已登录会话都会失效，需要用新密码重新登录（手机、其它浏览器也一样）。</div>'
      + '</div>',
  });
}
// 兼容：老页面若还留着这两个按钮（缓存里的旧 HTML），照样能点（§110 后侧栏已删）
if ($('btn-logout')) $('btn-logout').onclick = doLogout;
if ($('btn-passwd')) $('btn-passwd').onclick = doPasswd;
// 会话过期兜底：任何 /api/* 回 401 → 跳登录页（登录 POST 自己除外，不然会打转）
(function() {
  const orig = window.fetch;
  window.fetch = function(input, init) {
    const url = (typeof input === 'string') ? input : ((input && input.url) || '');
    return orig.apply(this, arguments).then(function(r) {
      if (r.status === 401 && url.indexOf('/api/login') < 0) {
        setTimeout(function() {
          location.href = '/login?next=' + encodeURIComponent(location.pathname + location.search);
        }, 60);
      }
      return r;
    });
  };
})();
</script>
</body>
</html>`

// ---------------------------------------------------------------------------
// 传输层优化（2026-09-21）：面板 HTML 95,964 B、/api/usage 8.9 KB 原来全是明文发的，
// 手机在公网访问时每次都要原样搬一遍 —— 这才是真正的"卡顿"来源（不是 JS 慢：
// 实测切页 71~151ms、轮询函数 5~7ms、零长任务）。这里加两件事：
//   ① gzip（HTML 约 1/4，JSON 约 1/5）
//   ② HTML 走 ETag + 304：既不会看到旧页面，也不用每次重下 94 KB
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 访问 IP 记录（2026-09-21）：面板挂在公网上，得能一眼看清"谁在访问"。
// 按 (IP + UA) 各记一行 —— 同一台路由器后面的电脑和手机会是两条记录，
// 设备类型直接由 UA 判断，不用你自己认。
// ---------------------------------------------------------------------------

type visitorRec struct {
	IP       string    `json:"ip"`
	UA       string    `json:"ua"`
	Device   string    `json:"device"` // 电脑 / 手机 / 平板
	OS       string    `json:"os"`
	Browser  string    `json:"browser"`
	FirstAt  time.Time `json:"first_at"`
	LastAt   time.Time `json:"last_at"`
	Count    int       `json:"count"`
	LastPath string    `json:"last_path"`
	Local    bool      `json:"local"` // 本机 / 内网来源
	You      bool      `json:"you"`   // 是不是发起这次查询的设备
	Blocked  bool      `json:"blocked"`
}

// visitLogEntry 一次"打开面板"的明细（API 轮询不记，否则会被刷屏）。
type visitLogEntry struct {
	At     time.Time `json:"at"`
	IP     string    `json:"ip"`
	Device string    `json:"device"`
	Path   string    `json:"path"`
}

const visitLogMax = 500

// loadVisitors 从磁盘恢复访问记录（2026-09-21：原来只在内存里，面板一重启就全丢，
// 用户手机访问过、我一发布就看不见了）。
func loadVisitors(path string) {
	visFile = path
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var d struct {
		Visitors []visitorRec    `json:"visitors"`
		Log      []visitLogEntry `json:"log"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	visMu.Lock()
	for i := range d.Visitors {
		v := d.Visitors[i]
		v.You, v.Blocked = false, false
		visits[v.IP+"|"+v.UA] = &v
	}
	visitLog = d.Log
	visMu.Unlock()
}

// saveVisitors 落盘（调用方不需要持锁）。
func saveVisitors() {
	if visFile == "" {
		return
	}
	visMu.Lock()
	list := make([]visitorRec, 0, len(visits))
	for _, v := range visits {
		list = append(list, *v)
	}
	logCopy := append([]visitLogEntry(nil), visitLog...)
	visDirty = false
	visMu.Unlock()
	sort.Slice(list, func(i, j int) bool { return list[i].LastAt.After(list[j].LastAt) })
	b, err := json.MarshalIndent(map[string]any{
		"visitors": list, "log": logCopy, "saved_at": time.Now(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(visFile), 0o755)
	_ = os.WriteFile(visFile, b, 0o644)
}

// flushVisitorsLoop 每 2 秒把脏数据落一次盘（访问很频繁，不能每次请求都写文件）。
func flushVisitorsLoop() {
	for range time.Tick(2 * time.Second) {
		visMu.Lock()
		dirty := visDirty
		visMu.Unlock()
		if dirty {
			saveVisitors()
		}
	}
}

// recentVisits 最近 n 条"打开面板"的明细（新的在前）。
func recentVisits(n int) []visitLogEntry {
	visMu.Lock()
	defer visMu.Unlock()
	out := make([]visitLogEntry, 0, n)
	for i := len(visitLog) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, visitLog[i])
	}
	return out
}

var (
	visMu    sync.Mutex
	visits   = map[string]*visitorRec{}
	visitLog = []visitLogEntry{} // 最近打开页面的明细（环形，最多 500 条）
	visFile  string
	visDirty bool

	// 拉黑的 IP（按 IP 拦，不看 UA）：面板挂公网，得能一键把可疑来源关在门外。
	// 落盘到 data/panel-blocked.json，重启不丢；万一把自己关外面了，
	// 删掉文件里那一条（或换台设备/本机访问）就能解禁。
	blockMu   sync.Mutex
	blockedIP = map[string]time.Time{}
	blockFile string
)

func loadBlocked(path string) {
	blockFile = path
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var d struct {
		IPs map[string]time.Time `json:"ips"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return
	}
	blockMu.Lock()
	blockedIP = d.IPs
	if blockedIP == nil {
		blockedIP = map[string]time.Time{}
	}
	blockMu.Unlock()
}

func saveBlocked() {
	if blockFile == "" {
		return
	}
	blockMu.Lock()
	b, err := json.MarshalIndent(map[string]any{"ips": blockedIP, "saved_at": time.Now()}, "", "  ")
	blockMu.Unlock()
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(blockFile), 0o755)
	_ = os.WriteFile(blockFile, b, 0o644)
}

func ipBlocked(ip string) bool {
	blockMu.Lock()
	defer blockMu.Unlock()
	_, ok := blockedIP[ip]
	return ok
}

func setBlocked(ip string, on bool) {
	blockMu.Lock()
	if on {
		blockedIP[ip] = time.Now()
	} else {
		delete(blockedIP, ip)
	}
	blockMu.Unlock()
	saveBlocked()
}

func blockedList() map[string]time.Time {
	blockMu.Lock()
	defer blockMu.Unlock()
	out := make(map[string]time.Time, len(blockedIP))
	for k, v := range blockedIP {
		out[k] = v
	}
	return out
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// classifyUA 从 User-Agent 猜设备/系统/浏览器（够用就行，不追求全）
func classifyUA(ua string) (device, osName, browser string) {
	l := strings.ToLower(ua)
	switch {
	case strings.Contains(l, "ipad") || strings.Contains(l, "tablet") || (strings.Contains(l, "android") && !strings.Contains(l, "mobile")):
		device = "平板"
	case strings.Contains(l, "mobile") || strings.Contains(l, "android") || strings.Contains(l, "iphone") || strings.Contains(l, "micromessenger") || strings.Contains(l, "harmony"):
		device = "手机"
	default:
		device = "电脑"
	}
	switch {
	case strings.Contains(l, "windows"):
		osName = "Windows"
	case strings.Contains(l, "harmony"):
		osName = "HarmonyOS"
	case strings.Contains(l, "android"):
		osName = "Android"
	case strings.Contains(l, "iphone") || strings.Contains(l, "ipad") || strings.Contains(l, "cpu os"):
		osName = "iOS"
	case strings.Contains(l, "mac os"):
		osName = "macOS"
	case strings.Contains(l, "linux"):
		osName = "Linux"
	}
	switch {
	case strings.Contains(l, "micromessenger"):
		browser = "微信"
	case strings.Contains(l, "edg/"):
		browser = "Edge"
	case strings.Contains(l, "firefox/"):
		browser = "Firefox"
	case strings.Contains(l, "chrome/"):
		browser = "Chrome"
	case strings.Contains(l, "safari/"):
		browser = "Safari"
	}
	return
}

func isLocalIP(ip string) bool {
	p := net.ParseIP(ip)
	if p == nil {
		return false
	}
	return p.IsLoopback() || p.IsPrivate() || p.IsLinkLocalUnicast()
}

// trackVisitor 记一次访问。挂在最外层中间件上，HTML 和 /api/* 都算。
func trackVisitor(r *http.Request) {
	ip, ua := clientIP(r), r.UserAgent()
	key := ip + "|" + ua
	dev, osName, br := classifyUA(ua)
	now := time.Now()
	visMu.Lock()
	defer visMu.Unlock()
	rec := visits[key]
	if rec == nil {
		// 兜底别把内存/文件撑爆：满 2000 条时只丢"最久没来"的那一条。
		// （原来超过 300 条就清 24 小时没动的 —— 用户要求"留下所有访问的"，所以放宽）
		if len(visits) >= 2000 {
			oldestKey, oldest := "", time.Time{}
			for k, v := range visits {
				if oldestKey == "" || v.LastAt.Before(oldest) {
					oldestKey, oldest = k, v.LastAt
				}
			}
			delete(visits, oldestKey)
		}
		rec = &visitorRec{IP: ip, UA: ua, FirstAt: now, Local: isLocalIP(ip)}
		visits[key] = rec
	}
	rec.Device, rec.OS, rec.Browser = dev, osName, br
	rec.LastAt = now
	rec.Count++
	rec.LastPath = r.URL.Path
	// 只在"打开面板"时记一条明细；API 轮询不记（否则几秒就把日志刷满）
	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		visitLog = append(visitLog, visitLogEntry{At: now, IP: ip, Device: dev, Path: r.URL.Path})
		if len(visitLog) > visitLogMax {
			visitLog = visitLog[len(visitLog)-visitLogMax:]
		}
	}
	visDirty = true
}

// blockedPage 被拉黑时返回的页面（写清怎么自救，免得把自己永久关在外面）
const blockedPage = `<!doctype html><meta charset="utf-8"><title>403</title>
<div style="font:14px/1.7 -apple-system,'Segoe UI','Microsoft YaHei',sans-serif;max-width:560px;margin:80px auto;padding:0 20px">
<h2 style="color:#b91c1c">403 · 这个 IP 已被面板拉黑</h2>
<p>该来源已不允许访问 BlueAPI 面板。</p>
<p style="color:#6b7280">要解禁：在服务器上编辑 <code>C:\lobsterai2api\data\panel-blocked.json</code>，
删掉 <code>ips</code> 里对应的 IP（或直接删掉该文件），然后重开浏览器即可。
也可以先用本机（127.0.0.1）或其它 IP 打开面板，在「访问 IP」页点「解禁」。</p>
</div>`

// trackHandler 记录访问 + 把被拉黑的 IP 挡在门外
func trackHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ip := clientIP(r); ipBlocked(ip) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, blockedPage)
			return
		}
		trackVisitor(r)
		next.ServeHTTP(w, r)
	})
}

// pageETag 面板 HTML 的内容指纹；内容一变指纹就变，浏览器自然拿不到旧页面。
var pageETag = func() string {
	sum := sha256.Sum256([]byte(page))
	return fmt.Sprintf(`"%x"`, sum[:8])
}()

type gzipResponseWriter struct {
	http.ResponseWriter
	gz       *gzip.Writer
	compress bool
	decided  bool
}

func (w *gzipResponseWriter) decide(code int, contentType string) {
	if w.decided {
		return
	}
	w.decided = true
	// 已经是压缩过的、无正文的（204/304）、非文本的，一律不压
	if w.Header().Get("Content-Encoding") != "" ||
		code == http.StatusNoContent || code == http.StatusNotModified || code < 200 {
		return
	}
	ct := strings.ToLower(contentType)
	// 事件流不压：SSE 要的是低延迟，压缩缓冲反而伤体验
	if strings.HasPrefix(ct, "text/event-stream") {
		return
	}
	w.compress = ct == "" ||
		strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "json") ||
		strings.Contains(ct, "javascript") ||
		strings.Contains(ct, "xml")
}

func (w *gzipResponseWriter) WriteHeader(code int) {
	w.decide(code, w.Header().Get("Content-Type"))
	if w.compress {
		w.Header().Del("Content-Length") // 长度变了，交给 gzip 自己定
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	if !w.decided {
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", http.DetectContentType(b))
		}
		w.WriteHeader(http.StatusOK)
	}
	if !w.compress {
		return w.ResponseWriter.Write(b)
	}
	if w.gz == nil {
		w.gz = gzip.NewWriter(w.ResponseWriter)
	}
	return w.gz.Write(b)
}

// Flush 必须透传，否则 ReverseProxy 的流式响应会被攒在缓冲区里
func (w *gzipResponseWriter) Flush() {
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *gzipResponseWriter) close() {
	if w.gz != nil {
		_ = w.gz.Close()
		w.gz = nil
	}
}

// gzipHandler 统一的压缩中间件（HTML + /api/* 代理回来的 JSON 都吃这一层）
func gzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Vary 要无条件加（不管这次压不压），否则中间缓存可能把 gzip 的那份
		// 发给"只认明文"的客户端
		w.Header().Add("Vary", "Accept-Encoding")
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		// 让上游直接发明文，由这一层统一压（避免上游压一遍、我再压一遍）
		r.Header.Set("Accept-Encoding", "identity")
		grw := &gzipResponseWriter{ResponseWriter: w}
		defer grw.close()
		next.ServeHTTP(grw, r)
	})
}

func main() {
	flag.Parse()
	// config.json 优先取 exe 同目录，其次 C:\lobsterai2api\
	if exe, err := os.Executable(); err == nil {
		if _, err := os.Stat(filepath.Join(filepath.Dir(exe), "config.json")); err == nil {
			configPath = filepath.Join(filepath.Dir(exe), "config.json")
		}
	}
	// 拉黑名单落盘到跟 config.json 同级的 data/ 下（跟主服务的 data 放一起）
	loadBlocked(filepath.Join(filepath.Dir(configPath), "data", "panel-blocked.json"))
	// 访问记录也落盘：以前只在内存，面板一重启用户刚用手机访问的记录就没了
	loadVisitors(filepath.Join(filepath.Dir(configPath), "data", "panel-visitors.json"))
	go flushVisitorsLoop()
	// 面板登录（2026-09-22）：之前面板裸奔 —— 知道 IP:8368 就能看/改一切。
	// 凭据跟其它数据放一起：data/panel-auth.json（首次启动自动生成，密码打进日志）
	initPanelAuth(filepath.Join(filepath.Dir(configPath), "data", "panel-auth.json"))
	// 登录记录（后台管理页要看"谁在试、锁了谁"）
	initLoginRecs(filepath.Join(filepath.Dir(configPath), "data", "panel-logins.json"))
	loadKeysFromConfig()
	log.Printf("api keys loaded: %d 个", len(getKeys()))

	target, _ := url.Parse(*upstream)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		req.Header.Set("Authorization", "Bearer "+proxyKey())
		// 面板自身的轮询请求（状态/模型列表/使用统计）标记为内部，不计入 key 使用次数
		switch req.URL.Path {
		case "/status", "/v1/models", "/models/pool", "/channels/balance", "/keys/stats":
			req.Header.Set("X-Lobster-Internal", "1")
		}
	}

	// 对外转发 /v1/*：让面板自己也能当 API 入口（2026-09-26 加，配合云隧道/域名）。
	// 为什么需要：挂了隧道之后一个地址就能同时进面板 + 调 API，
	// 客户端 Base URL 直接填 https://<隧道域名>/v1，不用再暴露 8367 那个数字端口。
	// 与上面 /api/ 代理的关键区别：**不动 Authorization** —— 用客户端带来的 sk- key，
	// 核心那边照旧校验；不然隧道地址会变成"谁拿到谁白嫖"的开放口子。
	v1proxy := httputil.NewSingleHostReverseProxy(target)
	v1proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}
	v1proxy.FlushInterval = -1 // SSE 流式必须立刻刷，不能攒着

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			// 2026-09-22 改成 no-store：今天连着两次踩到"明明部署了新面板，浏览器却在用旧页面"
			// （no-cache 要求回源校验，但实测在部分情况下仍会拿到旧的）。
			// 代价是每次重下这份 HTML —— 它已经过 gzip（~30KB），面板就自己在用，值。
			w.Header().Set("Cache-Control", "no-store, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("ETag", pageETag)
			if inm := r.Header.Get("If-None-Match"); inm != "" && strings.Contains(inm, pageETag) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			io.WriteString(w, page)
			return
		}
		http.NotFound(w, r)
	})

	// 后端代理：/api/* → :8367/*（前端友好路径映射）
	http.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		p := strings.TrimPrefix(r.URL.Path, "/api")
		switch p {
		case "/status", "/healthz":
			// 原样
		case "/models":
			p = "/v1/models"
		case "/poolmodels":
			// 只返回龙虾账号池自己的模型（不含反代接管），模型测试用它跟反代分组分开
			p = "/models/pool"
		case "/channelbalance":
			// 反代账户余额 / 用量（积分卡切来源用）
			p = "/channels/balance"
		case "/channelslogin":
			// 给反代配「后台登录态」（读账户余额用）
			p = "/channels/login"
		case "/chat":
			p = "/v1/chat/completions"
		case "/keyusage":
			p = "/keys/stats"
		case "/callslog":
			p = "/calls/log"
		case "/usage":
			p = "/usage/stats"
		}
		r2.URL.Path = p
		proxy.ServeHTTP(w, r2)
	})

	// /v1/*：原样透传给核心（流式友好）。鉴权靠客户端的 sk- key，不看面板登录态。
	http.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		v1proxy.ServeHTTP(w, r.Clone(r.Context()))
	})

	// 健康检查
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	// 登录相关（这几个路径被 authGate 放行，详情见 auth.go）
	http.HandleFunc("/login", handleLoginPage)
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/logout", handleLogout)
	http.HandleFunc("/api/passwd", handlePasswd)
	http.HandleFunc("/api/authstate", handleAuthState)
	http.HandleFunc("/api/captcha", handleCaptcha)
	// 后台管理页（改账号/密码、开关登录与验证码、解锁 IP、看登录记录）
	http.HandleFunc("/api/admin/state", handleAdminState)
	http.HandleFunc("/api/admin/set", handleAdminSet)
	http.HandleFunc("/api/admin/unlock", handleAdminUnlock)
	http.HandleFunc("/api/me", handleMe)

	// 实时积分：读 auths token 问上游（带 60s 缓存，?force=1 强制回源）
	http.HandleFunc("/api/credits", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 2026-09-22 §111：以前不挑方法，DELETE/PUT 也能打（?force=1 还会真回源问上游）。
		// 这个接口只读，只认 GET。
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "GET only"})
			return
		}
		force := r.URL.Query().Get("force") == "1"
		credits, at, cached := liveCreditsCached(force)
		json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"credits":    credits,
			"cached_at":  at,
			"from_cache": cached,
		})
	})

	// 访问 IP 列表：谁在用电脑、谁在用手机访问这个面板
	http.HandleFunc("/api/visitors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		ip, ua := clientIP(r), r.UserAgent()
		visMu.Lock()
		list := make([]visitorRec, 0, len(visits))
		for _, v := range visits {
			cp := *v
			cp.You = (cp.IP == ip && cp.UA == ua)
			cp.Blocked = ipBlocked(cp.IP)
			list = append(list, cp)
		}
		total := len(visits)
		visMu.Unlock()
		ipSet := map[string]bool{}
		devCount := map[string]int{}
		for _, v := range list {
			ipSet[v.IP] = true
			devCount[v.Device]++
		}
		sort.Slice(list, func(i, j int) bool { return list[i].LastAt.After(list[j].LastAt) })
		youDev, _, _ := classifyUA(ua)
		json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "visitors": list, "total": total,
			"distinct_ips": len(ipSet), "devices": devCount,
			"you":          map[string]string{"ip": ip, "device": youDev},
			"blocked_list": blockedList(),
			"log":          recentVisits(100),
			"now":          time.Now(),
		})
	})

	// 拉黑 / 解禁一个 IP（按 IP 拦，不看设备）
	http.HandleFunc("/api/visitors/block", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "POST only"})
			return
		}
		var req struct {
			IP string `json:"ip"`
			On *bool  `json:"on"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		ip := strings.TrimSpace(req.IP)
		if ip == "" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "ip required"})
			return
		}
		// 2026-09-22 §111：以前不校验，随手传个 "not-an-ip" 也会写进黑名单文件。
		// 现在必须是合法的 IPv4/IPv6（带端口的 "1.2.3.4:5678" 也顺手剥掉端口）。
		if h, _, err := net.SplitHostPort(ip); err == nil {
			ip = h
		}
		if net.ParseIP(ip) == nil {
			json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "error": "invalid_ip",
				"message": "这不像一个 IP 地址：" + req.IP,
			})
			return
		}
		on := true
		if req.On != nil {
			on = *req.On
		}
		// 别把自己关在门外：当前请求用的 IP 一律不许拉黑
		if on && ip == clientIP(r) {
			json.NewEncoder(w).Encode(map[string]any{
				"ok":    false,
				"error": "这是你当前正在用的 IP，禁掉你自己也进不来了。要禁它请换一台设备（比如用手机）来操作。",
			})
			return
		}
		setBlocked(ip, on)
		log.Printf("[visitors] IP %s %s", ip, map[bool]string{true: "已拉黑", false: "已解禁"}[on])
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "ip": ip, "blocked": on})
	})

	// 删掉一条访问记录（只是清历史；该 IP 若被拉黑仍然是拦着的）
	http.HandleFunc("/api/visitors/delete", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "POST only"})
			return
		}
		var req struct {
			IP string `json:"ip"`
			UA string `json:"ua"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		visMu.Lock()
		delete(visits, req.IP+"|"+req.UA)
		visMu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// 服务器扫码登录：POST 触发（助手 /begin → 浏览器控制器 /launch）
	http.HandleFunc("/api/scanlogin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "POST only"})
			return
		}
		bodyBytes, _ := io.ReadAll(r.Body)
		// 浏览器控制器自己生成 state/登录链接并打开页面
		resp, err := http.Post("http://127.0.0.1:8370/launch", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "浏览器控制器不可用: " + err.Error()})
			return
		}
		defer resp.Body.Close()
		var lr struct {
			Ok    bool   `json:"ok"`
			State string `json:"state"`
			Error string `json:"error"`
		}
		if json.NewDecoder(resp.Body).Decode(&lr) != nil || !lr.Ok {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": lr.Error})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "state": lr.State})
	})

	// 二维码图片 + 登录状态（代理到浏览器控制器）
	http.HandleFunc("/api/scanqr", func(w http.ResponseWriter, r *http.Request) {
		resp, err := http.Get("http://127.0.0.1:8370/qr.png")
		if err != nil {
			http.Error(w, "qr not ready", http.StatusNotFound)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-store")
		io.Copy(w, resp.Body)
	})

	http.HandleFunc("/api/scanstate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp, err := http.Get("http://127.0.0.1:8370/state")
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		defer resp.Body.Close()
		io.Copy(w, resp.Body)
	})

	// 手机验证码登录：填手机号 / 填验证码（代理到浏览器控制器）
	http.HandleFunc("/api/scanphone", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "POST only"})
			return
		}
		resp, err := http.Post("http://127.0.0.1:8370/fill-phone", "application/json", r.Body)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		defer resp.Body.Close()
		io.Copy(w, resp.Body)
	})

	http.HandleFunc("/api/scansms", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "POST only"})
			return
		}
		resp, err := http.Post("http://127.0.0.1:8370/fill-sms", "application/json", r.Body)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		defer resp.Body.Close()
		io.Copy(w, resp.Body)
	})

	// API Key 管理：GET 列表 / POST 追加（__generate__ 随机生成） / DELETE 删除
	http.HandleFunc("/api/apikey", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case "GET":
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "keys": getKeys()})
		case "POST":
			var body struct {
				Key string `json:"key"`
			}
			raw, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(raw, &body); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 解析失败: " + err.Error()})
				return
			}
			newKey := strings.TrimSpace(body.Key)
			if newKey == "__generate__" {
				newKey = genKey()
			}
			if newKey == "" {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Key 不能为空"})
				return
			}
			cur := getKeys()
			for _, k := range cur {
				if k == newKey {
					json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "该 Key 已存在"})
					return
				}
			}
			next := append(append([]string{}, cur...), newKey)
			if err := saveKeysToConfig(next); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "写 config.json 失败: " + err.Error()})
				return
			}
			setKeys(next)
			if err := reloadMain(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "keys": next, "warning": "已保存但热重载失败: " + err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "keys": next, "message": "Key 已添加（历史保留），已热生效（未重启）"})

		case "DELETE":
			target := r.URL.Query().Get("key")
			if target == "" {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "缺少 key 参数"})
				return
			}
			cur := getKeys()
			next := []string{}
			found := false
			for _, k := range cur {
				if k == target {
					found = true
					continue
				}
				next = append(next, k)
			}
			if !found {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "找不到该 Key"})
				return
			}
			if len(next) == 0 {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "至少保留一个 Key"})
				return
			}
			if err := saveKeysToConfig(next); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "写 config.json 失败: " + err.Error()})
				return
			}
			setKeys(next)
			if err := reloadMain(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "keys": next, "warning": "已删除但热重载失败: " + err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "keys": next, "message": "Key 已删除，已热生效（未重启）"})
		default:
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "method not allowed"})
		}
	})

	// 账号管理：POST 添加 / DELETE 删除，写 auths 后重启主服务
	http.HandleFunc("/api/accounts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		authDir := "auths"
		if abs, err := filepath.Abs(authDir); err == nil {
			authDir = abs
		}

		switch r.Method {
		case "POST":
			var a struct {
				AccessToken   string `json:"accessToken"`
				RefreshToken  string `json:"refreshToken"`
				ExpiresAt     int64  `json:"expiresAt"`
				UID           string `json:"uid"`
				Nickname      string `json:"nickname"`
				Uuid          string `json:"uuid"`
				FirstKeyfrom  string `json:"firstKeyfrom"`
				LatestKeyfrom string `json:"latestKeyfrom"`
			}
			body, _ := io.ReadAll(r.Body)
			// 兼容嵌套形 {"auth":{...},"account":{...}} 与扁平形
			var probe map[string]json.RawMessage
			if err := json.Unmarshal(body, &probe); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 解析失败: " + err.Error()})
				return
			}
			if _, nested := probe["auth"]; nested {
				var n struct {
					Auth struct {
						AccessToken   string `json:"accessToken"`
						RefreshToken  string `json:"refreshToken"`
						ExpiresAt     int64  `json:"expiresAt"`
						Uuid          string `json:"uuid"`
						FirstKeyfrom  string `json:"firstKeyfrom"`
						LatestKeyfrom string `json:"latestKeyfrom"`
					} `json:"auth"`
					Account struct {
						UID      string `json:"uid"`
						UserId   string `json:"userId"`
						Nickname string `json:"nickname"`
					} `json:"account"`
				}
				if err := json.Unmarshal(body, &n); err != nil {
					json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "嵌套形解析失败: " + err.Error()})
					return
				}
				a.AccessToken = n.Auth.AccessToken
				a.RefreshToken = n.Auth.RefreshToken
				a.ExpiresAt = n.Auth.ExpiresAt
				a.Uuid = n.Auth.Uuid
				a.FirstKeyfrom = n.Auth.FirstKeyfrom
				a.LatestKeyfrom = n.Auth.LatestKeyfrom
				a.UID = n.Account.UID
				a.Nickname = n.Account.Nickname
			} else if err := json.Unmarshal(body, &a); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 解析失败: " + err.Error()})
				return
			}
			if a.AccessToken == "" || a.RefreshToken == "" {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "accessToken 和 refreshToken 必填"})
				return
			}
			if a.UID == "" {
				a.UID = "acc-" + strings.ReplaceAll(time.Now().Format("150405"), ":", "")
			}
			if a.FirstKeyfrom == "" {
				a.FirstKeyfrom = "official"
			}
			if a.LatestKeyfrom == "" {
				a.LatestKeyfrom = "official"
			}
			if a.ExpiresAt <= 0 {
				a.ExpiresAt = time.Now().Add(30 * 24 * time.Hour).Unix()
			}
			_ = os.MkdirAll(authDir, 0755)
			fp := filepath.Join(authDir, "lobsterai-"+a.UID+".json")
			data, _ := json.MarshalIndent(a, "", "  ")
			if err := os.WriteFile(fp, data, 0644); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "写文件失败: " + err.Error()})
				return
			}
			if err := reloadMain(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "file": fp, "error": "已写入但热重载失败: " + err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "file": fp, "message": "账号已添加，已热生效（未重启）"})

		case "DELETE":
			uid := r.URL.Query().Get("uid")
			if uid == "" {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "缺少 uid 参数"})
				return
			}
			fp := filepath.Join(authDir, "lobsterai-"+uid+".json")
			if _, err := os.Stat(fp); os.IsNotExist(err) {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "找不到该账号文件: " + fp})
				return
			}
			if err := os.Remove(fp); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "删除失败: " + err.Error()})
				return
			}
			if err := reloadMain(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "已删除但热重载失败: " + err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "message": "账号已删除，已热生效（未重启）"})

		default:
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "method not allowed"})
		}
	})

	log.Printf("panel listening on %s -> %s", *listen, *upstream)

	// TCP 隧道网关：:8369 → 127.0.0.1:18367（服务器上的扫码回调助手）
	// 用户本机隧道客户端连到这里，把浏览器对 127.0.0.1:18367 的访问转发给助手
	go func() {
		ln, err := net.Listen("tcp", ":8369")
		if err != nil {
			log.Printf("tunnel gateway listen: %v", err)
			return
		}
		log.Printf("tunnel gateway on :8369 -> 127.0.0.1:18367")
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				up, err := net.DialTimeout("tcp", "127.0.0.1:18367", 5*time.Second)
				if err != nil {
					return
				}
				defer up.Close()
				go func() {
					_, _ = io.Copy(up, c)
					_ = up.Close()
				}()
				_, _ = io.Copy(c, up)
			}(c)
		}
	}()

	// 中间件顺序：记录/拉黑 → gzip → 登录闸门 → 业务路由
	// （闸门放最后一道，保证没登录的连 /api/* 都摸不到）
	// 中间件顺序：记录/拉黑 → gzip → 登录闸门 → panic 兜底 → 业务路由
	h := trackHandler(gzipHandler(authGate(panelRecover(http.DefaultServeMux))))

	// 可选：面板再加一个 HTTPS 监听（给域名 / 云隧道用，2026-09-26 加）。
	// 配置在 config.json 的 panel_tls.addr / cert / key；证书没就位就只跑 HTTP，不影响现有入口。
	tlsAddr, tlsCert, tlsKey := panelTLSSettings()
	if tlsAddr != "" {
		if panelFileExists(tlsCert) && panelFileExists(tlsKey) {
			go func() {
				log.Printf("[panel-tls] HTTPS 监听 %s   证书 %s", tlsAddr, tlsCert)
				if err := http.ListenAndServeTLS(tlsAddr, tlsCert, tlsKey, h); err != nil {
					log.Printf("[panel-tls] HTTPS 起不来: %v", err)
				}
			}()
		} else {
			log.Printf("[panel-tls] 配了 %s 但证书还没就位（cert=%s）→ 先只跑 HTTP", tlsAddr, tlsCert)
		}
	}
	log.Fatal(http.ListenAndServe(*listen, h))
}

// panelTLSSettings 读 config.json 里的 panel_tls（可选）：
//
//	{"panel_tls": {"addr": ":9443", "cert": "...fullchain.pem", "key": "...privkey.pem"}}
func panelTLSSettings() (addr, cert, key string) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return "", "", ""
	}
	var cfg struct {
		PanelTLS struct {
			Addr string `json:"addr"`
			Cert string `json:"cert"`
			Key  string `json:"key"`
		} `json:"panel_tls"`
	}
	if json.Unmarshal(raw, &cfg) != nil {
		return "", "", ""
	}
	return strings.TrimSpace(cfg.PanelTLS.Addr), strings.TrimSpace(cfg.PanelTLS.Cert), strings.TrimSpace(cfg.PanelTLS.Key)
}

func panelFileExists(p string) bool {
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

// psRestartMain 重启主服务的 PowerShell 脚本。
//
// 关键（2026-09-20 踩坑）：计划任务的 MultipleInstancesPolicy = IgnoreNew。
// 如果只 Stop-Process 再 Start-ScheduledTask，任务实例状态可能仍是 Running
// （任务管理器尚未感知进程退出），Start 会被 IgnoreNew 直接忽略 → 主服务永久起不来（失联）。
// 所以必须：① 先 Stop-ScheduledTask 把任务实例置回 Ready；② 杀进程；③ 再 Start；
// ④ 末尾做一次"进程存在性校验 + 兜底再启一次"。
const psRestartMain = "Stop-ScheduledTask -TaskName LobsterAI2API -ErrorAction SilentlyContinue; " +
	"Stop-Process -Name lobsterai2api -Force -ErrorAction SilentlyContinue; " +
	"Start-Sleep -Seconds 2; " +
	"Start-ScheduledTask -TaskName LobsterAI2API; " +
	"Start-Sleep -Seconds 3; " +
	"if (-not (Get-Process lobsterai2api -ErrorAction SilentlyContinue)) { Start-ScheduledTask -TaskName LobsterAI2API }"

// restartMain 通过计划任务重启主服务（SYSTEM 权限）
func restartMain() error {
	cmd := exec.Command("powershell", "-NoProfile", "-Command", psRestartMain)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// reloadMain 让主服务原地热重载（重读 config.json 的 Key 列表 + 重扫 auths 目录），
// **不重启进程**：请求不中断、keyStats/usage 等内存统计不丢。
//
// 背景（2026-09-21）：原来面板增删 Key / 账号一律 restartMain()，
// 因为 APIKey/APIKeys 和 auths 目录都是启动时读一次。后果是每次小改动都断流几秒、
// 统计清零、还可能撞上计划任务 IgnoreNew 的脏状态直接失联。
// 主服务现已提供 POST /reload，这里改成调它；失败才回退重启（保底可用）。
func reloadMain() error {
	body, _ := json.Marshal(map[string]string{})
	req, err := http.NewRequest("POST", "http://127.0.0.1:8367/reload", bytes.NewReader(body))
	if err != nil {
		return restartMain()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+proxyKey())
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// 主服务不在（或还没起 /reload 路由）→ 回退老路子，保证功能不退化
		return restartMain()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var r struct {
		Ok       bool   `json:"ok"`
		Error    string `json:"error"`
		Keys     int    `json:"keys"`
		Accounts int    `json:"accounts"`
	}
	if json.Unmarshal(raw, &r) != nil || !r.Ok {
		return fmt.Errorf("热重载失败(%s): %s", resp.Status, r.Error)
	}
	return nil
}
