package server

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CallLogEntry 一条调用流水（面板 API Key 页「记录」那张表的行）。
//
// 为什么单独落盘（2026-09-22）：用户要的是「第一张图那种记录」——
// 时间 / 消息 / 模型 / 输入TOKENS / 缓存命中TOKENS / 输出TOKENS / 消耗，
// 还要能按日期范围筛 + 显示"共 N 条调用"。内存里只留 30 条明细显然不够，
// 所以按行 append 到 JSONL，面板要查的时候现读现筛。
//
// 注意：**逐单积分拿不到**（上游 usage 里没有积分字段，响应头也没有）。
// 表里的"积分估算"是面板用**该账号实测的 今日积分消耗 ÷ 今日 token** 折算的，
// 属于估算，不能当成有道后台那个准数。
type CallLogEntry struct {
	At     time.Time `json:"at"`
	Key    string    `json:"key"`               // 哪个 API Key 调的（本地文件，不外传）
	KeyIdx int       `json:"key_idx,omitempty"` // 面板上的 #编号（-1 表示没对上）
	Model  string    `json:"model,omitempty"`   // 上游实际收到的模型名
	Acct   string    `json:"acct,omitempty"`    // 走账号池时用的 uid（反代为空）
	Src    string    `json:"src,omitempty"`     // lobster / ch_xxx
	Msg    string    `json:"msg,omitempty"`     // 第一条 user 消息预览（截断）
	In     int64     `json:"in,omitempty"`      // prompt_tokens
	Cached int64     `json:"cached,omitempty"`  // 缓存命中 tokens
	Out    int64     `json:"out,omitempty"`     // completion_tokens
	Reason int64     `json:"reason,omitempty"`  // 思考 tokens（含在 Out 里）
	Total  int64     `json:"total,omitempty"`   // total_tokens
	Stream bool      `json:"stream,omitempty"`
	OK     bool      `json:"ok"`
	Note   string    `json:"note,omitempty"`
	MS     int64     `json:"ms,omitempty"` // 端到端耗时（毫秒）
}

// callLog 调用流水的落盘器：热路径只往 channel 里丢，后台协程串行写，
// 避免每个请求都等一次磁盘 IO（也不能像 keyStats 那样直接写，量大）。
type callLog struct {
	mu     sync.Mutex
	file   string
	ch     chan CallLogEntry
	closed bool
	lines  int
}

const (
	callLogCap    = 12000 // 文件里最多留多少行（超了重写一份，留最新的 2/3）
	callLogKeep   = 8000
	callLogBufLen = 512
)

func newCallLog(fp string) *callLog {
	c := &callLog{file: fp, ch: make(chan CallLogEntry, callLogBufLen)}
	if fp == "" {
		return c
	}
	if n, err := countLines(fp); err == nil {
		c.lines = n
	}
	go c.loop()
	return c
}

func countLines(fp string) (int, error) {
	f, err := os.Open(fp)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			n++
		}
	}
	return n, sc.Err()
}

// Append 非阻塞入队（队列满了就丢这条 —— 记录不值得拖慢转发）。
func (c *callLog) Append(e CallLogEntry) {
	if c == nil || c.file == "" {
		return
	}
	select {
	case c.ch <- e:
	default:
	}
}

func (c *callLog) loop() {
	for e := range c.ch {
		c.write(e)
	}
}

func (c *callLog) write(e CallLogEntry) {
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = os.MkdirAll(filepath.Dir(c.file), 0755)
	f, err := os.OpenFile(c.file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
	c.lines++
	if c.lines > callLogCap {
		c.trimLocked()
	}
}

// trimLocked 文件太大 → 只留最新 callLogKeep 行（调用方持有 c.mu）
func (c *callLog) trimLocked() {
	raw, err := os.ReadFile(c.file)
	if err != nil {
		return
	}
	parts := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(parts) <= callLogKeep {
		c.lines = len(parts)
		return
	}
	keep := strings.Join(parts[len(parts)-callLogKeep:], "\n") + "\n"
	tmp := c.file + ".tmp"
	if os.WriteFile(tmp, []byte(keep), 0644) == nil {
		_ = os.Rename(tmp, c.file)
		c.lines = callLogKeep
	}
}

// Query 读文件、按 key + 日期范围过滤，返回（命中总数，第 offset 页起的 limit 条、新的在前）。
//
// 分页口径（2026-09-22 加）：offset=0 是最新的那一页；offset 按"条"算（offset=(page-1)*limit）。
func (c *callLog) Query(key, fromDay, toDay string, limit, offset int) (int, []CallLogEntry) {
	if c == nil || c.file == "" {
		return 0, nil
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	f, err := os.Open(c.file)
	if err != nil {
		return 0, nil
	}
	defer f.Close()
	var all []CallLogEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e CallLogEntry
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		if key != "" && e.Key != key {
			continue
		}
		if fromDay != "" || toDay != "" {
			d := e.At.Format("2006-01-02")
			if fromDay != "" && d < fromDay {
				continue
			}
			if toDay != "" && d > toDay {
				continue
			}
		}
		all = append(all, e)
	}
	total := len(all)
	// 新的在前：文件里是旧的在前，所以从尾巴往回取；offset 决定从倒数第几条开始
	start := total - 1 - offset
	if start < 0 {
		return total, []CallLogEntry{}
	}
	out := make([]CallLogEntry, 0, limit)
	for i := start; i >= 0 && len(out) < limit; i-- {
		out = append(out, all[i])
	}
	return total, out
}
