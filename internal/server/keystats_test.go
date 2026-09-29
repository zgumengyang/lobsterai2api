package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestKeyStatsPersistRoundTrip 锁定 2026-09-21 修的坑：
// keyStats 原来只存内存，面板「生成新 Key」会重启主服务 → 统计全丢 →
// 所有 Key 都显示"未使用"。现在必须落盘 + 启动时恢复。
func TestKeyStatsPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keystats.json")

	h1 := &Handler{keyFile: f}
	h1.keyMu.Lock()
	h1.keyStats = map[string]*KeyUsage{
		"sk-a": {LastUsed: time.Now(), Count: 42},
	}
	h1.flushKeyStatsLocked()
	h1.keyMu.Unlock()

	if _, err := os.Stat(f); err != nil {
		t.Fatalf("统计文件没写出来: %v", err)
	}

	// 模拟重启：新 Handler 从磁盘恢复
	h2 := &Handler{keyFile: f}
	h2.loadKeyStats()
	st, ok := h2.keyStats["sk-a"]
	if !ok {
		t.Fatalf("重启后没恢复 sk-a 的统计（这正是面板显示'未使用'的 bug）")
	}
	if st.Count != 42 {
		t.Fatalf("计数恢复错误: 期望 42, 实际 %d", st.Count)
	}
}

// TestKeyStatsLoadMissingFile 文件不存在时不应 panic，且要能正常记录新 key。
func TestKeyStatsLoadMissingFile(t *testing.T) {
	h := &Handler{keyFile: filepath.Join(t.TempDir(), "nope.json")}
	h.loadKeyStats()
	if h.keyStats == nil {
		t.Fatalf("keyStats 应为空 map 而不是 nil，否则首次 noteKeyUse 会写入 nil map")
	}
	h.noteKeyUse("sk-new") // noteKeyUse 自己会加锁，不能再在外面套 keyMu
	if got := h.keyStats["sk-new"].Count; got != 1 {
		t.Fatalf("首次记录失败: 期望 1, 实际 %d", got)
	}
}

// TestKeyStatsNoFileNoPanic 未配置文件路径时应静默跳过（保持原有可选持久化语义）。
func TestKeyStatsNoFileNoPanic(t *testing.T) {
	h := &Handler{}
	h.loadKeyStats()
	h.keyMu.Lock()
	h.saveKeyStatsLocked()
	h.keyMu.Unlock()
}
