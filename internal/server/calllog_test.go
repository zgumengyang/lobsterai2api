package server

import (
	"path/filepath"
	"testing"
	"time"
)

// 调用流水：按 key / 日期范围过滤、新的在前、条数上限
func TestCallLogQuery(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "keycalls.jsonl")
	c := newCallLog(fp)
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.Local)
	for i := 0; i < 5; i++ {
		c.Append(CallLogEntry{At: base.Add(time.Duration(i) * time.Minute), Key: "k1", Model: "m1", In: int64(i), OK: true})
	}
	c.Append(CallLogEntry{At: base.Add(30 * time.Hour), Key: "k2", Model: "m2", OK: true})

	// 等后台协程把队列写进文件（最多等 3 秒）
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := countLines(fp); n >= 6 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	total, rows := c.Query("k1", "", "", 10, 0)
	if total != 5 || len(rows) != 5 {
		t.Fatalf("k1 命中 total=%d rows=%d，期望 5/5", total, len(rows))
	}
	if !rows[0].At.After(rows[4].At) {
		t.Fatal("明细应该新的在前")
	}
	if rows[0].In != 4 {
		t.Fatalf("最后一条应该是 In=4，实际 %d", rows[0].In)
	}

	// 日期范围（北京时间的日期字符串）
	if tot, rs := c.Query("k1", "2026-09-22", "2026-09-22", 3, 0); tot != 5 || len(rs) != 3 {
		t.Fatalf("带日期范围 total=%d rows=%d，期望 5/3", tot, len(rs))
	}
	if tot, _ := c.Query("k1", "2026-09-23", "2026-09-23", 10, 0); tot != 0 {
		t.Fatalf("9-23 不该有 k1 的记录，得到 %d", tot)
	}
	if tot, _ := c.Query("k2", "2026-09-23", "2026-09-23", 10, 0); tot != 1 {
		t.Fatalf("k2 在 9-23 应该命中 1 条，得到 %d", tot)
	}
	// 不传 key = 所有 key
	if tot, _ := c.Query("", "", "", 100, 0); tot != 6 {
		t.Fatalf("不按 key 过滤应该 6 条，得到 %d", tot)
	}

	// 分页：每页 2 条，第 1 页应为最新的两条（In=4,3），第 3 页只剩 1 条（In=0）
	_, p1 := c.Query("k1", "", "", 2, 0)
	_, p2 := c.Query("k1", "", "", 2, 2)
	_, p3 := c.Query("k1", "", "", 2, 4)
	_, p9 := c.Query("k1", "", "", 2, 99)
	if len(p1) != 2 || p1[0].In != 4 || p1[1].In != 3 {
		t.Fatalf("第 1 页不对: %+v", p1)
	}
	if len(p2) != 2 || p2[0].In != 2 || p2[1].In != 1 {
		t.Fatalf("第 2 页不对: %+v", p2)
	}
	if len(p3) != 1 || p3[0].In != 0 {
		t.Fatalf("第 3 页不对: %+v", p3)
	}
	if len(p9) != 0 {
		t.Fatalf("越界页应该返回空，得到 %d 条", len(p9))
	}
}

// 消息预览：压空白 + 按 rune 截断（别切坏中文）
func TestBriefN(t *testing.T) {
	if got := briefN("  a\n\nb\t c  ", 10); got != "a b c" {
		t.Fatalf("空白压缩不对: %q", got)
	}
	if got := briefN("一二三四五", 3); got != "一二三…" {
		t.Fatalf("中文截断不对: %q", got)
	}
	if got := briefN("", 5); got != "" {
		t.Fatalf("空串应返回空，得到 %q", got)
	}
}
