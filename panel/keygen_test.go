package main

import (
	"strings"
	"testing"
)

// TestGenKeyFormat 锁定 2026-09-21 改的 key 格式：
// sk-blueapi- + 32 位 base62（只含大小写字母和数字）。
func TestGenKeyFormat(t *testing.T) {
	k := genKey()

	if !strings.HasPrefix(k, "sk-blueapi-") {
		t.Fatalf("前缀不对: %q", k)
	}
	body := strings.TrimPrefix(k, "sk-blueapi-")
	if len(body) != genKeyRandLen {
		t.Fatalf("随机部分应 %d 位，实际 %d 位: %q", genKeyRandLen, len(body), k)
	}
	for i, c := range body {
		if !strings.ContainsRune(keyAlphabet, c) {
			t.Fatalf("第 %d 位出现非法字符 %q（key=%q）", i, c, k)
		}
	}
	// 旧格式是 sk-lobster- + 小写 hex，确认已经不再产出
	if strings.HasPrefix(k, "sk-lobster-") {
		t.Fatalf("还在生成旧格式的 key: %q", k)
	}
	if strings.ContainsAny(body, "-_") {
		t.Fatalf("随机部分不该含 - 或 _（怕客户端截断）: %q", k)
	}
}

// TestGenKeyUnique 连发 2000 个必须零重复。
func TestGenKeyUnique(t *testing.T) {
	seen := make(map[string]bool, 2000)
	for i := 0; i < 2000; i++ {
		k := genKey()
		if seen[k] {
			t.Fatalf("第 %d 次生成撞了重复 key: %q", i, k)
		}
		seen[k] = true
	}
}

// TestKeyAlphabetIsBase62 字符集必须是 62 个且无重复（拒绝采样里按 62 取模）。
func TestKeyAlphabetIsBase62(t *testing.T) {
	if len(keyAlphabet) != 62 {
		t.Fatalf("字符集应为 62 个，实际 %d", len(keyAlphabet))
	}
	seen := map[rune]bool{}
	for _, c := range keyAlphabet {
		if seen[c] {
			t.Fatalf("字符集有重复: %q", c)
		}
		seen[c] = true
	}
}
