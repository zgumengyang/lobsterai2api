package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestSetValidKeysHotSwap 锁定 2026-09-21 的改动：
// 面板增删 Key 后不再重启主服务，而是调 SetValidKeys 原地换掉生效集合。
func TestSetValidKeysHotSwap(t *testing.T) {
	h := &Handler{cfg: Config{APIKey: "sk-old"}}

	if got := h.validKeys(); !got["sk-old"] || len(got) != 1 {
		t.Fatalf("初始 key 集合不对: %v", got)
	}

	n := h.SetValidKeys([]string{"sk-new1", " sk-new2 "})
	if n != 2 {
		t.Fatalf("应生效 2 个 key（含去空格），实际 %d", n)
	}

	got := h.validKeys()
	if !got["sk-new1"] || !got["sk-new2"] {
		t.Fatalf("新 key 未生效: %v", got)
	}
	if got["sk-old"] {
		t.Fatalf("旧 key 应被替换掉: %v", got)
	}
}

// TestSetValidKeysRejectsEmpty 防呆：空列表不能把鉴权清空（否则全站变成无鉴权）。
func TestSetValidKeysRejectsEmpty(t *testing.T) {
	h := &Handler{cfg: Config{APIKey: "sk-keep-me"}}
	h.SetValidKeys([]string{"sk-a"})

	if n := h.SetValidKeys(nil); n != 0 {
		t.Fatalf("空列表应被拒绝，返回 0，实际 %d", n)
	}
	if n := h.SetValidKeys([]string{"", "   "}); n != 0 {
		t.Fatalf("全空白应被拒绝，返回 0，实际 %d", n)
	}
	if !h.validKeys()["sk-a"] {
		t.Fatalf("拒绝空列表后原 key 应保持不变")
	}
}

// TestReloadHandlerOK POST /reload 正常路径：调用注入的重载函数并回传数量。
func TestReloadHandlerOK(t *testing.T) {
	h := &Handler{}
	called := 0
	h.SetReload(func() (int, int, error) {
		called++
		return 3, 40, nil
	})

	rec := httptest.NewRecorder()
	h.reloadHandler(rec, httptest.NewRequest(http.MethodPost, "/reload", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码应为 200，实际 %d", rec.Code)
	}
	var r struct {
		Ok       bool `json:"ok"`
		Keys     int  `json:"keys"`
		Accounts int  `json:"accounts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("响应不是 JSON: %v", err)
	}
	if !r.Ok || r.Keys != 3 || r.Accounts != 40 || called != 1 {
		t.Fatalf("重载结果不对: %+v called=%d", r, called)
	}
}

// TestReloadHandlerError 重载函数报错时要回传 ok=false + 原因，而不是假装成功。
func TestReloadHandlerError(t *testing.T) {
	h := &Handler{}
	h.SetReload(func() (int, int, error) { return 0, 0, errors.New("config 坏了") })

	rec := httptest.NewRecorder()
	h.reloadHandler(rec, httptest.NewRequest(http.MethodPost, "/reload", nil))

	var r struct {
		Ok    bool   `json:"ok"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	if r.Ok {
		t.Fatalf("出错时不该回 ok=true")
	}
	if r.Error != "config 坏了" {
		t.Fatalf("错误原因没回传: %q", r.Error)
	}
}

// TestReloadHandlerNotWired 未注入重载函数时（老版本/裸构建）要明确回 ok=false，不能 panic。
func TestReloadHandlerNotWired(t *testing.T) {
	h := &Handler{}
	rec := httptest.NewRecorder()
	h.reloadHandler(rec, httptest.NewRequest(http.MethodPost, "/reload", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码应为 200，实际 %d", rec.Code)
	}
}

// TestKeepaliveHandlerOK POST /keepalive 正常路径：把三个计数原样回传。
func TestKeepaliveHandlerOK(t *testing.T) {
	h := &Handler{}
	h.SetKeepalive(func() (int, int, int) { return 2, 1, 0 })

	rec := httptest.NewRecorder()
	h.keepaliveHandler(rec, httptest.NewRequest(http.MethodPost, "/keepalive", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码应为 200，实际 %d", rec.Code)
	}
	var r struct {
		Ok        bool `json:"ok"`
		Refreshed int  `json:"refreshed"`
		Failed    int  `json:"failed"`
		Banned    int  `json:"banned"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("响应不是 JSON: %v", err)
	}
	if !r.Ok || r.Refreshed != 2 || r.Failed != 1 || r.Banned != 0 {
		t.Fatalf("续期统计不对: %+v", r)
	}
}

// TestKeepaliveHandlerNotWired 未注入时不 panic，回 ok=false。
func TestKeepaliveHandlerNotWired(t *testing.T) {
	h := &Handler{}
	rec := httptest.NewRecorder()
	h.keepaliveHandler(rec, httptest.NewRequest(http.MethodPost, "/keepalive", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码应为 200，实际 %d", rec.Code)
	}
	var r struct {
		Ok bool `json:"ok"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	if r.Ok {
		t.Fatalf("未接线时不该回 ok=true")
	}
}
