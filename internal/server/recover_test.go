package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// panic 要有兜底：正常情况下回干净的 500；流式已经写过字节的情况下不能二次写头
func TestWithRecover(t *testing.T) {
	h := WithRecover(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/x", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("panic 后 http=%d，期望 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal_panic") {
		t.Fatalf("500 响应里该带 internal_panic 标记，实际: %s", rec.Body.String())
	}

	// 流式中途 panic：头和数据都已经发出去，不能再写 500（否则 net/http 会报 superfluous WriteHeader）
	h2 := WithRecover(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"a\":1}\n\n"))
		panic("mid-stream")
	}))
	rec2 := httptest.NewRecorder()
	h2.ServeHTTP(rec2, httptest.NewRequest("GET", "/v1/chat/completions", nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("中途 panic 不该改状态码，实际 %d", rec2.Code)
	}
	if !strings.Contains(rec2.Body.String(), "data: {\"a\":1}") {
		t.Fatal("已经发出去的字节不能被吞掉")
	}
	if strings.Contains(rec2.Body.String(), "internal_panic") {
		t.Fatal("头已发出时不该再写 500 体")
	}
}
