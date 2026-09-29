package relay

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ChatProbe 必须校验响应形状：HTTP 200 也可能是"假成功"（MiniMax base_resp / 中转 error 对象）。
func TestChatProbeShapeValidation(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{"正常 choices", 200, `{"choices":[{"message":{"content":"pong"}}]}`, false},
		{"MiniMax 200 假成功", 200, `{"base_resp":{"status_code":2049,"status_msg":"invalid api key"},"usage":{}}`, true},
		{"中转 200 带 error 对象", 200, `{"error":{"message":"rpm exhausted"}}`, true},
		{"200 但没有 choices", 200, `{"id":"x","object":"chat.completion"}`, true},
		{"choices 为 null", 200, `{"choices":null,"base_resp":{"status_code":2056,"status_msg":"用量上限"}}`, true},
		{"余额不足 403", 403, `{"error":{"message":"Insufficient account balance"}}`, true},
		{"限流 429", 429, `{"error":{"message":"rpm exhausted"}}`, true},
	}
	for _, c := range cases {
		status, body := c.status, c.body
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		ch := &Channel{Name: "t", BaseURL: srv.URL + "/v1", APIKey: "sk-x", Models: []string{"m"}}
		err := ChatProbe(ch, "m", 5*time.Second)
		srv.Close()
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

// 探测请求本身要合规：Bearer 头 + max_tokens=1 + 非流式。
func TestChatProbeRequestShape(t *testing.T) {
	var gotAuth, gotCT string
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer srv.Close()
	if err := ChatProbe(&Channel{BaseURL: srv.URL + "/v1", APIKey: "sk-abc"}, "m1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer sk-abc" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q", gotCT)
	}
	if got["max_tokens"] != float64(1) {
		t.Errorf("max_tokens 应为 1，实际 %v", got["max_tokens"])
	}
	if got["model"] != "m1" {
		t.Errorf("model = %v", got["model"])
	}
	if got["stream"] != false {
		t.Errorf("stream 应为 false，实际 %v", got["stream"])
	}
}
