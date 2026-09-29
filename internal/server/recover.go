package server

import (
	"log"
	"net/http"
	"runtime/debug"
)

// panicRecorder 包一层 ResponseWriter，用来知道"响应头有没有已经发出去"。
//
// 为什么需要：流式接口（/v1/chat/completions）中途 panic 时，HTTP 200 和部分字节
// 早就发给客户端了，这时再写 500 会触发 net/http 的
// "superfluous response.WriteHeader call" 噪音日志。知道状态就能只做该做的事。
type panicRecorder struct {
	http.ResponseWriter
	wrote bool
}

func (p *panicRecorder) WriteHeader(code int) {
	p.wrote = true
	p.ResponseWriter.WriteHeader(code)
}

func (p *panicRecorder) Write(b []byte) (int, error) {
	p.wrote = true
	return p.ResponseWriter.Write(b)
}

// Flush 必须显式实现：upstream.Stream / relay.streamOut 都会做 w.(http.Flusher) 断言，
// 少了它 SSE 就不再逐块推给客户端（体感就是"一直在转圈"）。
func (p *panicRecorder) Flush() {
	if f, ok := p.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WithRecover 兜住任何 handler 里的 panic：
//  1. 把 panic 值 + 完整调用栈打进日志（否则只剩 net/http 自己那一行，根本定位不到在哪儿）
//  2. 响应头还没发出去 → 回一个干净的 500 JSON；已经发出去了（流式场景）→ 只记日志，别画蛇添足
//
// 背景（2026-09-22 整体体检发现）：2026-09-20 14:42~14:47 日志里有 **24 次**
//
//	http: panic serving 220.203.226.37:xxxxx: runtime error: invalid memory address or nil pointer dereference
//
// 全部落在 chatCompletions，客户端看到的就是"连接直接断"。那批代码后来被大面积重写、
// 之后再没复现；但这类"裸断"一定要有兜底：以后再出问题能自证 + 给客户端一个可读错误。
func WithRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &panicRecorder{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				log.Printf("[panic] %s %s %s -> %v\n%s", r.Method, r.URL.Path, r.RemoteAddr, v, debug.Stack())
				if !rec.wrote {
					writeOpenAIError(rec, http.StatusInternalServerError, "internal_panic", "网关内部错误（已记录日志，请重试）")
				}
			}
		}()
		next.ServeHTTP(rec, r)
	})
}
