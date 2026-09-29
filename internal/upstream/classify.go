// Package upstream 封装对 LobsterAI 上游（chat / quota / auth）的全部 HTTP 调用，
// 以及错误分类（驱动 pool 冷却状态机）。
package upstream

import (
	"fmt"
	"net/http"
	"strings"
)

// ErrKind 错误分类，pool 据此决定冷却时长。
type ErrKind int

const (
	ErrNone        ErrKind = iota // 成功
	ErrHardCredit                 // 余额不足（402 或 body 关键词）→ 长冷却
	ErrSoftRate                   // 429 软限流 → 短冷却
	ErrSessionDead                // 401 + 40100/40101 刷新被拒 → 禁用
	ErrNotFound                   // 404 上游偶发 → 短冷却不累计 errCount（防雪崩）
	ErrServer                     // 5xx 上游故障
	ErrBanned                     // 账号被上游封禁（40302）→ 立即禁用，不再参与选号
	ErrClient                     // 其他 4xx / 业务错误
)

func (k ErrKind) String() string {
	switch k {
	case ErrHardCredit:
		return "hard_credit"
	case ErrSoftRate:
		return "soft_rate"
	case ErrSessionDead:
		return "session_dead"
	case ErrNotFound:
		return "not_found"
	case ErrServer:
		return "server"
	case ErrBanned:
		return "banned"
	case ErrClient:
		return "client"
	default:
		return "none"
	}
}

// Error 带分类的上游错误。
type Error struct {
	Kind   ErrKind
	Status int
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream %s (http %d): %s", e.Kind, e.Status, e.Msg)
}

// hardMarkers 余额不足关键词（小写比较 + 中文原文比较双通道）。
var hardMarkers = []string{
	"insufficient credit", "no credit", "credit exhausted", "out of credit",
	"quota exceeded", "quota exhaust", "payment required", "credit not enough",
	"not enough credit", "freecreditsused", "free credits used",
	"积分不足", "额度不足", "余额不足", "积分用完", "额度用尽", "没有积分", "积分耗尽",
	// 2026-09-22 实测漏网：上游会回「免费额度已用完，请升级套餐」（带推广口吻），
	// 旧词表只有"额度用尽/额度不足"，匹配不到"额度已用完" → 被当成普通 4xx → 号不冷却 → 一直白撞。
	"免费额度", "额度已用完", "额度用完", "免费额度已用完", "请升级套餐", "升级套餐",
	"free quota", "quota used up", "quota exhausted", "insufficient balance",
}

// QuotaWords 上游"额度用完"类字样（含推广口吻，如"请升级套餐"）。
// 用途：① Classify 判 hard_credit；② 挡掉别原样透传给客户（SSE 中途也要挡）。
var QuotaWords = []string{
	"免费额度", "额度已用完", "额度用完", "额度用尽", "额度不足", "余额不足", "积分不足", "积分用完",
	"请升级套餐", "升级套餐", "quota exceeded", "insufficient balance", "insufficient credit",
	"insufficient quota", "out of credit", "payment required", "free quota",
}

// IsQuotaText 这段文本是不是"额度用完/推广"类
func IsQuotaText(s string) bool {
	low := strings.ToLower(s)
	for _, w := range QuotaWords {
		if strings.Contains(low, strings.ToLower(w)) || strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// QuotaNotice 给客户看的中立提示（替代上游原文）
const QuotaNotice = "上游额度暂时不可用，网关已自动切换其它线路，请稍后重试"

// SanitizeSSELine 流式（SSE）里也要挡：上游常把额度错误塞在 data 块里，
// 一旦客户端已经收到 200 + 部分数据，就没法再换线路了 —— 只能把这一行**整行换成**中立的 error 事件，
// 否则客户端会把它当普通内容显示出来（用户实测："stream disconnected before completion: 免费额度已用完，请升级套餐"）。
func SanitizeSSELine(line string) string {
	t := strings.TrimSpace(line)
	if !strings.HasPrefix(t, "data:") || !IsQuotaText(line) {
		return line
	}
	return "data: {\"error\":{\"message\":\"" + QuotaNotice + "\",\"type\":\"api_error\",\"code\":\"upstream_quota\"}}\n\n"
}

// sessionDeadMarkers 龙虾刷新被拒的错误码（40100 / 40101 为终止性失败）。
var sessionDeadMarkers = []string{"40100", "40101", "token rejected", "refresh token was rejected"}

// bannedMarkers 账号被封禁（HTTP 403 + code 40302）。
// 2026-09-21 实测：有道对批量登录的账号下发 403{"code":40302,"message":"账号已被禁用"}。
// 旧版本把它归入 ErrClient（“请求形态错误”）→ 死号永不被禁用，
// 每次请求都白撞 3 个死号后报 no_healthy_account。
var bannedMarkers = []string{"40302", "账号已被禁用", "账号已被封禁", "账号封禁", "用户已被禁用"}

// Classify 按 HTTP 状态码 + body 判定错误类别。
func Classify(status int, body string) ErrKind {
	if status == http.StatusPaymentRequired {
		return ErrHardCredit
	}
	lower := strings.ToLower(body)
	for _, m := range hardMarkers {
		if strings.Contains(lower, strings.ToLower(m)) || strings.Contains(body, m) {
			return ErrHardCredit
		}
	}
	for _, m := range sessionDeadMarkers {
		if strings.Contains(body, m) {
			return ErrSessionDead
		}
	}
	// 封号判定必须放在通用 4xx 之前：40302 是终止性的，
	// 不能当成“请求形态错误”处理。
	for _, m := range bannedMarkers {
		if strings.Contains(body, m) {
			return ErrBanned
		}
	}
	if status == http.StatusTooManyRequests {
		return ErrSoftRate
	}
	if status == http.StatusNotFound {
		return ErrNotFound
	}
	if status >= 500 {
		return ErrServer
	}
	if status >= 400 {
		return ErrClient
	}
	return ErrNone
}
