// Package scheduler 定时任务：每日签到（09/21点）+ token keepalive（22点）。
// 签到成功后重新查余额，余额 > 0 的冷却账号自动解冻。
package scheduler

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"lobsterai2api/internal/pool"
	"lobsterai2api/internal/upstream"
)

// Config 调度器依赖。
type Config struct {
	Pool           *pool.Pool
	Upstream       *upstream.Client
	CheckinHours   []int // 默认 [9, 21]
	KeepaliveHours []int // 默认 [22]
}

// Scheduler 调度器。
type Scheduler struct {
	cfg Config
}

// New 构建。
func New(cfg Config) *Scheduler {
	if len(cfg.CheckinHours) == 0 {
		cfg.CheckinHours = []int{9, 21}
	}
	if len(cfg.KeepaliveHours) == 0 {
		cfg.KeepaliveHours = []int{22}
	}
	return &Scheduler{cfg: cfg}
}

// nextFire 返回 now 之后最近的一个整点触发时间；hours 为本地小时（0-23）。
func nextFire(now time.Time, hours []int) time.Time {
	var earliest time.Time
	for _, h := range hours {
		t := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, now.Location())
		if !t.After(now) {
			t = t.Add(24 * time.Hour)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest
}

// Run 主循环，阻塞直到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	all := append(append([]int{}, s.cfg.CheckinHours...), s.cfg.KeepaliveHours...)
	for {
		next := nextFire(time.Now(), all)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			h := time.Now().Hour()
			if contains(s.cfg.CheckinHours, h) {
				s.RunCheckinNow()
			}
			if contains(s.cfg.KeepaliveHours, h) {
				s.RunKeepaliveNow()
			}
		}
	}
}

// isBannedErr 判断是否为“账号被封禁”（40302）。
// 上游 err 里包含 40302 / “账号已被禁用” 就算。
func isBannedErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, m := range []string{"40302", "账号已被禁用", "账号已被封禁", "账号封禁"} {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

func contains(hours []int, h int) bool {
	for _, v := range hours {
		if v == h {
			return true
		}
	}
	return false
}

// CheckinStats 一轮「每日积分礼」的统计（面板「立即签到」按钮用）。
// CheckinRow 单个账号的签到结果（面板弹窗逐条显示）。
type CheckinRow struct {
	UID     string `json:"uid"`
	Nick    string `json:"nick"`
	Status  string `json:"status"` // claimed / already / no_activity / failed / banned
	Credits int64  `json:"credits"`
	Error   string `json:"error,omitempty"`
}

type CheckinStats struct {
	Total   int   // 参与账号数
	Claimed int   // 本次真领到
	Credits int64 // 合计到账积分
	Already int   // 今天已经领过
	NoAct   int   // 上游没有可领的活动
	Failed  int   // 请求失败
	Banned  int   // 顺带查出被封
	Rows    []CheckinRow
}

// RunCheckinNow 定时触发用（结果不要统计）。
func (s *Scheduler) RunCheckinNow() { _ = s.RunCheckinOnce() }

// RunCheckinOnce 立即对所有账号领「每日积分礼」+ 余额刷新 + 解冻，并返回统计。
// 冷却中的账号也参与（领到分就能解冻它们）；禁用的跳过。
func (s *Scheduler) RunCheckinOnce() CheckinStats {
	var stats CheckinStats
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshToken == "" {
			continue
		}
		// 邀请进度先刷（2026-09-26 用户要求）：不管这个号有没有积分都要刷 ——
		// 0 分/封号的分支后面会 continue，放在那儿就永远刷不到了。
		if ip, ierr := s.cfg.Upstream.InviteProgress(a); ierr == nil && ip != nil {
			s.cfg.Pool.SetInvite(st.UID, ip.InvitedCount, ip.InvitationCode,
				ip.Stage1Threshold, ip.Stage1Completed, ip.TotalRewardCredits)
		}
		stats.Total++
		row := CheckinRow{UID: st.UID, Nick: a.Nickname}
		res, err := s.cfg.Upstream.DailyCheckin(a)
		switch {
		case err != nil:
			stats.Failed++
			row.Status, row.Error = "failed", err.Error()
			log.Printf("checkin %s: %v", st.UID, err)
		case res.NoActivity:
			stats.NoAct++
			row.Status = "no_activity"
		case res.AlreadyToday:
			stats.Already++
			row.Status = "already"
		default:
			stats.Claimed++
			stats.Credits += res.Credits
			row.Status, row.Credits = "claimed", res.Credits
			log.Printf("checkin %s: 领到 %d 积分（活动 %s）", st.UID, res.Credits, res.ActivityCode)
		}
		remain, _, err := s.cfg.Upstream.QuotaUsage(a)
		if err != nil {
			// 同上：无积分 = 余额 0，不是查询失败
			if errors.Is(err, upstream.ErrNoCredits) {
				s.markOutOfCredits(st.UID, "checkin")
				stats.Rows = append(stats.Rows, row)
				continue
			}
			log.Printf("quota %s: %v", st.UID, err)
			// 40302 = 账号被有道封禁，终止性的 → 直接禁用，不再参与选号
			if isBannedErr(err) {
				s.cfg.Pool.Disable(st.UID, "账号已被有道封禁 (40302)")
				log.Printf("checkin %s: 封号 → 已禁用", st.UID)
				stats.Banned++
				row.Status, row.Error = "banned", "账号已被有道封禁 (40302)"
			}
			stats.Rows = append(stats.Rows, row)
			continue
		}
		s.cfg.Pool.ReenableIfCredits(st.UID, remain)
		stats.Rows = append(stats.Rows, row)
	}
	return stats
}

// markOutOfCredits 把"上游确认没积分了"落成 0 + 长冷却。
//
// 为什么要单独一个函数（2026-09-22 实测）：`QuotaUsage` 遇到
// `totalCreditsRemaining` 缺失/≤0 时返回的是 `upstream.ErrNoCredits` —— 那是**余额 0 的权威答案**，
// 不是"查询失败"。旧代码把它当普通 error 只打一行日志就 continue，
// 于是这三个号一直顶着一个过期的积分值继续被选号，请求打到一半上游才回"免费额度已用完"，
// 用户看到的就是"正在重新连接 / 5"。
func (s *Scheduler) markOutOfCredits(uid, from string) {
	s.cfg.Pool.SetCredits(uid, 0)
	s.cfg.Pool.Cooldown(uid, pool.CoolHard, 12*time.Hour, "上游余额为0（profile 无积分）")
	log.Printf("%s %s: 上游确认无积分 → 积分归零 + 硬冷却 12h，不再参与选号", from, uid)
}

// RunQuotaRefresh 立即刷新所有账号余额（不做签到）：
// 余额 > 0 → 更新积分并解冻；余额 = 0 → 更新积分并立即硬冷却，从选号中排除。
// 这是「死号永不参与选号」的主动保障，每 10 分钟跑一次。
func (s *Scheduler) RunQuotaRefresh() {
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.AccessToken == "" {
			continue
		}
		// 邀请进度先刷（不受"有没有积分/是否被封"影响，见 checkin 里同样的注释）
		if ip, ierr := s.cfg.Upstream.InviteProgress(a); ierr == nil && ip != nil {
			s.cfg.Pool.SetInvite(st.UID, ip.InvitedCount, ip.InvitationCode,
				ip.Stage1Threshold, ip.Stage1Completed, ip.TotalRewardCredits)
		}
		remain, _, err := s.cfg.Upstream.QuotaUsage(a)
		if err != nil {
			// 上游权威回答"没积分了" → 归零 + 硬冷却（否则过期积分会让它一直被选号）
			if errors.Is(err, upstream.ErrNoCredits) {
				s.markOutOfCredits(st.UID, "quota-refresh")
				continue
			}
			log.Printf("quota-refresh %s: %v", st.UID, err)
			// 2026-09-21 实测：有道批量封号后，旧版本这里只打日志 continue，
			// 导致 38 个死号永远留在池里被重复选中。
			if isBannedErr(err) {
				s.cfg.Pool.Disable(st.UID, "账号已被有道封禁 (40302)")
				log.Printf("quota-refresh %s: 封号 → 已禁用", st.UID)
			}
			continue
		}
		s.cfg.Pool.SetCredits(st.UID, remain)
		if remain <= 0 {
			s.cfg.Pool.Cooldown(st.UID, pool.CoolHard, 12*time.Hour, "余额0，定期刷新主动排除")
		} else {
			s.cfg.Pool.ReenableIfCredits(st.UID, remain)
		}
	}
}

// RunKeepaliveNow 立即对所有账号刷新 token；session 死亡的自动禁用。
func (s *Scheduler) RunKeepaliveNow() {
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshToken == "" {
			continue
		}
		if err := s.cfg.Upstream.RefreshToken(a); err != nil {
			log.Printf("keepalive %s: %v", st.UID, err)
			var ue *upstream.Error
			if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
				s.cfg.Pool.Disable(st.UID, "refresh session dead")
			}
			continue
		}
		if err := a.SaveAtomic(); err != nil {
			log.Printf("keepalive %s save: %v", st.UID, err)
		}
	}
}

// RunKeepaliveOnce 跑一轮 token 续期，并返回统计（供面板手动触发看得见结果）。
//
// 与 RunKeepaliveNow 的差别：
//  1. 返回 成功 / 失败 / 封禁 三个计数；
//  2. 额外识别 ErrBanned —— 2026-09-21 实测：被有道封禁的账号在 refresh 时同样回
//     403 40302，老逻辑只认 ErrSessionDead，导致封号账号在续期这条路上"看起来没事"，
//     要等下一次签到/配额刷新才被禁用。这里直接判掉。
func (s *Scheduler) RunKeepaliveOnce() (ok, failed, banned int) {
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshToken == "" {
			continue
		}
		if err := s.cfg.Upstream.RefreshToken(a); err != nil {
			failed++
			log.Printf("keepalive %s: %v", st.UID, err)
			var ue *upstream.Error
			if errors.As(err, &ue) {
				switch ue.Kind {
				case upstream.ErrBanned:
					s.cfg.Pool.Disable(st.UID, "refresh 时发现账号已被封禁 (40302)")
					banned++
					log.Printf("keepalive %s: 封号 → 已禁用", st.UID)
				case upstream.ErrSessionDead:
					s.cfg.Pool.Disable(st.UID, "refresh session dead")
				}
			}
			continue
		}
		if err := a.SaveAtomic(); err != nil {
			log.Printf("keepalive %s save: %v", st.UID, err)
		}
		ok++
	}
	return ok, failed, banned
}
