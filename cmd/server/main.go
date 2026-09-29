// main.go lobsterai2api 入口：加载配置、构建 pool、起调度器与 HTTP 服务。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"lobsterai2api/internal/auth"
	"lobsterai2api/internal/pool"
	"lobsterai2api/internal/relay"
	"lobsterai2api/internal/scheduler"
	"lobsterai2api/internal/server"
	"lobsterai2api/internal/upstream"
)

// logMaxBytes service.log 轮转阈值：超过就把当前文件改名 .1（只留一代），重开新文件。
// 之前是纯 O_APPEND 无轮转，只有 chat_stream 一行一条日志，长期跑必然把盘撑满。
const logMaxBytes = 16 << 20

// rotateWriter 按大小轮转的日志写入器（够用就好，不引第三方库）。
type rotateWriter struct {
	path string
	f    *os.File
	size int64
}

func newRotateWriter(path string) (*rotateWriter, error) {
	w := &rotateWriter{path: path}
	if fi, err := os.Stat(path); err == nil {
		w.size = fi.Size()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w.f = f
	return w, nil
}

func (w *rotateWriter) Write(p []byte) (int, error) {
	if w.size+int64(len(p)) > logMaxBytes {
		_ = w.f.Close()
		// 改名失败（被占用等）也不能卡住，退回直接截断，至少不撑盘
		_ = os.Rename(w.path, w.path+".1")
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return 0, err
		}
		w.f = f
		w.size = 0
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

func main() {
	cfgPath := flag.String("config", "config.json", "path to config json")
	flag.Parse()

	// 日志落盘：设了 LB2A_LOG_FILE 就把 log 输出写入该文件（计划任务 stdout 是丢弃的，
	// 之前日志全丢，排查冷却/500 只能盲猜）。追加写。
	if lf := os.Getenv("LB2A_LOG_FILE"); lf != "" {
		if f, err := newRotateWriter(lf); err == nil {
			log.SetOutput(f)
		}
	}

	cfg, err := Load(*cfgPath)
	if err != nil {
		// 配置文件不存在时给一次机会用纯默认 + env
		if os.IsNotExist(err) {
			log.Printf("config %s not found, using defaults+env", *cfgPath)
			cfg, err = Load("")
		}
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	auths, err := auth.LoadDir(cfg.AuthDir)
	if err != nil {
		log.Fatalf("load auths: %v", err)
	}
	log.Printf("loaded %d account(s) from %s", len(auths), cfg.AuthDir)

	p := pool.New(cfg.StateFile)
	// SyncToDir：以 auths 目录为准对齐账号池，
	// 目录里消失的账号一并剔除（修复删除账号后 state.json 残留问题）
	p.SyncToDir(auths)

	up := upstream.New()
	up.HTTP.Timeout = time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second

	sch := scheduler.New(scheduler.Config{
		Pool:           p,
		Upstream:       up,
		CheckinHours:   cfg.Schedule.CheckinHours,
		KeepaliveHours: cfg.Schedule.KeepaliveHours,
	})

	h := server.NewHandler(server.Config{
		Pool:         p,
		Upstream:     up,
		APIKey:       cfg.APIKey,
		APIKeys:      cfg.AllKeys(),
		HardCooldown: cfg.HardCreditDur,
		SoftCooldown: cfg.SoftRateDur,
		MaxRotate:    cfg.MaxRotate,
		ErrThreshold: cfg.Cooldown.ErrThresh,
		ErrCooldown:  cfg.ErrCooldownDur,
		UsageFile:    filepath.Join(filepath.Dir(cfg.StateFile), "usage.json"),
		CallLogFile:  filepath.Join(filepath.Dir(cfg.StateFile), "keycalls.jsonl"),
		RouteFile:    filepath.Join(filepath.Dir(cfg.StateFile), "route.json"),
		NotesFile:    filepath.Join(filepath.Dir(cfg.StateFile), "notes.json"),
		PriceFile:    filepath.Join(filepath.Dir(cfg.StateFile), "price.json"),
		LimitsFile:   filepath.Join(filepath.Dir(cfg.StateFile), "keylimits.json"),
		KeyFile:      filepath.Join(filepath.Dir(cfg.StateFile), "keystats.json"),
		Relay:        relay.NewStore(filepath.Join(filepath.Dir(cfg.StateFile), "channels.json")),
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 热重载：面板增删 API Key / 增删账号后调 POST /reload 即可，**无需重启进程**。
	// 背景：APIKey/APIKeys 与 auths 目录原来都是启动时读一次，改完不重启就看不到变更；
	// 现在的做法是原地重读 config.json + 重扫 auths 目录 + 同步账号池，请求不中断、统计不丢。
	h.SetReload(func() (int, int, error) {
		newCfg, err := Load(*cfgPath)
		if err != nil {
			return 0, 0, fmt.Errorf("重读 config: %w", err)
		}
		all := newCfg.AllKeys()
		nKeys := h.SetValidKeys(all)

		newAuths, err := auth.LoadDir(newCfg.AuthDir)
		if err != nil {
			return nKeys, 0, fmt.Errorf("重扫 auths: %w", err)
		}
		before := p.Count()
		p.SyncToDir(newAuths)
		after := p.Count()
		// 账号数变了才触发配额刷新：新号可能 0 分需冷却、删号不必白打一轮上游
		if after != before {
			go sch.RunQuotaRefresh()
		}
		return nKeys, after, nil
	})

	go sch.Run(ctx)

	// 手动续期：面板「立即续期」按钮 → POST /keepalive → 原地刷所有账号 token。
	// token 30 天有效、refreshToken 180 天，定时任务 22:00 才跑；
	// 这里给一个随时可以点名的入口，方便重启后/改配置后立刻确认续期链是否还通。
	h.SetKeepalive(func() (int, int, int) {
		return sch.RunKeepaliveOnce()
	})
	// 「每日积分礼」手动入口：每个账号每天能领 100 积分（定时任务 9/21 点各跑一次）。
	// 重复领是安全的 —— 上游按天记账，第二次返回「今天已领」。
	h.SetCheckin(func() map[string]any {
		st := sch.RunCheckinOnce()
		rows := make([]map[string]any, 0, len(st.Rows))
		for _, r := range st.Rows {
			rows = append(rows, map[string]any{
				"uid": r.UID, "nick": r.Nick, "status": r.Status,
				"credits": r.Credits, "error": r.Error,
			})
		}
		return map[string]any{
			"total":       st.Total,
			"claimed":     st.Claimed,
			"credits":     st.Credits,
			"already":     st.Already,
			"no_activity": st.NoAct,
			"failed":      st.Failed,
			"banned":      st.Banned,
			"rows":        rows,
		}
	})
	// 启动后延迟拉一次积分 + 每 10 分钟定期刷新：
	// 0 分账号被主动冷却踢出选号，保证「有分账号顶上」是写死的硬规则
	go func() {
		time.Sleep(5 * time.Second)
		sch.RunQuotaRefresh()
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sch.RunQuotaRefresh()
			}
		}
	}()

	srv := &http.Server{
		Addr: cfg.Listen,
		// WithRecover：兜住 handler 里的 panic —— 打完整栈 + 回干净的 500，
		// 不让客户端看到"连接被裸断"（见 internal/server/recover.go 的背景说明）
		Handler:           server.WithRecover(h),
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("lobsterai2api listening on %s (api_key=%v)", cfg.Listen, cfg.APIKey != "")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
	log.Printf("bye")
}
