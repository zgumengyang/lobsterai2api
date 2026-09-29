package upstream

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"lobsterai2api/internal/auth"
)

// 每日积分礼三步链路：slot → context → actions/check_in，顺带校验请求形状。
func TestDailyCheckinFlow(t *testing.T) {
	var slotQuery, ctxQuery, actionBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/client-activities/slot":
			slotQuery = r.URL.RawQuery
			io.WriteString(w, `{"code":0,"message":"success","data":{"slotState":"available","activity":{"activityCode":"daily-check-in-evergreen-prod-20260814","configRevision":1,"activityType":"daily_check_in"}}}`)
		case r.URL.Path == "/api/client-activities/daily-check-in-evergreen-prod-20260814/context":
			ctxQuery = r.URL.RawQuery
			io.WriteString(w, `{"code":0,"message":"success","data":{"state":{"claimedToday":false,"rewardCredits":100}}}`)
		case r.URL.Path == "/api/client-activities/daily-check-in-evergreen-prod-20260814/actions/check_in":
			b, _ := io.ReadAll(r.Body)
			actionBody = string(b)
			io.WriteString(w, `{"code":0,"message":"success","data":{"replayed":false,"result":{"creditsGranted":100,"periodKey":"2026-09-21"}}}`)
		default:
			w.WriteHeader(404)
			io.WriteString(w, `{"code":404,"message":"not found"}`)
		}
	}))
	defer srv.Close()
	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)

	c := New()
	res, err := c.DailyCheckin(&auth.Auth{AccessToken: "tok"})
	if err != nil {
		t.Fatalf("签到失败: %v", err)
	}
	if res.ActivityCode != "daily-check-in-evergreen-prod-20260814" || res.Credits != 100 || res.AlreadyToday {
		t.Fatalf("结果不对: %+v", res)
	}
	for _, kv := range []string{"placement=desktop_sidebar", "containerApiVersion=2", "platform=win32"} {
		if !strings.Contains(slotQuery, kv) {
			t.Errorf("slot 查询串缺 %s: %s", kv, slotQuery)
		}
	}
	if !strings.Contains(ctxQuery, "configRevision=1") {
		t.Errorf("context 查询串缺 configRevision: %s", ctxQuery)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(actionBody), &body); err != nil {
		t.Fatalf("领取体不是 JSON: %s", actionBody)
	}
	if body["configRevision"] != float64(1) {
		t.Errorf("configRevision = %v", body["configRevision"])
	}
	key, _ := body["idempotencyKey"].(string)
	if !strings.HasPrefix(key, "daily-check-in-") {
		t.Errorf("idempotencyKey 形状不对: %q", key)
	}
	if _, ok := body["payload"]; !ok {
		t.Error("body 里缺 payload")
	}
}

// 今天已经领过 → AlreadyToday，且不应该再发领取请求。
func TestDailyCheckinAlreadyToday(t *testing.T) {
	claimed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/client-activities/slot":
			io.WriteString(w, `{"code":0,"message":"ok","data":{"slotState":"available","activity":{"activityCode":"act","configRevision":3}}}`)
		case strings.HasSuffix(r.URL.Path, "/context"):
			io.WriteString(w, `{"code":0,"message":"ok","data":{"state":{"claimedToday":true}}}`)
		default:
			claimed = true
			io.WriteString(w, `{"code":0,"message":"ok","data":{}}`)
		}
	}))
	defer srv.Close()
	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)
	res, err := New().DailyCheckin(&auth.Auth{AccessToken: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.AlreadyToday || res.Credits != 0 {
		t.Fatalf("应判为已领: %+v", res)
	}
	if claimed {
		t.Error("已领的情况下不该再发 check_in 请求")
	}
}

// 上游没有活动 → NoActivity，不算失败。
func TestDailyCheckinNoActivity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":0,"message":"ok","data":{"slotState":"empty"}}`)
	}))
	defer srv.Close()
	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)
	res, err := New().DailyCheckin(&auth.Auth{AccessToken: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.NoActivity {
		t.Fatalf("应判为无活动: %+v", res)
	}
}
