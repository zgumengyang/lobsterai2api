package relay

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func idsOf(cs []*Channel) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.ID)
	}
	return out
}

func sameOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 优先级 = 路由顺序：新建排末尾、Match 取最靠前的、Reorder 生效、停用跳过、落盘可恢复。
func TestPriorityOrder(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "channels.json"))
	a := s.Add(&Channel{Name: "a", BaseURL: "http://a", APIKey: "k", Models: []string{"m1", "m2"}, Enabled: true})
	b := s.Add(&Channel{Name: "b", BaseURL: "http://b", APIKey: "k", Models: []string{"m1"}, Enabled: true})
	c := s.Add(&Channel{Name: "c", BaseURL: "http://c", APIKey: "k", Models: []string{"m1"}, Enabled: true})

	if got := idsOf(s.List()); !sameOrder(got, []string{a.ID, b.ID, c.ID}) {
		t.Fatalf("初始顺序 = %v（新建应该排末尾）", got)
	}
	if got := s.Match("m1"); got == nil || got.ID != a.ID {
		t.Fatalf("m1 应命中优先级最高的 a，实际 %v", got)
	}

	// 把 c 提到最前
	if n, ok := s.Reorder([]string{c.ID, a.ID, b.ID}); !ok || n != 3 {
		t.Fatalf("Reorder 失败 n=%d ok=%v", n, ok)
	}
	if got := s.Match("m1"); got == nil || got.ID != c.ID {
		t.Fatalf("重排后 m1 应命中 c，实际 %v", got)
	}
	if got := idsOf(s.List()); !sameOrder(got, []string{c.ID, a.ID, b.ID}) {
		t.Fatalf("重排后顺序 = %v", got)
	}

	// 停用的不参与路由
	s.SetEnabled(c.ID, false)
	if got := s.Match("m1"); got == nil || got.ID != a.ID {
		t.Fatalf("停用 c 后应命中 a，实际 %v", got)
	}

	// 未知 ID → 整个操作放弃，顺序不动
	if _, ok := s.Reorder([]string{"ch_nope"}); ok {
		t.Fatal("带未知 ID 的 Reorder 应该返回 false")
	}
	if got := idsOf(s.List()); !sameOrder(got, []string{c.ID, a.ID, b.ID}) {
		t.Fatalf("失败的重排不该改动顺序，实际 %v", got)
	}

	// 优先级要落盘：重启（重新 load）后顺序不变
	s2 := NewStore(s.file)
	if got := idsOf(s2.List()); !sameOrder(got, []string{c.ID, a.ID, b.ID}) {
		t.Fatalf("重启后顺序 = %v", got)
	}

	// 通配符兜底也吃优先级
	w := s.Add(&Channel{Name: "w", BaseURL: "http://w", APIKey: "k", Models: []string{"*"}, Enabled: true})
	if got := s.Match("没人声明的模型"); got == nil || got.ID != w.ID {
		t.Fatalf("通配渠道应兜住未知模型，实际 %v", got)
	}
}

// 老配置文件（没有 priority 字段）要按原顺序补编号，行为不变。
func TestLegacyFileKeepsOrder(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "channels.json")
	legacy := `{"channels":[
		{"id":"ch_1","name":"one","base_url":"http://1","api_key":"k","models":["m"],"enabled":true},
		{"id":"ch_2","name":"two","base_url":"http://2","api_key":"k","models":["m"],"enabled":true}
	],"default_upstream":"ch_1","fallback_target":"lobster"}`
	if err := writeFile(f, legacy); err != nil {
		t.Fatal(err)
	}
	s := NewStore(f)
	if got := idsOf(s.List()); !sameOrder(got, []string{"ch_1", "ch_2"}) {
		t.Fatalf("老文件顺序 = %v", got)
	}
	if got := s.Match("m"); got == nil || got.ID != "ch_1" {
		t.Fatalf("老文件里第一个渠道应该优先，实际 %v", got)
	}
	// 老文件没有 order → 账号池自动补在最末
	if got := s.Order(); !sameOrder(got, []string{"ch_1", "ch_2", "lobster"}) {
		t.Fatalf("老文件的默认顺序 = %v（账号池应在最末）", got)
	}
}

// 顺序表里能放龙虾账号池：池拖到最前 → 请求先走池；夹在中间 → 前面的渠道优先。
func TestPickWithPool(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "channels.json"))
	a := s.Add(&Channel{Name: "a", BaseURL: "http://a", APIKey: "k", Models: []string{"m"}, Enabled: true})
	b := s.Add(&Channel{Name: "b", BaseURL: "http://b", APIKey: "k", Models: []string{"m", "n"}, Enabled: true})

	if got := s.Order(); !sameOrder(got, []string{a.ID, b.ID, "lobster"}) {
		t.Fatalf("默认顺序 = %v（账号池应在最末）", got)
	}
	if got := s.Pick("m"); got != a.ID {
		t.Fatalf("m 应选中 a，实际 %q", got)
	}
	if got := s.Pick("n"); got != b.ID {
		t.Fatalf("n 应选中 b，实际 %q", got)
	}
	if got := s.Pick("没人声明的模型"); got != "lobster" {
		t.Fatalf("没人接管的模型应落账号池，实际 %q", got)
	}

	// ① 账号池拖到最前 → 什么都先走池
	if _, ok := s.Reorder([]string{"lobster", a.ID, b.ID}); !ok {
		t.Fatal("Reorder 失败")
	}
	if got := s.Order(); !sameOrder(got, []string{"lobster", a.ID, b.ID}) {
		t.Fatalf("拖动后顺序 = %v", got)
	}
	if got := s.Pick("m"); got != "lobster" {
		t.Fatalf("账号池在最前时 m 也该先走池，实际 %q", got)
	}

	// ② 账号池夹在中间 → 排它前面的渠道优先，后面的让位
	if _, ok := s.Reorder([]string{a.ID, "lobster", b.ID}); !ok {
		t.Fatal("Reorder 失败")
	}
	if got := s.Pick("m"); got != a.ID {
		t.Fatalf("m 应由池前面的 a 接管，实际 %q", got)
	}
	if got := s.Pick("n"); got != "lobster" {
		t.Fatalf("n 只有池后面的 b 接管 → 应先走池，实际 %q", got)
	}

	// ③ 未知 ID 整体放弃，顺序不动
	if _, ok := s.Reorder([]string{"lobster", "ch_nope"}); ok {
		t.Fatal("带未知 ID 的 Reorder 应返回 false")
	}
	if got := s.Order(); !sameOrder(got, []string{a.ID, "lobster", b.ID}) {
		t.Fatalf("失败的重排不该改动顺序，实际 %v", got)
	}

	// ④ 顺序落盘：重启后保持
	s2 := NewStore(s.file)
	if got := s2.Order(); !sameOrder(got, []string{a.ID, "lobster", b.ID}) {
		t.Fatalf("重启后顺序 = %v", got)
	}

	// ⑤ 新建渠道插在账号池前面（不会抢到池前头，也不会掉到池后面）
	c := s.Add(&Channel{Name: "c", BaseURL: "http://c", APIKey: "k", Models: []string{"m"}, Enabled: true})
	if got := s.Order(); !sameOrder(got, []string{a.ID, c.ID, "lobster", b.ID}) {
		t.Fatalf("新建后顺序 = %v", got)
	}

	// ⑥ 删渠道后顺序表不留残渣
	s.Delete(a.ID)
	if got := s.Order(); !sameOrder(got, []string{c.ID, "lobster", b.ID}) {
		t.Fatalf("删除后顺序 = %v", got)
	}
}
