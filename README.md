# lobsterai2api

把 **有道龙虾（LobsterAI）账号池**变成标准 **OpenAI 兼容接口**的网关，外加一个 Web 管理面板。
多账号调度、每日签到、token 自动续期、上游反代/回退、按 Key 限速与用量记账、调用流水与可视化，全部自带。

- 语言：Go（**零第三方依赖**，纯标准库）
- 端口：网关 `:8367`、面板 `:8368`（都可在 config 或环境变量里改）
- 面板额外支持 HTTPS 监听（`panel_tls`，给域名/隧道用）

---

## 目录结构

```
cmd/server          网关主程序（OpenAI 兼容入口 + 管理 API）
cmd/login           扫码 / 手机号登录，把凭证写进 auths/lobsterai-<uid>.json
cmd/credit          查账号积分
internal/auth       账号凭证的读写
internal/pool       账号池：选号顺序 / 冷却 / 禁用 / 状态机
internal/relay      上游反代（多上游、模型接管、模型名映射、优先级）
internal/scheduler  定时任务：每日签到、token 续期、余额刷新、邀请进度
internal/server     HTTP 路由与业务处理（对话、限速、用量、流水、管理 API）
internal/upstream   与龙虾服务器通信（登录 / 刷新 / 对话 / SSE 解析与消毒）
panel/              Web 管理面板（单文件前端 + 面板自身的登录/代理/HTTPS）
authhelper/         登录辅助小工具
tunnelclient/       隧道辅助小工具
```

---

## 快速开始

```bash
# 1) 配置：复制样例，按需改
cp config.example.json config.json
#   upstream.base_url  龙虾服务器地址
#   listen / 面板端口
#   api_keys           客户端要用的 Key（可多个）

# 2) 添加账号（浏览器扫码 / 手机号）
go build -o login.exe ./cmd/login && ./login.exe
#   也可以直接用面板的「手机登录」页

# 3) 跑网关
go build -o lobsterai2api.exe ./cmd/server && ./lobsterai2api.exe

# 4) 跑面板
go build -o lobsterai2api-panel.exe ./panel && ./lobsterai2api-panel.exe
```

客户端（OpenAI SDK / Cherry Studio / NextChat …）：

```
Base URL : http://<host>:8367/v1
API Key  : config.json 里 api_keys 的任意一个
模型     : 走 /v1/models 拉；要指定上游可写 <渠道名>/<模型名> 或 lobster/<模型名>
```

> 面板里也内置了 `/v1` 透传 —— 把面板挂到域名/隧道后，客户端 Base URL 可以直接用
> `https://<你的域名>/v1`，一个地址同时进面板和调接口。

---

## 账号池是怎么选号的

- **顺序用（默认）**：按账号「首次登录时间」升序，先把第一个号的积分用完；
  该号 0 分 / 冷却 / 连续报错 → 自动切下一个。
- **冷却梯度**：上游判定余额为 0 → 硬冷却 12 小时；429 限速 → 软冷却；连续错误 → 中冷却。
- **失败回退**：池子失败 → 按「优先级」依次试其它上游渠道，带请求形态记忆（同类失败不重复试）。
- **健康探测**：后台每 10 分钟刷一次余额，顺带刷新每个号的邀请进度。

---

## 面板能干什么

| 页 | 内容 |
|---|---|
| 数据概览 | 池子总积分、今日/累计 token、API 密钥数、上游路由统计、按天明细 |
| 账号池 | 在线/冷却/冻结状态、积分、今日单数·token·消耗、**邀请进度**、冻结/解冻、删除、清除冷却、立即续期、立即签到 |
| 上游反代 | 多上游渠道、模型接管、模型名映射、余额、后台登录态、优先级拖动排序 |
| API Key | 逐个 Key 的每分钟限速 / 每日额度 / 严格模式；调用流水（时间·消息·模型·token·缓存·费用），可筛日期、分页 |
| 后台管理 | 登录开关、数字验证码、改账号/密码、防护阈值、解锁 IP、最近登录记录 |
| 访问 IP | 谁在访问面板（按 IP + UA 聚合） |

面板自身的登录：账号密码 + 数字验证码 + 按 IP 限速与连错锁定；右上角用户牌菜单（后台管理 / 改密码 / 退出）。

---

## 定时任务（默认时间）

| 时间 | 干什么 |
|---|---|
| 09:00 / 21:00 | 给所有账号领「每日积分礼」 |
| 22:00 | 刷新所有账号 token（keepalive） |
| 每 10 分钟 | 刷新余额（0 分号主动排除）+ 刷新邀请进度 |

---

## HTTPS / 外网访问

两种办法：

1. **隧道**：`start-tunnel.bat`（Cloudflare 快速隧道）—— 地址是随机域名，重启会变。
2. **自己的域名 + 证书**：面板支持额外开一个 HTTPS 监听，配置写在 `config.json`：

```json
"panel_tls": {
  "addr": ":9443",
  "cert": "C:\\lobsterai2api\\cert\\live\\<域名>\\fullchain.pem",
  "key":  "C:\\lobsterai2api\\cert\\live\\<域名>\\privkey.pem"
}
```

证书没过期前只需重启面板即可生效；证书本身可以用 ACME 的 **DNS-01** 自动续（不依赖 80 端口）。

---

## 测试

```bash
go test ./...
```

账号池选号、冷却与解冻、限速与每日额度、请求形态记忆、SSE 消毒、签到链路、用量记账等都有单测。

---

## 部署建议（Windows）

- 网关和面板各挂一个**计划任务**，开机自启；升级时先备份 exe 再替换。
- 面板/网关都建议加一个**看守**：每分钟探测端口与 HTTP，挂了自动拉起并告警。
- `.gitignore` 已经排除敏感文件，别手动 `git add -f` 把它们传上去。

---

## 别把这些传上来

```
data/       账号池状态、API Key 统计、调用流水、登录记录、面板凭证
auths/      各账号的 token（等于账号密码）
config.json 含 API Key 等配置
cert/       证书私钥、DNS API 密钥
HANDOFF.md  开发/运维记录（含服务器与账号细节）
```

## 许可

见 [LICENSE](LICENSE)。
