# 本机扫码登录一条龙：生成登录链接 → 打开浏览器 → 扫码 → 产出 auth JSON 到剪贴板
# 用法: 双击 login-local.bat
$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)

# 环境变量（上游/门户）
$env:LB2A_UPSTREAM_BASE = "https://lobsterai-server.youdao.com"
$env:LB2A_LOGIN_PORTAL  = "https://lobsterai.youdao.com"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  LobsterAI 扫码登录一条龙" -ForegroundColor Cyan
Write-Host "  流程: 生成链接 → 自动开浏览器 → 手机扫码 → 复制 JSON" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# 清理旧状态
Remove-Item "$env:TEMP\lb2api-login-state.json*" -ErrorAction SilentlyContinue

# 起回调服务器（login url 会一直挂着等回调，后台跑）
Write-Host "[1/4] 启动本地回调服务器..." -ForegroundColor Yellow
$proc = Start-Process -FilePath ".\login.exe" -ArgumentList "url" -WorkingDirectory (Get-Location) -PassThru -NoNewWindow -RedirectStandardOutput "$env:TEMP\lb2api-login-url.txt" -RedirectStandardError "$env:TEMP\lb2api-login-err.txt"

# 等 URL 出现（最多 10 秒）
$url = $null
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 500
    if (Test-Path "$env:TEMP\lb2api-login-url.txt") {
        $url = (Get-Content "$env:TEMP\lb2api-login-url.txt" -Raw -ErrorAction SilentlyContinue).Trim()
        if ($url -match "^https?://") { break }
    }
}

if (-not $url) {
    Write-Host "[错误] 拿不到登录链接，看错误: " -ForegroundColor Red
    Get-Content "$env:TEMP\lb2api-login-err.txt" -ErrorAction SilentlyContinue
    pause
    exit 1
}

Write-Host "[2/4] 打开浏览器..." -ForegroundColor Yellow
Start-Process $url
Write-Host "  登录链接: $url" -ForegroundColor Gray
Write-Host "  请在打开的页面用手机号/微信扫码登录" -ForegroundColor Gray
Write-Host ""

# 等 login url 进程自己结束（回调完成自动退出，超时 10 分钟）
Write-Host "[3/4] 等待扫码完成..." -ForegroundColor Yellow
Wait-Process -Id $proc.Id -Timeout 620 -ErrorAction SilentlyContinue
if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) {
    Write-Host "[超时] 10 分钟未完成登录" -ForegroundColor Red
    Stop-Process -Id $proc.Id -Force
    pause
    exit 1
}

# 读 auth 文件（最新一个）
Write-Host "[4/4] 读取 auth JSON..." -ForegroundColor Yellow
$authFile = Get-ChildItem ".\auths\lobsterai-*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $authFile) {
    Write-Host "[错误] auths 目录没有生成文件，看 login 错误:" -ForegroundColor Red
    Get-Content "$env:TEMP\lb2api-login-err.txt" -ErrorAction SilentlyContinue
    pause
    exit 1
}

$json = Get-Content $authFile.FullName -Raw
Set-Clipboard -Value $json

Write-Host "  生成文件: $($authFile.FullName)" -ForegroundColor Green
Write-Host "  JSON 已复制到剪贴板！" -ForegroundColor Green
Write-Host ""
Write-Host "下一步: 打开面板 http://124.221.39.166:8368" -ForegroundColor Cyan
Write-Host "  → 「添加账号」→ 完整 auth JSON 框里 Ctrl+V 粘贴 → 点「添加账号并重载」" -ForegroundColor Cyan
Write-Host ""
Write-Host "（JSON 内容预览，确认无误后可直接复制给其他机器用）" -ForegroundColor Gray
Write-Host $json -ForegroundColor Gray
Write-Host ""
pause
