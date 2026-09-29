$ErrorActionPreference = "Stop"
Set-Location C:\lobsterai2api

Write-Output "=== 1/5 写多 Key 配置 ==="
$cfg = @{
    api_key  = "sk-lobster-local-2026"
    api_keys = @(
        "sk-lobster-local-2026",
        "sk-lobster-2ed8581d4714f3fb0fe4969b"
    )
    auth_dir = "./auths"
    state_file = "./data/state.json"
    listen = ":8367"
    cooldown = @{
        hard_credit = "12h"
        soft_rate = "60s"
        err_threshold = 3
        err_cooldown = "10m"
    }
    schedule = @{
        checkin_hours = @(9, 21)
        keepalive_hours = @(22)
    }
    upstream = @{
        base_url = "https://lobsterai-server.youdao.com"
        timeout_seconds = 180
    }
}
$json = $cfg | ConvertTo-Json -Depth 5
[System.IO.File]::WriteAllText("C:\lobsterai2api\config.json", $json, (New-Object System.Text.UTF8Encoding $false))
Write-Output "config written"

Write-Output "=== 2/5 停服务 ==="
Get-Process lobsterai2api, lobsterai2api-panel -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

Write-Output "=== 3/5 替换 exe ==="
Move-Item -Force "C:\lobsterai2api\lobsterai2api-main-new.exe" "C:\lobsterai2api\lobsterai2api.exe"
if (Test-Path "C:\lobsterai2api\lobsterai2api-panel-new.exe") {
    Move-Item -Force "C:\lobsterai2api\lobsterai2api-panel-new.exe" "C:\lobsterai2api\lobsterai2api-panel.exe"
}
Write-Output "exe replaced"

Write-Output "=== 4/5 启动服务 ==="
Start-ScheduledTask -TaskName "LobsterAI2API"
Start-ScheduledTask -TaskName "LobsterAI2API-Panel"
Start-Sleep -Seconds 4

Write-Output "=== 5/5 验证 ==="
Write-Output "-- 进程 --"
Get-Process lobsterai2api, lobsterai2api-panel -ErrorAction SilentlyContinue | Select-Object ProcessName, Id
Write-Output "-- 面板 Key 列表 --"
try { (Invoke-RestMethod -Uri "http://127.0.0.1:8368/api/apikey" -TimeoutSec 10) | ConvertTo-Json -Compress } catch { $_.Exception.Message }
Write-Output "-- 老 Key sk-lobster-local-2026 是否有效 --"
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8367/v1/models" -Headers @{Authorization="Bearer sk-lobster-local-2026"} -TimeoutSec 10
    Write-Output "老 Key: OK ($($r.data.Count) 模型)"
} catch { Write-Output "老 Key: 失败 $($_.Exception.Message)" }
Write-Output "-- 新 Key sk-lobster-2ed8... 是否有效 --"
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8367/v1/models" -Headers @{Authorization="Bearer sk-lobster-2ed8581d4714f3fb0fe4969b"} -TimeoutSec 10
    Write-Output "新 Key: OK ($($r.data.Count) 模型)"
} catch { Write-Output "新 Key: 失败 $($_.Exception.Message)" }
