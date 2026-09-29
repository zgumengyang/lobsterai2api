$ErrorActionPreference = "Stop"

Write-Output "=== 1/3 杀掉残留面板进程 ==="
Get-Process lobsterai2api-panel -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Output "=== 2/3 注册计划任务 ==="
$action = New-ScheduledTaskAction -Execute "C:\lobsterai2api\lobsterai2api-panel.exe" -WorkingDirectory "C:\lobsterai2api"
$triggerBoot = New-ScheduledTaskTrigger -AtStartup
$triggerLogon = New-ScheduledTaskTrigger -AtLogOn -User "Administrator"
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 3650) -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "LobsterAI2API-Panel" -Action $action -Trigger $triggerBoot -Settings $settings -Principal $principal -Force | Out-Null
Write-Output "task registered"

Write-Output "=== 3/3 启动并验证 ==="
Start-ScheduledTask -TaskName "LobsterAI2API-Panel"
Start-Sleep -Seconds 3

$p = Get-Process lobsterai2api-panel -ErrorAction SilentlyContinue
if ($p) { Write-Output "panel running pid=$($p.Id)" } else { Write-Output "PANEL NOT RUNNING" }

try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:8368/healthz" -TimeoutSec 10 -UseBasicParsing
    Write-Output ("healthz: " + $r.Content)
} catch {
    Write-Output "healthz error: $($_.Exception.Message)"
}

Write-Output "==== 面板代理验证 ===="
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8368/api/status" -TimeoutSec 10
    Write-Output ("proxy status: " + ($r | ConvertTo-Json -Compress))
} catch {
    Write-Output "proxy error: $($_.Exception.Message)"
}
