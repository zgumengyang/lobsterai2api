$ErrorActionPreference = "Stop"

Write-Output "=== 1/4 设置系统环境变量 ==="
[Environment]::SetEnvironmentVariable("LB2A_UPSTREAM_BASE","https://lobsterai-server.youdao.com","Machine")
[Environment]::SetEnvironmentVariable("LB2A_LOGIN_PORTAL","https://lobsterai.youdao.com","Machine")
Write-Output "env ok"

Write-Output "=== 2/4 杀掉残留进程 ==="
Get-Process lobsterai2api -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Output "=== 3/4 注册计划任务(开机自启+登录自启) ==="
$action = New-ScheduledTaskAction -Execute "C:\lobsterai2api\lobsterai2api.exe" -Argument "-config C:\lobsterai2api\config.json" -WorkingDirectory "C:\lobsterai2api"
$triggerBoot = New-ScheduledTaskTrigger -AtStartup
$triggerLogon = New-ScheduledTaskTrigger -AtLogOn -User "Administrator"
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 3650) -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "LobsterAI2API" -Action $action -Trigger $triggerBoot -Settings $settings -Principal $principal -Force | Out-Null
Write-Output "task registered"

Write-Output "=== 4/4 启动任务并验证 ==="
Start-ScheduledTask -TaskName "LobsterAI2API"
Start-Sleep -Seconds 4

Get-ScheduledTask -TaskName "LobsterAI2API" | Select-Object TaskName,State
$p = Get-Process lobsterai2api -ErrorAction SilentlyContinue
if ($p) { Write-Output "process running pid=$($p.Id)" } else { Write-Output "PROCESS NOT RUNNING" }

try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8367/status" -TimeoutSec 10
    Write-Output ("status: " + ($r | ConvertTo-Json -Compress))
} catch {
    Write-Output "status error: $($_.Exception.Message)"
}
