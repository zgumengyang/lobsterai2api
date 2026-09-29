[Environment]::SetEnvironmentVariable("LB2A_UPSTREAM_BASE","https://lobsterai-server.youdao.com","Machine")
[Environment]::SetEnvironmentVariable("LB2A_LOGIN_PORTAL","https://lobsterai.youdao.com","Machine")
Write-Output "env set"

$proc = Get-Process lobsterai2api -ErrorAction SilentlyContinue
if ($proc) {
    Write-Output "service already running, stopping old..."
    Stop-Process -Name lobsterai2api -Force
    Start-Sleep -Seconds 2
}

Start-Process -FilePath "C:\lobsterai2api\lobsterai2api.exe" -ArgumentList "-config","C:\lobsterai2api\config.json" -WorkingDirectory "C:\lobsterai2api" -WindowStyle Hidden
Start-Sleep -Seconds 3

$p = Get-Process lobsterai2api -ErrorAction SilentlyContinue
if ($p) { Write-Output "service started, pid=$($p.Id)" } else { Write-Output "SERVICE FAILED TO START" }

Write-Output "==== local test ===="
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8367/status" -TimeoutSec 10
    $r | ConvertTo-Json -Compress
} catch {
    Write-Output "status check error: $($_.Exception.Message)"
}
