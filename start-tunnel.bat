@echo off
chcp 65001 >nul
title LobsterAI2API - 公网隧道
cd /d "%~dp0"

echo ============================================================
echo   LobsterAI2API 公网隧道（Cloudflare 快速隧道）
echo   前提: start.bat 服务已在跑 (127.0.0.1:8367)
echo   说明: 启动后看屏幕上的 trycloudflare.com 地址即为公网入口
echo   注意: 快速隧道地址每次重启会变，长期用请配置命名隧道
echo ============================================================
echo.

if not exist "%~dp0bin\cloudflared.exe" (
    echo [错误] 找不到 cloudflared.exe，请先下载到 bin\ 目录
    pause
    exit /b 1
)

curl.exe -s --max-time 3 http://127.0.0.1:8367/status >nul 2>&1
if errorlevel 1 (
    echo [警告] 本地服务 8367 没响应！先开 start.bat 再跑本脚本
    pause
    exit /b 1
)

echo 本地服务在线，启动隧道（Ctrl+C 停止）...
echo.
"%~dp0bin\cloudflared.exe" tunnel --url http://127.0.0.1:8367 --no-autoupdate

echo.
echo 隧道已退出
pause
