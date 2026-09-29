@echo off
chcp 65001 >nul
title LobsterAI2API - 反代服务
cd /d "%~dp0"

echo ============================================================
echo   LobsterAI2API 一键启动
echo   监听:  http://127.0.0.1:8367
echo   API Key: sk-lobster-local-2026
echo   Base URL: http://127.0.0.1:8367/v1
echo ============================================================
echo.

rem 上游地址（必填，代码只认环境变量，不认 config.json 里的 base_url）
set LB2A_UPSTREAM_BASE=https://lobsterai-server.youdao.com
set LB2A_LOGIN_PORTAL=https://lobsterai.youdao.com

echo [1/2] 检查可执行文件...
if not exist "%~dp0lobsterai2api.exe" (
    echo [错误] 找不到 lobsterai2api.exe，请先执行 go build
    pause
    exit /b 1
)
if not exist "%~dp0auths" (
    echo [提示] auths 目录不存在，先登录账号
    mkdir "%~dp0auths"
)

echo [2/2] 启动服务（Ctrl+C 停止）...
echo.
"%~dp0lobsterai2api.exe" -config "%~dp0config.json"

echo.
echo 服务已退出
pause
