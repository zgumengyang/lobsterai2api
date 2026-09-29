@echo off
chcp 65001 >nul
title LobsterAI2API - 添加账号
cd /d "%~dp0"

set LB2A_UPSTREAM_BASE=https://lobsterai-server.youdao.com
set LB2A_LOGIN_PORTAL=https://lobsterai.youdao.com

echo ============================================================
echo   添加新账号（OAuth 扫码登录）
echo   提示：浏览器打开输出的链接，手机号/微信登录
echo ============================================================
echo.

echo [1/2] 生成授权链接...
"%~dp0login.exe" url
echo.
echo 浏览器完成登录后按任意键继续...
pause >nul

echo.
echo [2/2] 获取 token 并保存...
"%~dp0login.exe" poll

echo.
echo 完成！重启 start.bat 加载新账号
pause
