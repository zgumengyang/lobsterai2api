@echo off
chcp 65001 >nul
title Relay admin login - auto copy login state
cd /d "%~dp0"

rem Default landing page is defined in relay-login.js (register page with referral code).
rem To use another site: drag-and-drop it onto this file, or pass it as an argument,
rem e.g.  relay-login.bat https://yuzu.p8.ink
set "SITE=%~1"

set "NODE="
where node >nul 2>nul
if not errorlevel 1 set "NODE=node"
if "%NODE%"=="" if exist "C:\Users\Administrator\Tools\nodejs\node.exe" set "NODE=C:\Users\Administrator\Tools\nodejs\node.exe"
if "%NODE%"=="" if exist "C:\Program Files\nodejs\node.exe" set "NODE=C:\Program Files\nodejs\node.exe"
if "%NODE%"=="" (
  echo [ERROR] node.exe not found. Please install Node.js first.
  pause
  exit /b 1
)

"%NODE%" "%~dp0relay-login.js" "%SITE%"
echo.
pause
