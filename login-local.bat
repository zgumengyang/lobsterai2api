@echo off
chcp 65001 >nul
title LobsterAI 扫码登录一条龙
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0login-local.ps1"
