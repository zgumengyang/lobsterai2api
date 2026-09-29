@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ==== 服务状态 ====
curl.exe -s --max-time 5 http://127.0.0.1:8367/status
echo.
echo.
echo ==== 模型数量 ====
curl.exe -s --max-time 5 http://127.0.0.1:8367/v1/models -H "Authorization: Bearer sk-lobster-local-2026" | find /c "object"
echo.
echo ==== 快速对话测试 ====
curl.exe -s --max-time 30 http://127.0.0.1:8367/v1/chat/completions ^
  -H "Authorization: Bearer sk-lobster-local-2026" ^
  -H "Content-Type: application/json" ^
  -d "{\"model\":\"deepseek-flash\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"stream\":false}"
echo.
pause
