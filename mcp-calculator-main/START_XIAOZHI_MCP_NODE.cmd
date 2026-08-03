@echo off
title XiaoZhi MCP Internet - Node
cd /d D:\XiaoZhiTools\mcp-calculator\mcp-calculator-main

echo.
echo MO TRANG XIAOZHI VA SAO CHEP DIA CHI MCP.
echo SAU DO QUAY LAI CUA SO NAY VA NHAN PHIM BAT KY.
echo KHONG DAN DIA CHI MCP VAO CMD.
echo.
pause >nul

powershell -NoProfile -Command "$ep=(Get-Clipboard -Raw).Trim(); if(-not $ep.StartsWith('wss://api.xiaozhi.me/mcp/')){Write-Host 'MCP endpoint khong dung'; exit 1}; $env:MCP_ENDPOINT=$ep; Set-Clipboard -Value 'DA_XOA'; & node '.\mcp_pipe_node.js'"

if errorlevel 1 (
  echo.
  echo DIA CHI MCP KHONG DUNG HOAC CHUONG TRINH GAP LOI.
  echo HAY DONG CUA SO VA THU LAI.
  pause
  exit /b 1
)

echo.
echo MCP DA DUNG. NHAN PHIM BAT KY DE DONG.
pause >nul