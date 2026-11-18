@echo off
chcp 65001 >nul
cd /d "%~dp0"

rem 优先用环境变量指定的解释器，其次 conda base（本机实测装有 Flask），最后退回 PATH 里的 python
set "PY=%POSITIONS_PYTHON%"
if not defined PY set "PY=E:\ProgramData\miniconda3\python.exe"
if not exist "%PY%" set "PY=python"

echo 使用解释器: %PY%
"%PY%" app.py

pause
