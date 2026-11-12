@echo off
REM ============================================================
REM market-monitor launcher  (called by Windows Task Scheduler)
REM   - picks a Python interpreter that CAN import sqlite3 + numpy
REM   - runs market_monitor.py in this folder
REM   - the program itself exits after the last session closes
REM
REM Override the interpreter with:  set MONITOR_PYTHON=<full path>
REM Note: output is kept in English on purpose (console codepage).
REM ============================================================
setlocal

REM ---------- 1. resolve python ----------
REM An explicit override always wins.
if defined MONITOR_PYTHON (
    set "PY=%MONITOR_PYTHON%"
    goto :run
)

REM Otherwise probe the usual conda locations and keep the first that
REM can actually import sqlite3 + numpy (some envs ship a broken
REM _sqlite3 DLL, which market_monitor.py needs).
for %%P in (
    "C:\Users\force\.conda\envs\python310\python.exe"
    "C:\Users\force\.conda\envs\bigtrade\python.exe"
    "C:\ProgramData\miniconda3\envs\python310\python.exe"
    "%USERPROFILE%\miniconda3\envs\python310\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
) do (
    if not defined PY if exist %%P (
        %%P -c "import sqlite3, numpy" >nul 2>nul && set "PY=%%~P"
    )
)

if not defined PY (
    for /f "delims=" %%I in ('where python 2^>nul') do (
        if not defined PY (
            "%%I" -c "import sqlite3, numpy" >nul 2>nul && set "PY=%%I"
        )
    )
)

if not defined PY (
    echo [ERROR] No usable Python found - needs sqlite3 ^+ numpy.
    echo         Please set MONITOR_PYTHON to the full python.exe path.
    exit /b 1
)

:run
REM ---------- 2. run ----------
cd /d "%~dp0"
echo [START] %date% %time%  python=%PY%
"%PY%" market_monitor.py %*
set "RC=%ERRORLEVEL%"
echo [EXIT ] %date% %time%  rc=%RC%
endlocal & exit /b %RC%
