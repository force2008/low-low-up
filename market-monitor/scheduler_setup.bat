@echo off
REM ============================================================
REM Register / refresh the market-monitor scheduled tasks
REM
REM   Run this file ONCE (no admin needed for user-level tasks).
REM   It (re)creates 3 daily tasks that launch start_monitor.bat:
REM     08:59  morning   -> program exits at 11:30
REM     12:59  afternoon -> program exits at 15:00 / 15:15
REM     20:59  night     -> program exits at 23:00 / 01:00 / 02:30
REM
REM   Uninstall:  scheduler_setup.bat uninstall
REM   The program also self-guards: it will not run on a non trading
REM   day and will not start a second instance while one is running.
REM
REM Note: output is kept in English on purpose (console codepage).
REM ============================================================
setlocal

set "TASK_BAT=%~dp0start_monitor.bat"
set "TASK_PREFIX=MarketMonitor"

if /i "%~1"=="uninstall" goto :uninstall

if not exist "%TASK_BAT%" (
    echo [ERROR] not found: %TASK_BAT%
    pause
    exit /b 1
)

echo ============================================================
echo  Removing old tasks ...
echo ============================================================
schtasks /delete /tn "%TASK_PREFIX%_Morning"   /f >nul 2>nul
schtasks /delete /tn "%TASK_PREFIX%_Afternoon" /f >nul 2>nul
schtasks /delete /tn "%TASK_PREFIX%_Night"     /f >nul 2>nul

echo ============================================================
echo  Creating tasks ...
echo ============================================================
schtasks /create /tn "%TASK_PREFIX%_Morning"   /tr "\"%TASK_BAT%\"" /sc DAILY /st 08:59 /f
schtasks /create /tn "%TASK_PREFIX%_Afternoon" /tr "\"%TASK_BAT%\"" /sc DAILY /st 12:59 /f
schtasks /create /tn "%TASK_PREFIX%_Night"     /tr "\"%TASK_BAT%\"" /sc DAILY /st 20:59 /f

echo.
echo ============================================================
echo  Done.
echo    %TASK_PREFIX%_Morning    08:59 -^> exits 11:30
echo    %TASK_PREFIX%_Afternoon  12:59 -^> exits 15:00 / 15:15
echo    %TASK_PREFIX%_Night      20:59 -^> exits 23:00 / 01:00 / 02:30
echo.
echo  Check with:  schtasks /query /tn "%TASK_PREFIX%_Morning"
echo  Run now   :  schtasks /run   /tn "%TASK_PREFIX%_Morning"
echo ============================================================
pause
exit /b 0

:uninstall
echo Removing tasks ...
schtasks /delete /tn "%TASK_PREFIX%_Morning"   /f >nul 2>nul
schtasks /delete /tn "%TASK_PREFIX%_Afternoon" /f >nul 2>nul
schtasks /delete /tn "%TASK_PREFIX%_Night"     /f >nul 2>nul
echo Done.
pause
exit /b 0
