@echo off
chcp 65001 >nul
echo Deleting sensitive account files and directories...
echo.

setlocal enabledelayedexpansion

rem ============================================================
rem  Log cleanup: set LOG_ROOT to your log root directory.
rem  Directory layout expected:
rem    LOG_ROOT\1..12\2026-10-20\RHMonitor_2026-10-20 10-3[001].log
rem  You can also drag a folder onto this .bat, or pass it as
rem  the first argument, to override the path below.
rem ============================================================
set "LOG_ROOT=D:\Program Files\融航资管交易平台风控端\log_files\logfiles-2026"
if not "%~1"=="" set "LOG_ROOT=%~1"

set "found=0"
set "temp_list=%TEMP%\delete_account_files.txt"

if exist "%temp_list%" del /f /q "%temp_list%"

for %%p in (
    yuqj0821
    fy0228
    yqj0929
    wangxy0617
    wangk0402
    sxk0812
    yq02
    WQ1017
    17156
    17882
) do (
    dir /s /b "*%%p_*" 2>nul >>"%temp_list%"
)

if not exist "%temp_list%" (
    echo No matching files found.
    goto end
)

for /f "delims=" %%a in ('type "%temp_list%" ^| sort /r') do (
    set "item=%%a"
    if exist "%%a\*" (
        echo Deleting directory: "%%a"
        rmdir /s /q "%%a"
    ) else if exist "%%a" (
        echo Deleting file: "%%a"
        del /f /q "%%a"
    )
    set "found=1"
)

if exist "%temp_list%" del /f /q "%temp_list%"

:end
echo.
if !found!==0 (
    echo No matching files found.
) else (
    echo Done.
)

echo.
call :clean_logs

endlocal
pause
exit /b

rem ============================================================
rem  Delete all log directories older than today.
rem  Month dirs before the current month are removed entirely
rem  (covers the cross-month case); within the current month
rem  only date dirs earlier than today are removed.
rem ============================================================
:clean_logs
if not exist "%LOG_ROOT%" (
    echo [LOG] Root directory not found: "%LOG_ROOT%" - skipped.
    exit /b
)

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "today=%%i"
set "todayMonth=%today:~4,2%"
if "%todayMonth:~0,1%"=="0" set "todayMonth=%todayMonth:~1%"

echo [LOG] Today: %today%, keeping logs from %today% onwards.
echo [LOG] Scanning "%LOG_ROOT%" ...

for /d %%m in ("%LOG_ROOT%\*") do (
    set "mn=%%~nxm"
    if "!mn:~0,1!"=="0" set "mn=!mn:~1!"
    if not "!mn!"=="" (
        if !mn! LSS !todayMonth! (
            echo [LOG] Delete month dir: "%%~fm"
            rmdir /s /q "%%~fm"
        ) else if !mn! EQU !todayMonth! (
            call :clean_month "%%~fm"
        )
    )
)
echo [LOG] Done.
exit /b

:clean_month
for /d %%d in ("%~1\*") do (
    for /f "tokens=1-3 delims=-" %%a in ("%%~nxd") do (
        set "dn=%%a%%b%%c"
        if not "!dn:~7,1!"=="" if "!dn:~8,1!"=="" if "!dn!" LSS "!today!" (
            echo [LOG] Delete date dir: "%%~fd"
            rmdir /s /q "%%~fd"
        )
    )
)
exit /b
