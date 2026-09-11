@echo off
REM =====================================================================
REM One-click register: Weekly Monday 10:00 refresh Top3 main contracts
REM HOW TO USE:
REM   1. Right-click this .bat -> "Run as Administrator" (schtasks /Create requires admin)
REM   2. On success, Task Scheduler library shows: low-low-up_RefreshTop3MainContracts_Weekly
REM   3. To DELETE: admin cmd ->  schtasks /Delete /TN "low-low-up_RefreshTop3MainContracts_Weekly" /F
REM =====================================================================
setlocal

REM =================== CONFIG (safe to edit) ==========================
set "TASKNAME=low-low-up_RefreshTop3MainContracts_Weekly"
set "RUN_BAT=D:\projects\low-low-up\utils\refresh_top3_main_contracts.bat"
set "RUNAS_USER=%USERDOMAIN%\%USERNAME%"

REM Schedule: weekly Monday 10:00. Change SCHEDULE_TIME=09:10 if you want earlier.
set "SCHEDULE_SC=WEEKLY"
set "SCHEDULE_DAY=MON"
set "SCHEDULE_TIME=10:00"
REM =================== END CONFIG =====================================

REM --- Guard: target .bat must exist ---
if not exist "%RUN_BAT%" (
    echo [ERROR] Target script not found: %RUN_BAT%
    echo         Please make sure refresh_top3_main_contracts.bat exists.
    echo.
    pause
    exit /b 1
)

REM --- Remove old task with same name (ignore error if not exists) ---
echo [INFO] Cleaning old task (if any): %TASKNAME%
schtasks /Delete /TN "%TASKNAME%" /F >nul 2>&1

echo.
echo [INFO] Creating scheduled task:
echo        Task Name : %TASKNAME%
echo        Run As    : %RUNAS_USER%
echo        Schedule  : %SCHEDULE_SC% / Day=%SCHEDULE_DAY% / Time=%SCHEDULE_TIME%
echo        Execute   : %RUN_BAT%
echo.

REM --- Create task. Use MINIMAL parameter set to maximize compatibility.
REM     Notes:
REM      * /DESCRIPTION and /RL LIMITED are intentionally REMOVED because
REM        older Windows 10/11 editions (Home/CN builds) frequently reject
REM        these args with "Invalid argument/option - '/DESCRIPTION'".
REM      * Single /ST only. TR path properly quoted.
schtasks /Create ^
    /SC %SCHEDULE_SC% ^
    /D  %SCHEDULE_DAY% ^
    /ST %SCHEDULE_TIME% ^
    /TN "%TASKNAME%" ^
    /TR "\"%RUN_BAT%\"" ^
    /RU "%RUNAS_USER%" ^
    /F

set "EC=%ERRORLEVEL%"

echo.
if %EC% EQU 0 (
    echo [OK] Task created successfully.
    echo.
    echo Useful commands:
    echo   Query status   : schtasks /Query /TN "%TASKNAME%" /FO LIST /V
    echo   RUN RIGHT NOW  : schtasks /Run   /TN "%TASKNAME%"
    echo   Stop running   : schtasks /End   /TN "%TASKNAME%"
    echo   Delete task    : schtasks /Delete /TN "%TASKNAME%" /F
    echo.
    echo Tip: run "RUN RIGHT NOW" once to verify output files refresh under data\contracts\.
) else (
    echo [ERROR] schtasks /Create FAILED (exitcode=%EC%).
    echo Common causes:
    echo   1. This .bat was NOT launched "As Administrator".
    echo      -> Fix: right-click the bat -^> Run as administrator.
    echo   2. Current user cannot create scheduled tasks.
    echo      -> Try re-running from an admin cmd.exe console.
    echo   3. Path contains non-ASCII characters (unlikely for RUN_BAT used here).
)

echo.
pause
endlocal
