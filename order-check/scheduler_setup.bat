@echo off
REM =====================================================
REM LowLowUp Scheduler Setup (English output to avoid encoding issues)
REM =====================================================

echo =====================================================
echo LowLowUp Scheduler Setup
echo =====================================================

REM Delete existing tasks
echo Deleting old tasks...
schtasks /delete /tn "LowLowUp_AM_Start" /f 2>nul
schtasks /delete /tn "LowLowUp_PM_Start" /f 2>nul
schtasks /delete /tn "LowLowUp_Night_Start" /f 2>nul

REM Set Python path
set PYTHON_PATH=C:\ProgramData\miniconda3\envs\python310\python.exe

REM Session 1: 08:59 AM start
echo Creating session 1 task (08:59)...
schtasks /create /tn "LowLowUp_AM_Start" /tr "\"%PYTHON_PATH%\" \"C:\projects\low-low-up\order-check\run_pipeline.py\" simu" /sc DAILY /st 08:59 /f

REM Session 2: 12:59 PM start
echo Creating session 2 task (12:59)...
schtasks /create /tn "LowLowUp_PM_Start" /tr "\"%PYTHON_PATH%\" \"C:\projects\low-low-up\order-check\run_pipeline.py\" simu" /sc DAILY /st 12:59 /f

REM Session 3: 20:59 start
echo Creating session 3 task (20:59)...
schtasks /create /tn "LowLowUp_Night_Start" /tr "\"%PYTHON_PATH%\" \"C:\projects\low-low-up\order-check\run_pipeline.py\" simu" /sc DAILY /st 20:59 /f

echo.
echo =====================================================
echo Scheduler created successfully!
echo.
echo Tasks:
echo   - LowLowUp_AM_Start    (08:59 start)
echo   - LowLowUp_PM_Start   (12:59 start)
echo   - LowLowUp_Night_Start (20:59 start)
echo.
echo Program will auto-exit at:
echo   - 11:30 (AM session close)
echo   - 15:15 (PM session close)
echo   - 02:30 (Night session close)
echo =====================================================
pause