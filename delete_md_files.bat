@echo off
echo Deleting sensitive account files and directories...
echo.

setlocal enabledelayedexpansion

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

endlocal
pause
