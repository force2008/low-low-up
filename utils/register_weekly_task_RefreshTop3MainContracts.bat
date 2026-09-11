@echo off
REM ================================================================
REM 一键注册 Windows 任务计划程序 的"每周一 10:00 刷新主力合约 Top3"任务
REM
REM 用法：
REM   1) 右键 -> 以管理员身份运行 本 bat（schtasks /Create 需要管理员权限）
REM   2) 成功后可以在 任务计划程序 -> 任务计划程序库 -> 看到：
REM         "low-low-up_周度刷新Top3主力合约"
REM   3) 想删除：管理员运行 schtasks /Delete /TN "low-low-up_周度刷新Top3主力合约" /F
REM ================================================================
setlocal EnableDelayedExpansion

REM ===== 配置区（按需修改，路径一律用绝对路径，避免计划任务相对路径漂移）=====
set "TASKNAME=low-low-up_周度刷新Top3主力合约"
set "TASK_DESC=每周一 10:00 自动刷新 data/contracts 下 main/main2/main3 主力合约"

REM 真正要执行的脚本（上一个我们刚写的 refresh_top3_main_contracts.bat 的绝对路径）
set "RUN_BAT=E:\projects-02\low-low-up\utils\refresh_top3_main_contracts.bat"

REM 运行身份：普通用户身份即可（CTP 登录凭证在当前用户下的 config 里）
set "RUNAS_USER=%USERDOMAIN%\%USERNAME%"

REM 计划时间：每周 周一 10:00:00；想调成 09:10 就改下面的 10:00
REM   语法参考：SCHTASKS /Create /SC WEEKLY /D MON /ST 10:00 ...
set "SCHEDULE_SC=WEEKLY"
set "SCHEDULE_DAY=MON"
set "SCHEDULE_TIME=10:00"

REM ===== 防御检查 =====
if not exist "%RUN_BAT%" (
    echo [ERROR] 找不到要执行的脚本：%RUN_BAT%
    echo 请检查 refresh_top3_main_contracts.bat 是否存在于 utils\ 目录下
    pause
    exit /b 1
)

REM ===== 旧任务同名先删除（避免重复注册）=====
echo [INFO] 清理同名旧任务（如果存在）：%TASKNAME%
schtasks /Delete /TN "%TASKNAME%" /F >nul 2>&1

REM ===== 创建任务 =====
echo [INFO] 正在创建计划任务："%TASKNAME%"
echo        执行用户：%RUNAS_USER%
echo        触发条件：%SCHEDULE_SC%  星期：%SCHEDULE_DAY%  时间：%SCHEDULE_TIME%
echo        执行脚本：%RUN_BAT%
echo.

schtasks /Create ^
    /SC %SCHEDULE_SC% ^
    /D  %SCHEDULE_DAY% ^
    /ST %SCHEDULE_TIME% ^
    /TN "%TASKNAME%" ^
    /TR "\"%RUN_BAT%\"" ^
    /RU "%RUNAS_USER%" ^
    /RL LIMITED ^
    /F ^
    /V1 ^
    /NP ^
    /SD "*" ^
    /ST "%SCHEDULE_TIME%" ^
    /DESCRIPTION "%TASK_DESC%"

set "EC=%errorlevel%"

REM ===== 结果反馈 =====
echo.
if %EC% equ 0 (
    echo [OK] 计划任务创建成功！
    echo.
    echo 查看任务：
    echo   schtasks /Query /TN "%TASKNAME%" /FO LIST /V
    echo 手动立刻运行一次：
    echo   schtasks /Run   /TN "%TASKNAME%"
    echo 删除任务：
    echo   schtasks /Delete /TN "%TASKNAME%" /F
) else (
    echo [ERROR] schtasks 创建失败（退出码 %EC%），常见原因：
    echo   1. 没有以 管理员身份 运行本 bat —— 右键 -> 以管理员身份运行
    echo   2. /RL 参数或用户权限问题 —— 可以把 bat 里 /RL LIMITED 改成 HIGHEST
)

pause
endlocal