@echo off
REM ================================================================
REM 每周一定时执行：刷新 主力合约 Top3（main / main2 / main3）
REM 内部实际调用：
REM   python utils\GetMainContractWithVolume.py online ^
REM       --refresh-instruments --md-wait 120 --top-n 3
REM ================================================================
setlocal

REM 1) 强制切盘 + 切工作目录（计划任务默认在 system32 下，必须 cd /d）
cd /d D:\projects\low-low-up

REM 2) 激活 conda 环境 python310（call 等它完成，不新开子shell）
call conda activate python310
if errorlevel 1 (
    echo [ERROR] conda activate python310 失败，请检查 conda 环境名是否正确
    exit /b 1
)

REM 3) 打印开始时间，便于日志
echo ----------------------------------------------------------------
echo [%date% %time%] 开始刷新主力合约 Top3 ...
echo ----------------------------------------------------------------

REM 4) 执行主力合约脚本
REM    online            = 实盘/仿真在线环境（优先接交易时段真实行情）
REM    --refresh-instruments = 强制重新 ReqQryInstrument，更新新上市/下市合约
REM    --md-wait 120     = 订阅行情后等120秒收推送（7x24/非活跃时段也能收得比较全）
REM    --top-n 3         = 输出 main/main2/main3 三级主力
python utils\GetMainContractWithVolume.py online --refresh-instruments --md-wait 120 --top-n 3
set EXITCODE=%errorlevel%

echo.
echo ----------------------------------------------------------------
echo [%date% %time%] 执行完成，退出码=%EXITCODE%
echo ----------------------------------------------------------------

REM 5) 把退出码传给计划任务，成功=0 失败=非0（计划任务里可以据此重试）
exit /b %EXITCODE%