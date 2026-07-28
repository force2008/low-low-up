# 停止 strategy_signal_check.py
$projectRoot = "D:\projects\low-low-up"
$pidFile = Join-Path $projectRoot "strategy_signal_check.pid"

if (Test-Path $pidFile) {
    $targetPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($targetPid) {
        try {
            Stop-Process -Id $targetPid -Force -ErrorAction Stop
            Write-Output "已结束 strategy_signal_check.py 进程 PID=$targetPid"
        } catch {
            Write-Output "进程 PID=$targetPid 不存在或无法结束"
        }
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
} else {
    Write-Output "PID 文件不存在，strategy_signal_check.py 可能未运行"
}
