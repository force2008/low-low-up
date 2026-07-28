# 启动 strategy_signal_check.py (online) 并记录 PID
$projectRoot = "D:\projects\low-low-up"
$pythonPath = "D:\Miniconda\envs\python310\python.exe"
$scriptPath = "strategy_signal_check.py"
$pidFile = Join-Path $projectRoot "strategy_signal_check.pid"

Set-Location $projectRoot

# 如果已有 PID 文件，先尝试结束旧进程
if (Test-Path $pidFile) {
    $oldPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($oldPid) {
        try {
            Stop-Process -Id $oldPid -Force -ErrorAction Stop
            Write-Output "已结束旧进程 PID=$oldPid"
        } catch {
            Write-Output "旧进程 PID=$oldPid 不存在或无法结束"
        }
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}

# 启动新进程
$proc = Start-Process -FilePath $pythonPath `
    -ArgumentList $scriptPath, "online" `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -PassThru

$proc.Id | Out-File -FilePath $pidFile -Encoding ascii
Write-Output "strategy_signal_check.py online 已启动，PID=$($proc.Id)"
