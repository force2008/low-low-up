$xmlPath = Join-Path $PSScriptRoot "AccountMonitor_task.xml"
$xmlContent = Get-Content $xmlPath -Raw
Register-ScheduledTask -TaskName "AccountMonitor" -Xml $xmlContent -Force
# 回显校验：schtasks 不依赖 ScheduledTasks 模块，兼容老版本 PowerShell
schtasks /query /tn "AccountMonitor" /v /fo list
