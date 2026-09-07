$xmlPath = Join-Path $PSScriptRoot "AccountMonitor_task.xml"
$xmlContent = Get-Content $xmlPath -Raw
Register-ScheduledTask -TaskName "AccountMonitor" -Xml $xmlContent -Force
Get-ScheduledTask -TaskName "AccountMonitor" | Get-ScheduledTaskTrigger | Select-Object StartTime
