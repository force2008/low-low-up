$action = New-ScheduledTaskAction -Execute "D:\projects\low-low-up\order-check\start_monitor.bat"
$trigger1 = New-ScheduledTaskTrigger -Daily -At "09:00"
$trigger2 = New-ScheduledTaskTrigger -Daily -At "13:00"
$trigger3 = New-ScheduledTaskTrigger -Daily -At "21:00"
Register-ScheduledTask -TaskName "AccountMonitor" -Action $action -Trigger $trigger1,$trigger2,$trigger3 -Force
