@echo off
:: setup_backup.bat
:: ================
:: Registers a Windows Task Scheduler task to run backup_db.py every 5 days.
:: Run this ONCE as Administrator.
::
:: What it does:
::   - Creates task "CRM_DB_Backup"
::   - Runs at 03:00 AM every 5 days
::   - Uses the Python from the crm_etl venv
::   - Keeps last 7 backups (configured in backup_db.py --keep)
::
:: To run manually at any time:
::   schtasks /run /tn "CRM_DB_Backup"
::
:: To delete the task:
::   schtasks /delete /tn "CRM_DB_Backup" /f

SET PROJECT_ROOT=D:\Projects\SQL Migration
SET PYTHON=D:\Projects\SQL Migration\.venv\Scripts\python.exe
SET SCRIPT=%PROJECT_ROOT%\scripts\backup_db.py
SET TASK_NAME=CRM_DB_Backup

echo.
echo  Registering backup task: %TASK_NAME%
echo  Python:  %PYTHON%
echo  Script:  %SCRIPT%
echo  Schedule: 03:00 AM every 5 days
echo.

:: Delete existing task if it exists (ignore error if not found)
schtasks /delete /tn "%TASK_NAME%" /f 2>nul

:: Create the task
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\" --keep 7" ^
  /sc daily ^
  /mo 5 ^
  /st 03:00 ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /f

IF %ERRORLEVEL% EQU 0 (
    echo  Task "%TASK_NAME%" registered successfully.
    echo.
    echo  To verify: schtasks /query /tn "%TASK_NAME%" /fo LIST
    echo  To run now: schtasks /run /tn "%TASK_NAME%"
) ELSE (
    echo  ERROR: Task registration failed. Make sure you ran this as Administrator.
)

pause
