@echo off
cd /d "%~dp0"
python app_gui.py
echo.
echo Program finished or crashed. If there was no window, check config_ui.json and try again.
pause