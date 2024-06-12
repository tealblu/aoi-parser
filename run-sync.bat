@echo off

rem Notify that program is running
echo Looking for file-sync.py...

rem Set the path to the Python script
set PYTHON_SCRIPT_PATH=".\file-sync.py"

rem Check if the Python script exists
if not exist "%PYTHON_SCRIPT_PATH%" (
    echo Python script not found at "%PYTHON_SCRIPT_PATH%"
    pause
    exit /b
)

rem Run the Python script
python "%PYTHON_SCRIPT_PATH%"

echo Done!