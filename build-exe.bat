@echo off

rem Charles Hartsell, 2024/06/11, script to build parser

echo.
echo.

rem set the name of the file to build
set BUILD_FILE=".\aoi-log-parser.py"

echo ----- program start -----
echo building %BUILD_FILE%...

rem run pyinstaller
pyinstaller --onefile %BUILD_FILE%

echo.
echo finished building! check the "/dist" file for your .exe
echo ----- program end -----
echo.
echo.

pause