@echo off
rem diAgnostic-framework - Desktop-Start (Core + natives Fenster)
set "VENV=%~dp0backend\.venv\Scripts\python.exe"
if not exist "%VENV%" (
  echo Backend-venv fehlt. Bitte zuerst ausfuehren:
  echo   py -3.14 -m venv backend\.venv
  echo   backend\.venv\Scripts\python.exe -m pip install -e "backend[dev,desktop]"
  echo   scripts\build.ps1  ^(Frontend bauen^)
  pause
  exit /b 1
)
"%VENV%" "%~dp0shell\run_desktop.py"
if errorlevel 1 pause
