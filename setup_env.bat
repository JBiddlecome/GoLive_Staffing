@echo off
echo ==========================================
echo GoLive Staffing - Environment Setup
echo ==========================================
echo This script will install Python 3.12, create a virtual environment, and install dependencies.
echo You may be prompted by Windows User Account Control (UAC) to allow the installation. Please click 'Yes'.
echo.
pause

echo [1/3] Installing Python 3.12...
winget install --id Python.Python.3.12 -e --accept-package-agreements --accept-source-agreements

echo.
echo [2/3] Creating virtual environment (.venv)...
py -3.12 -m venv .venv
if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment creation failed. Make sure Python 3.12 is installed correctly.
    pause
    exit /b
)

echo.
echo [3/3] Installing dependencies...
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo ==========================================
echo Setup Complete!
echo You can now use run_golive.bat to start the application.
echo ==========================================
pause
