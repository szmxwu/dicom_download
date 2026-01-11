@echo off
echo Starting DICOM Download Server...
echo.
REM 检查是否有 Python 环境
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.8 or 3.9.
    pause
    exit /b
)

REM 检查并安装依赖
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
)

echo Activating virtual environment...
call venv\Scripts\activate

echo Installing/Updating requirements...
pip install -r requirements.txt

echo.
echo Server is starting at http://127.0.0.1:5005
python app.py
pause
