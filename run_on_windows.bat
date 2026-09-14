@echo off
chcp 65001 >nul
title DICOM Web 服务
echo ==========================================
echo    DICOM 下载与处理 Web 服务
echo ==========================================
echo.

REM 切换到脚本所在目录
cd /d "%~dp0"

REM 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.8 或更高版本
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/2] Python 版本及路径:
python --version
python -c "import sys; print(sys.executable)"
echo.

REM 使用用户已安装依赖的当前环境；启动时不创建环境、不安装依赖
echo [2/2] 检查当前环境依赖...
python -m pip check
if errorlevel 1 (
    echo [错误] 当前环境依赖不完整，请激活安装离线包的环境后重试。
    pause
    exit /b 1
)
python -c "import src; import flask, flask_socketio, dotenv, openpyxl, PIL"
if errorlevel 1 (
    echo [错误] 应用依赖无法加载，请在当前 Python 环境完成离线安装后重试。
    pause
    exit /b 1
)

echo.
echo ==========================================
echo    启动 Web 服务...
echo    访问地址: http://localhost:5005
echo    按 Ctrl+C 停止服务
echo ==========================================
echo.

REM 启动应用（使用 -m 方式确保模块路径正确）
python -m src.web.app

set "dicom_exit_code=%errorlevel%"

echo.
echo 服务已停止
pause
exit /b %dicom_exit_code%
