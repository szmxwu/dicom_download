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

echo [1/3] Python 版本:
python --version
echo.

REM 检查并创建虚拟环境
if not exist "venv" (
    echo [2/3] 创建虚拟环境...
    python -m venv venv
    if errorlevel 1 (
        echo [错误] 创建虚拟环境失败
        pause
        exit /b 1
    )
)

REM 激活虚拟环境
echo [2/3] 激活虚拟环境...
call venv\Scripts\activate.bat

REM 安装依赖
echo [3/3] 检查并安装依赖...
pip install -q -r requirements.txt
if errorlevel 1 (
    echo [警告] 依赖安装可能不完整，尝试继续启动...
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

REM 退出时停用虚拟环境
call venv\Scripts\deactivate.bat

echo.
echo 服务已停止
pause
