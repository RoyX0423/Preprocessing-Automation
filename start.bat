@echo off
REM preprocessing-automation 一键启动 (Windows)
chcp 65001 >nul
cd /d "%~dp0"
where python >nul 2>nul
if errorlevel 1 (
    echo [错误] 未找到 python, 请安装 Python 3.10+ 并勾选 "Add Python to PATH"
    pause
    exit /b 1
)
python -c "import flask, PIL, numpy" >nul 2>nul
if errorlevel 1 (
    echo [提示] 缺少依赖 flask / pillow / numpy, 正在安装...
    python -m pip install flask pillow numpy
)
echo 服务启动: http://127.0.0.1:8050
python app.py
