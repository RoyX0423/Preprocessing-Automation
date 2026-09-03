#!/bin/bash
# preprocessing-automation 一键启动 (macOS / Linux)
# 依赖: python3 + flask pillow numpy requests + ffmpeg
cd "$(dirname "$0")"

# 依次探测可用解释器
PY=""
for py in python3 python; do
    if command -v "$py" >/dev/null 2>&1; then
        PY="$py"
        break
    fi
done
if [ -z "$PY" ]; then
    echo "[错误] 未找到 python3, 请先安装 Python 3.10+ (https://www.python.org/downloads/)"
    exit 1
fi

# 缺依赖则提示安装, 不自动装以避免污染用户环境
if ! "$PY" -c "import flask, PIL, numpy, requests" >/dev/null 2>&1; then
    echo "[提示] 缺少依赖, 请先执行:"
    echo "  $PY -m pip install flask pillow numpy requests"
    exit 1
fi

echo "服务启动: http://127.0.0.1:8050"
exec "$PY" app.py
