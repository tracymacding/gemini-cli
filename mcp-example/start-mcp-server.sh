#!/bin/bash

# 加载环境变量
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 加载 .env 文件
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# 启动 MCP server
exec node "$SCRIPT_DIR/index-expert-enhanced.js"