#!/bin/bash

# Gemini CLI MCP 命令包装器 - 去除 ANSI 颜色代码
# 解决在某些环境下出现 \u001b 乱码的问题

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 运行 Gemini CLI MCP 命令并去除 ANSI 转义序列
"$SCRIPT_DIR/bundle/gemini.js" mcp "$@" 2>&1 | sed 's/\x1b\[[0-9;]*m//g'