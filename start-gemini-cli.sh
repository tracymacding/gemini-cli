#!/bin/bash

##
# 使用本地 CLI + DeepSeek + MCP
# 这个脚本同时支持 DeepSeek 和 MCP 工具！
##

set -e

# 颜色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🤖 启动 Gemini CLI (DeepSeek + MCP)${NC}"
echo "===================================="
echo ""

# 切换到项目目录
cd /home/disk5/dingkai/github/gemini-cli

# 加载环境变量
if [ -f .env ]; then
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ 已加载 .env 配置${NC}"
fi

# 检查 DeepSeek API Key
if [ -z "$DEEPSEEK_API_KEY" ]; then
    echo -e "${RED}❌ DEEPSEEK_API_KEY 未设置${NC}"
    echo "请编辑 .env 文件并设置 DEEPSEEK_API_KEY"
    exit 1
fi

echo -e "${GREEN}✅ DeepSeek API Key: ${DEEPSEEK_API_KEY:0:8}...${NC}"
echo ""

# 检查 API 服务器
echo -e "${BLUE}📡 检查中心 API 服务器...${NC}"
if curl -s http://localhost:3002/health > /dev/null 2>&1; then
    echo -e "${GREEN}   ✅ API 服务器运行正常${NC}"
else
    echo -e "${YELLOW}   ⚠️  API 服务器未运行${NC}"
    echo "   启动命令: cd mcp-example && ./start-api-server.sh"
    echo ""
fi

# 检查 MCP 配置
echo -e "${BLUE}🔧 检查 MCP 配置...${NC}"
MCP_STATUS=$(node ./bundle/gemini.js mcp list 2>&1 | grep "starrocks-expert" || echo "未配置")
if echo "$MCP_STATUS" | grep -q "Connected"; then
    echo -e "${GREEN}   ✅ MCP 服务器已连接${NC}"
elif echo "$MCP_STATUS" | grep -q "starrocks-expert"; then
    echo -e "${YELLOW}   ⚠️  MCP 服务器已配置但未连接${NC}"
else
    echo -e "${RED}   ❌ MCP 服务器未配置${NC}"
fi
echo ""

# 检查本地 CLI
CLI_PATH="./bundle/gemini.js"
if [ ! -f "$CLI_PATH" ]; then
    echo -e "${RED}❌ CLI 文件不存在: $CLI_PATH${NC}"
    echo "请运行: npm run build && npm run bundle"
    exit 1
fi

echo -e "${GREEN}✅ 使用本地 CLI: $CLI_PATH${NC}"
echo ""

# 启动提示
echo "===================================="
echo -e "${GREEN}🚀 启动 Gemini CLI...${NC}"
echo "===================================="
echo ""
echo "💡 使用的功能:"
echo "   • DeepSeek 模型 (deepseek-chat)"
echo "   • MCP 工具 (StarRocks 诊断)"
echo ""
echo "💡 可用命令:"
echo "   /mcp list          - 列出 MCP 服务器"
echo "   /help              - 查看帮助"
echo ""
echo "💡 快速测试:"
echo "   请帮我分析 StarRocks 的存储健康状况"
echo ""
echo "按 Ctrl+D 或输入 /exit 退出"
echo ""

# 启动本地 CLI (DeepSeek + MCP)
exec node "$CLI_PATH" --provider deepseek -m deepseek-chat "$@"
