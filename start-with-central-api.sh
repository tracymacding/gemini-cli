#!/bin/bash

##
# 启动 Gemini CLI + Solution C 架构
# - 中心 API 服务器（包含所有 33 个工具）
# - Thin MCP Server（本地执行 SQL）
# - Gemini CLI + DeepSeek
##

set -e

# 颜色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🚀 启动 Solution C 架构${NC}"
echo "==========================================="
echo ""
echo "架构："
echo "  Gemini CLI"
echo "     ↓ (Stdio)"
echo "  Thin MCP Server (本地)"
echo "     ↓ (HTTP)"
echo "  中心 API 服务器（所有 Expert）"
echo ""
echo "==========================================="
echo ""

# 切换到项目目录
cd /home/disk5/dingkai/github/gemini-cli

# 检查中心 API 服务器是否运行
echo -e "${BLUE}📡 检查中心 API 服务器...${NC}"
if curl -s http://localhost:3002/health -H "X-API-Key: demo-key" > /dev/null 2>&1; then
    TOOLS_COUNT=$(curl -s http://localhost:3002/health -H "X-API-Key: demo-key" | python3 -c "import sys, json; print(json.load(sys.stdin)['tools'])" 2>/dev/null || echo "unknown")
    echo -e "${GREEN}   ✅ 中心 API 服务器运行正常${NC}"
    echo -e "   📦 工具数量: ${TOOLS_COUNT}"
else
    echo -e "${RED}   ❌ 中心 API 服务器未运行${NC}"
    echo ""
    echo -e "${YELLOW}请先启动中心 API 服务器：${NC}"
    echo "   cd /home/disk5/dingkai/github/gemini-cli/mcp-example"
    echo "   export API_PORT=3002 API_KEY=demo-key SR_HOST=localhost SR_USER=root SR_PASSWORD=\"\""
    echo "   node index-expert-api-complete.js"
    echo ""
    exit 1
fi

# 检查 DeepSeek API Key
if [ -f .env ]; then
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ 已加载 .env 配置${NC}"
fi

if [ -z "$DEEPSEEK_API_KEY" ]; then
    echo -e "${RED}❌ DEEPSEEK_API_KEY 未设置${NC}"
    echo "请编辑 .env 文件并设置 DEEPSEEK_API_KEY"
    exit 1
fi

echo -e "${GREEN}✅ DeepSeek API Key: ${DEEPSEEK_API_KEY:0:8}...${NC}"
echo ""

# 检查 MCP 配置
echo -e "${BLUE}🔧 检查 MCP 配置...${NC}"
MCP_STATUS=$(node ./bundle/gemini.js mcp list 2>&1 | grep "starrocks-expert" || echo "未配置")
if echo "$MCP_STATUS" | grep -q "Connected"; then
    echo -e "${GREEN}   ✅ Thin MCP Server 已配置${NC}"
else
    echo -e "${YELLOW}   ⚠️  MCP 配置可能有问题${NC}"
fi
echo ""

# 启动提示
echo "==========================================="
echo -e "${GREEN}🚀 启动 Gemini CLI...${NC}"
echo "==========================================="
echo ""
echo "💡 Solution C 架构说明:"
echo "   1. 中心 API 服务器: http://localhost:3002 (所有 33 个工具)"
echo "   2. Thin MCP Server: 本地 (协调 SQL 执行)"
echo "   3. Gemini CLI: 交互界面"
echo ""
echo "💡 可用功能:"
echo "   • 所有 11 个 Expert"
echo "   • 所有 33 个工具 (包括 cache-expert)"
echo "   • DeepSeek AI 模型"
echo ""
echo "💡 示例命令:"
echo "   /mcp list              - 列出所有工具"
echo "   请分析缓存性能          - cache-expert"
echo "   检查 compaction 状态    - compaction-expert"
echo "   做全面系统健康检查      - 多专家协调"
echo ""
echo "按 Ctrl+D 或输入 /exit 退出"
echo ""

# 启动 Gemini CLI
exec node ./bundle/gemini.js --provider deepseek -m deepseek-chat "$@"
