#!/bin/bash

##
# Gemini CLI with MCP Support
# 使用全局 gemini 命令，自动加载 ~/.gemini/settings.json 中的 MCP 配置
##

set -e

# 颜色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 加载环境变量（与 start-gemini-cli.sh 保持一致）
if [ -f .env ]; then
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ 已加载 .env 配置${NC}"
fi

echo -e "${BLUE}🤖 启动 Gemini CLI (支持 MCP)${NC}"
echo "=================================="
echo ""

# 检查全局 gemini 命令
if ! command -v gemini &> /dev/null; then
    echo -e "${RED}❌ 全局 gemini 命令不存在${NC}"
    echo "   请先安装: npm install -g @google/generative-ai-cli"
    exit 1
fi

echo -e "${GREEN}✅ Gemini CLI 已安装: $(which gemini)${NC}"
echo ""

# 检查 API 服务器
echo -e "${BLUE}📡 检查中心 API 服务器...${NC}"
if curl -s http://localhost:3002/health > /dev/null 2>&1; then
    API_STATUS=$(curl -s http://localhost:3002/health | jq -r '.status')
    API_UPTIME=$(curl -s http://localhost:3002/health | jq -r '.uptime')
    echo -e "${GREEN}   ✅ API 服务器运行正常 (状态: $API_STATUS, 运行: ${API_UPTIME}秒)${NC}"

    # 检查工具数量
    TOOLS_COUNT=$(curl -s http://localhost:3002/api/tools -H "X-API-Key: demo-key" 2>/dev/null | jq -r '.tools | length' 2>/dev/null || echo "0")
    if [ "$TOOLS_COUNT" = "3" ]; then
        echo -e "${GREEN}   ✅ API 返回 3 个工具${NC}"
    else
        echo -e "${YELLOW}   ⚠️  API 返回工具数量: $TOOLS_COUNT (期望 3)${NC}"
    fi
else
    echo -e "${YELLOW}   ⚠️  API 服务器未运行${NC}"
    echo -e "${YELLOW}   💡 请先启动:${NC}"
    echo "      cd /home/disk5/dingkai/github/gemini-cli/mcp-example"
    echo "      ./start-api-server.sh"
    echo ""
    read -p "是否继续启动 Gemini? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi
echo ""

# 检查 MCP 配置
echo -e "${BLUE}🔧 检查 MCP 配置...${NC}"
if [ -f ~/.gemini/settings.json ]; then
    if jq -e '.mcpServers."starrocks-expert"' ~/.gemini/settings.json > /dev/null 2>&1; then
        echo -e "${GREEN}   ✅ MCP 服务器已配置: starrocks-expert${NC}"

        # 检查脚本文件
        MCP_SCRIPT=$(jq -r '.mcpServers."starrocks-expert".args[0]' ~/.gemini/settings.json)
        if [ -f "$MCP_SCRIPT" ]; then
            echo -e "${GREEN}   ✅ MCP 脚本存在: $MCP_SCRIPT${NC}"
        else
            echo -e "${RED}   ❌ MCP 脚本不存在: $MCP_SCRIPT${NC}"
        fi
    else
        echo -e "${YELLOW}   ⚠️  MCP 服务器未配置${NC}"
        echo "   💡 请参考: ~/.starrocks-mcp/GEMINI_CONFIG_EXAMPLE.json"
    fi
else
    echo -e "${YELLOW}   ⚠️  Gemini 配置文件不存在: ~/.gemini/settings.json${NC}"
    echo "   💡 请先配置 MCP 服务器"
fi
echo ""

# 检查 Thin MCP Server
echo -e "${BLUE}📦 检查 Thin MCP Server...${NC}"
if [ -d ~/.starrocks-mcp ]; then
    echo -e "${GREEN}   ✅ Thin MCP Server 已安装${NC}"
else
    echo -e "${YELLOW}   ⚠️  Thin MCP Server 未安装${NC}"
    echo "   💡 运行安装: cd /home/disk5/dingkai/github/gemini-cli/mcp-example && ./install-thin-mcp.sh"
fi
echo ""

# 启动提示
echo "=================================="
echo -e "${GREEN}🚀 启动 Gemini CLI...${NC}"
echo "=================================="
echo ""
echo "💡 启动后可用命令:"
echo "   /mcp-list-servers  - 列出 MCP 服务器"
echo "   /mcp-list-tools    - 列出可用工具"
echo "   /help              - 查看帮助"
echo ""
echo "💡 快速诊断:"
echo "   请帮我分析 StarRocks 的存储健康状况"
echo "   检查一下 Compaction 是否正常"
echo ""
echo "按 Ctrl+D 或输入 /exit 退出"
echo ""

# 启动全局 Gemini CLI
exec gemini "$@"
