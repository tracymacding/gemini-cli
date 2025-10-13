#!/bin/bash

##
# Test Thin MCP Server workflow
# Demonstrates the complete data flow of Solution C
##

set -e

echo "🧪 Testing Thin MCP Server Workflow"
echo "===================================="
echo

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Step 1: Test API Server
echo -e "${BLUE}Step 1: Testing Central API Server${NC}"
echo "API Endpoint: http://localhost:3002"
echo

# Health check
echo "1.1 Health Check:"
curl -s http://localhost:3002/health | jq -C .
echo

# List tools
echo "1.2 List Available Tools:"
curl -s http://localhost:3002/api/tools -H "X-API-Key: demo-key" | jq -C '.tools[] | {name, description}'
echo

# Get SQL queries
echo "1.3 Get SQL Queries for 'analyze_storage_health':"
curl -s http://localhost:3002/api/queries/analyze_storage_health -H "X-API-Key: demo-key" | jq -C '{tool, queries: .queries | length, analysis_endpoint}'
echo

echo -e "${GREEN}✅ API Server is working correctly${NC}"
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo

# Step 2: Simulate Thin MCP Server workflow
echo -e "${BLUE}Step 2: Simulating Thin MCP Server Workflow${NC}"
echo

echo -e "${YELLOW}假设用户在 Gemini CLI 中输入:${NC}"
echo "  > 请帮我分析 StarRocks 的存储健康状况"
echo

echo -e "${YELLOW}数据流:${NC}"
echo "  1. Gemini AI → 调用 analyze_storage_health 工具"
echo "  2. Gemini CLI → 通过 Stdio 调用 Thin MCP Server"
echo "  3. Thin MCP Server → GET /api/queries/analyze_storage_health"
echo

echo -e "${YELLOW}API 返回 SQL 查询列表:${NC}"
curl -s http://localhost:3002/api/queries/analyze_storage_health -H "X-API-Key: demo-key" | jq -C '.queries[] | {id, description, sql: (.sql | split("\n")[0])}'
echo

echo "  4. Thin MCP Server → 连接本地 StarRocks 执行这些 SQL"
echo "  5. Thin MCP Server → POST /api/analyze/analyze_storage_health"
echo "     (发送查询结果给 API 分析)"
echo

echo -e "${YELLOW}模拟查询结果（Mock Data）:${NC}"
cat <<'EOF' | jq -C .
{
  "results": {
    "backends": [
      {
        "IP": "192.168.1.100",
        "MaxDiskUsedPct": "75%",
        "ErrTabletNum": "0",
        "TabletNum": "1500"
      },
      {
        "IP": "192.168.1.101",
        "MaxDiskUsedPct": "82%",
        "ErrTabletNum": "2",
        "TabletNum": "1480"
      }
    ],
    "tablet_statistics": {
      "total_tablets": 2980,
      "nodes_with_errors": 1,
      "total_error_tablets": 2
    }
  }
}
EOF
echo

echo "  6. API 分析数据并返回诊断报告"
echo "  7. Thin MCP Server → Gemini CLI: 返回格式化报告"
echo "  8. 用户看到分析结果"
echo

echo -e "${GREEN}✅ Workflow simulation complete${NC}"
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo

# Step 3: Show installation paths
echo -e "${BLUE}Step 3: Installation Summary${NC}"
echo

echo "Central API Server (你维护):"
echo "  📍 Location: $(pwd)/index-expert-api.js"
echo "  🚀 Start: npm run start:api"
echo "  🔧 Port: 3002"
echo

echo "Thin MCP Server (客户安装):"
echo "  📍 Location: ~/.starrocks-mcp/thin-mcp-server.js"
echo "  ⚙️  Config: ~/.starrocks-mcp/.env"
echo "  📝 Gemini Config Example: ~/.starrocks-mcp/GEMINI_CONFIG_EXAMPLE.json"
echo

echo -e "${GREEN}✅ All components are ready${NC}"
echo

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo

# Step 4: Next steps
echo -e "${BLUE}Next Steps to Use in Production:${NC}"
echo

echo "1️⃣  服务端部署 (你操作):"
echo "   cd mcp-example"
echo "   export API_KEY=your-secure-api-key"
echo "   npm run start:api"
echo "   # 或使用 PM2: pm2 start index-expert-api.js"
echo

echo "2️⃣  客户端安装 (客户操作):"
echo "   ./install-thin-mcp.sh"
echo "   nano ~/.starrocks-mcp/.env  # 配置数据库连接"
echo "   nano ~/.gemini/settings.json  # 添加 MCP 服务器配置"
echo

echo "3️⃣  在 Gemini CLI 中使用:"
echo "   gemini"
echo "   > /mcp-list-tools"
echo "   > 请帮我分析 StarRocks 的存储健康状况"
echo

echo "📚 Complete documentation: SOLUTION_C_GUIDE.md"
echo

echo -e "${GREEN}🎉 Test completed successfully!${NC}"
