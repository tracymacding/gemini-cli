# StarRocks Expert - Thin MCP Client

这是 StarRocks Expert 系统的**客户端组件**（Thin MCP Server），用于将 StarRocks 数据库连接到支持 MCP 协议的 AI 客户端。

## 📋 项目说明

本目录仅包含**客户端代码**，服务端代码已迁移到独立项目：

- **服务端（Central API）**: [operation-experts/starrocks-expert](https://github.com/tracymacding/operation-experts/tree/main/starrocks-expert)
- **客户端（本目录）**: Thin MCP Server

## 🎯 客户端功能

- 实现 MCP (Model Context Protocol) Stdio Server
- 连接本地 StarRocks 数据库执行 SQL
- 连接远程中心 API 获取诊断分析
- 支持 Prometheus 监控指标查询
- 适配各种 AI 客户端（Gemini CLI、Claude Desktop 等）

## 📦 文件说明

```
starrocks-mcp-server/
├── starrocks-mcp.js           # StarRocks MCP Server
├── install-starrocks-mcp.sh   # 安装脚本
├── .env.example               # 环境变量模板
├── package.json               # 依赖配置
├── QUICK_START.md             # 快速开始指南
└── archive-central-server/    # 归档：旧的中心服务代码（已废弃）
```

## 🚀 快速开始

### 1. 安装客户端

```bash
cd /path/to/gemini-cli/starrocks-mcp-server
./install-starrocks-mcp.sh
```

这会将 Thin MCP Server 安装到 `~/.starrocks-mcp/`

### 2. 配置环境变量

编辑 `~/.starrocks-mcp/.env`：

```bash
# 远程中心 API
CENTRAL_API_URL=http://your-server-ip:3002
CENTRAL_API_KEY=your-api-key

# 本地 StarRocks 数据库
SR_HOST=localhost
SR_PORT=9030
SR_USER=root
SR_PASSWORD=
SR_DATABASE=information_schema

# Prometheus（可选）
PROMETHEUS_HOST=localhost
PROMETHEUS_PORT=9090
```

### 3. 配置 AI 客户端

**Gemini CLI**：

编辑 `~/.gemini/settings.json`：

```json
{
  "mcpServers": {
    "starrocks-expert": {
      "command": "node",
      "args": ["/home/your-user/.starrocks-mcp/starrocks-mcp.js"],
      "env": {}
    }
  }
}
```

**Claude Desktop**：

编辑 Claude 配置文件，添加相同的 MCP 服务器配置。

### 4. 启动使用

```bash
# 使用 Gemini CLI
gemini --provider deepseek -m deepseek-chat

# 在对话中
> /mcp list
> 分析 StarRocks 存储健康状况
```

## 🔧 测试

```bash
# 测试安装
node ~/.starrocks-mcp/starrocks-mcp.js --version
```

## 📚 完整文档

- [快速开始](QUICK_START.md)
- [服务端部署](https://github.com/tracymacding/operation-experts/tree/main/starrocks-expert)
- [架构文档](archive-central-server/docs/ARCHITECTURE.md)（归档）

## 🗑️ 归档说明

`archive-central-server/` 目录包含旧的中心服务代码，已迁移到独立项目，仅供参考。

在确认新项目运行稳定后，可以删除该目录。

## 🔄 迁移历史

- **2025-10-13**: 中心服务代码迁移到 `operation-experts/starrocks-expert`
- **归档位置**: `archive-central-server/`

---

**维护**: StarRocks Team
**许可**: Apache-2.0
