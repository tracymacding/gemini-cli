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
- 适配各种 AI 客户端（Gemini CLI、Claude Desktop 等）

## 📦 文件说明

```
starrocks-mcp-server/
├── starrocks-mcp.js           # StarRocks MCP Server
├── install-starrocks-mcp.sh   # 安装脚本
├── package.json               # 依赖配置
├── QUICK_START.md             # 快速开始指南
└── archive-central-server/    # 归档：旧的中心服务代码（已废弃）
```

## 🚀 完整配置指南

**配置前准备：**

在开始配置前，请确保你有以下信息：

- ✅ StarRocks 数据库连接信息（主机、端口、用户名、密码）
- ✅ 中心 API 服务器地址和 Token
- ✅ **DeepSeek API Key**（启动脚本默认使用 DeepSeek，必须配置）
- ✅ Node.js >= 18（用于运行 MCP 服务器）

### 第一步：安装 MCP 客户端

运行安装脚本，将 Thin MCP Server 安装到 `~/.starrocks-mcp/`：

```bash
cd /path/to/gemini-cli/starrocks-mcp-server
./install-starrocks-mcp.sh
```

安装脚本会：

- ✅ 检查 Node.js 版本（需要 >= 18）
- ✅ 复制 `starrocks-mcp.js` 到 `~/.starrocks-mcp/`
- ✅ 安装依赖包（`@modelcontextprotocol/sdk`, `mysql2`, `dotenv`）
- ✅ 创建配置文件模板

### 第二步：配置数据库和 API 连接

编辑 `~/.starrocks-mcp/.env`，修改为你的实际配置：

```bash
# StarRocks 数据库配置（本地数据库）
SR_HOST=localhost          # StarRocks FE 地址
SR_USER=root               # 数据库用户名
SR_PASSWORD=               # 数据库密码（如果有）
SR_PORT=9030               # StarRocks 查询端口

# 中心 API 配置
CENTRAL_API=http://your-central-api-server         # 中心 API 地址
CENTRAL_API_TOKEN=demo-key                         # API 认证 Token
```

**重要说明：**

- `SR_HOST`: 填写你本地 StarRocks FE 的 IP 地址
- `SR_PASSWORD`: 如果数据库有密码，必须填写
- `CENTRAL_API`: 使用你的中心 API 服务器地址
- `CENTRAL_API_TOKEN`: 如果 API 需要认证，填写对应的 token

### 第三步：配置 Gemini CLI

编辑 `~/.gemini/settings.json`，添加 MCP 服务器配置：

```json
{
  "mcpServers": {
    "starrocks-expert": {
      "command": "node",
      "args": ["/home/your-username/.starrocks-mcp/starrocks-mcp.js"],
      "env": {
        "SR_HOST": "localhost",
        "SR_USER": "root",
        "SR_PASSWORD": "",
        "SR_PORT": "9030",
        "CENTRAL_API": "http://your-central-api-server",
        "CENTRAL_API_TOKEN": "demo-key"
      }
    }
  }
}
```

**配置说明：**

- `command`: 固定为 `"node"`
- `args`: 修改路径中的 `your-username` 为你的实际用户名
- `env`: 环境变量配置，需要与 `.env` 文件中的配置保持一致

### 第四步：配置 DeepSeek API Key（必需）

**重要：** `start-gemini-cli.sh` 启动脚本默认使用 DeepSeek 模型，因此必须配置 DeepSeek API Key。

在 gemini-cli 项目根目录创建 `.env` 文件：

```bash
cd /path/to/gemini-cli
cat > .env <<EOF
DEEPSEEK_API_KEY=your-deepseek-api-key
EOF
```

**获取 DeepSeek API Key**：

1. 访问 https://platform.deepseek.com/
2. 注册账号并登录
3. 在 API Keys 页面创建新的 API Key
4. 复制 API Key 并粘贴到 `.env` 文件中

**为什么使用 DeepSeek？**

- 成本更低（相比 Gemini）
- 性能优秀，适合诊断分析任务
- 支持 MCP 工具调用

## 🎮 启动和使用

**注意：** `start-gemini-cli.sh` 启动脚本默认使用 **DeepSeek** 模型，请确保已配置 `DEEPSEEK_API_KEY`（参见第四步）。

### 方式一：使用启动脚本（推荐）

在 gemini-cli 项目根目录运行：

```bash
cd /path/to/gemini-cli
./start-gemini-cli.sh
```

启动脚本会自动：

- ✅ 检查 DeepSeek API Key
- ✅ 检查中心 API 服务器状态
- ✅ 检查 MCP 服务器连接状态
- ✅ 启动 Gemini CLI 并加载 StarRocks 专家工具

**启动成功后，你会看到：**

```
🤖 启动 Gemini CLI (DeepSeek + MCP)
====================================

✅ 已加载 .env 配置
✅ DeepSeek API Key: sk-xxxxx...

📡 检查中心 API 服务器...
   ✅ API 服务器运行正常

🔧 检查 MCP 配置...
   ✅ MCP 服务器已连接

✅ 使用本地 CLI: ./bundle/gemini.js

====================================
🚀 启动 Gemini CLI...
====================================

💡 使用的功能:
   • DeepSeek 模型 (deepseek-chat)
   • MCP 工具 (StarRocks 诊断)

💡 可用命令:
   /mcp list          - 列出 MCP 服务器
   /help              - 查看帮助

💡 快速测试:
   请帮我分析 StarRocks 的存储健康状况
```

### 方式二：直接使用 Gemini CLI

```bash
# 使用 DeepSeek 模型
gemini --provider deepseek -m deepseek-chat

# 或使用 Gemini 模型
gemini -m gemini-2.0-flash-exp
```

## 💬 在对话中使用 StarRocks 专家

启动后，你可以直接向 AI 提问：

```
用户: /mcp list
AI: 显示已连接的 MCP 服务器和可用工具列表

用户: 请帮我分析 StarRocks 的存储健康状况
AI: 调用 analyze_storage_amplification 工具，分析存储放大率

用户: 检查一下有哪些 compaction 任务正在运行
AI: 调用 get_running_compaction_tasks 工具，查询 compaction 状态

用户: 查看最近的数据导入任务
AI: 调用 check_load_job_status 工具，检查导入任务状态
```

## 🔍 可用的诊断工具

MCP 服务器提供 33 个专业诊断工具，覆盖：

- **存储分析**: 存储放大率、分区存储、表统计
- **Compaction**: 压缩任务监控、性能调优
- **数据导入**: 导入任务状态、频率分析
- **查询性能**: 慢查询分析、执行计划
- **内存管理**: 内存使用、缓存统计
- **系统运维**: 配置检查、集群状态

完整工具列表：使用 `/mcp list` 命令查看

## 🔧 测试和验证

### 测试 MCP 服务器连接

```bash
# 方式一：通过 Gemini CLI
node ./bundle/gemini.js mcp list

# 方式二：直接测试 MCP 服务器
cd ~/.starrocks-mcp
node starrocks-mcp.js
# 按 Ctrl+C 退出
```

### 测试中心 API 连接

```bash
# 测试 API 健康状态
curl -H "X-API-Key: demo-key" http://your-central-api-server/health

# 查看可用工具列表
curl -H "X-API-Key: demo-key" http://your-central-api-server/api/tools | jq
```

### 测试数据库连接

```bash
mysql -h localhost -P 9030 -u root -p -e "SELECT 1"
```

## ❓ 常见问题

### 1. MCP 服务器显示 "Disconnected"

**可能原因：**

- 中心 API 服务器未运行
- 数据库连接失败
- 配置文件路径错误

**解决方法：**

```bash
# 检查中心 API
curl -H "X-API-Key: demo-key" http://your-central-api-server/health

# 检查配置文件
cat ~/.gemini/settings.json | jq
cat ~/.starrocks-mcp/.env

# 重新安装 MCP 客户端
cd starrocks-mcp-server
./install-starrocks-mcp.sh
```

### 2. DeepSeek API Key 未设置

**错误信息：**

```
❌ DEEPSEEK_API_KEY 未设置
```

**解决方法：**

```bash
# 创建 .env 文件
echo "DEEPSEEK_API_KEY=your-api-key" > .env
```

### 3. 数据库连接失败

**可能原因：**

- StarRocks 服务未启动
- 端口配置错误
- 用户名密码错误

**解决方法：**

```bash
# 检查 StarRocks 是否运行
netstat -tuln | grep 9030

# 测试连接
mysql -h localhost -P 9030 -u root -p

# 检查配置
cat ~/.starrocks-mcp/.env
```

## 📚 完整文档

- [快速开始](QUICK_START.md)
- [服务端部署](https://github.com/tracymacding/operation-experts/tree/main/starrocks-expert)
- [架构文档](archive-central-server/docs/ARCHITECTURE.md)（归档）

## 🗑️ 归档说明

`archive-central-server/` 目录包含旧的中心服务代码，已迁移到独立项目，仅供参考。

在确认新项目运行稳定后，可以删除该目录。

## 🔄 更新历史

- **2025-10-13**: 移除 `SR_DATABASE` 配置项（SQL 使用完整数据库限定）
- **2025-10-13**: 中心服务代码迁移到 `operation-experts/starrocks-expert`
- **2025-10-13**: 重命名 `install-thin-mcp.sh` → `install-starrocks-mcp.sh`

---

**维护**: StarRocks Team
**许可**: Apache-2.0
