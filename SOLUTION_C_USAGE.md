# Solution C 架构使用指南

## ✅ 架构说明

你现在使用的是 **Solution C** 架构（混合版）：

```
Gemini CLI (交互界面)
   ↓ Stdio
Thin MCP Server (本地 - 协调者)
   ↓ HTTP: GET /api/queries/:tool
   ↓ HTTP: POST /api/analyze/:tool
中心 API 服务器 (远程 - 执行所有 Expert)
   ├─ 所有 11 个 Expert
   ├─ 所有 33 个工具
   └─ 连接 StarRocks 数据库
```

### 数据流程

1. **用户请求**：在 Gemini CLI 中输入 "请分析缓存性能"
2. **Thin MCP Server**：接收请求，调用 `analyze_cache_performance` 工具
3. **请求 API**：`GET http://localhost:3002/api/queries/analyze_cache_performance`
4. **中心 API**：返回工具信息（占位 SQL）
5. **执行工具**：`POST http://localhost:3002/api/analyze/analyze_cache_performance`
6. **中心 API**：在服务器端连接数据库，调用 cache-expert，执行分析
7. **返回结果**：分析报告 → Thin MCP Server → Gemini CLI → 用户

### 关键特点

✅ **所有 33 个工具可用**（包括 cache-expert 的 3 个工具）
✅ **零维护升级**（只需更新中心 API 服务器）
✅ **数据库密码可以在服务器端**（更安全）
✅ **客户端极简**（thin-mcp-server.js 只有 ~330 行）

## 🚀 如何启动

### 方式 1: 一键启动（推荐）

#### Step 1: 启动中心 API 服务器

在**第一个终端**:

```bash
cd /home/disk5/dingkai/github/gemini-cli/mcp-example

# 启动中心 API（包含所有 33 个工具）
export API_PORT=3002
export API_KEY=demo-key
export SR_HOST=localhost
export SR_USER=root
export SR_PASSWORD=""

node index-expert-api-complete.js
```

你会看到：

```
🚀 StarRocks Central API Server (Complete)
================================================

   📡 API endpoint:     http://localhost:3002
   ❤️  Health check:    http://localhost:3002/health
   🔧 List tools:       http://localhost:3002/api/tools

   🔑 Authentication:   Enabled
   📦 Tools loaded:     33
   🧠 Experts loaded:   10

   架构模式: 服务器端执行 + Thin MCP Client
```

#### Step 2: 启动 Gemini CLI

在**第二个终端**:

```bash
cd /home/disk5/dingkai/github/gemini-cli

# 使用便捷脚本
./start-with-central-api.sh
```

### 方式 2: 手动启动

```bash
cd /home/disk5/dingkai/github/gemini-cli

# 加载环境变量
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# 启动 CLI
node ./bundle/gemini.js --provider deepseek -m deepseek-chat
```

## 🧪 测试功能

### 1. 验证 MCP 连接

```bash
> /mcp list
```

应该看到:
```
✓ starrocks-expert: node .../thin-mcp-server.js (stdio) - Connected
```

### 2. 测试 Storage Expert

```bash
> 请分析存储放大情况
```

Thin MCP Server 输出:
```
🔧 Executing tool: analyze_storage_amplification
   Step 1: Fetching SQL queries from Central API...
   Got 1 queries to execute
   Step 2: Executing SQL queries locally...
   Step 3: Sending results to Central API for analysis...
   Analysis completed
```

### 3. 测试 Cache Expert（你要的！）

```bash
> 请分析缓存性能
```

工具: `analyze_cache_performance`

```bash
> 检查缓存抖动
```

工具: `analyze_cache_jitter`

```bash
> 分析元数据缓存
```

工具: `analyze_metadata_cache`

### 4. 测试 Compaction Expert

```bash
> 查看高 compaction score 的分区
```

工具: `get_high_compaction_partitions`

### 5. 测试多专家协调

```bash
> 做一个全面的系统健康检查
```

工具: `expert_analysis` (coordinator)

## 📊 所有可用工具（33 个）

### Storage Expert (1 个)
- `analyze_storage_amplification`

### Compaction Expert (7 个)
- `get_table_partitions_compaction_score`
- `get_high_compaction_partitions`
- `get_compaction_threads`
- `set_compaction_threads`
- `get_running_compaction_tasks`
- `analyze_high_compaction_score`
- `analyze_slow_compaction_tasks`

### Ingestion Expert (6 个)
- `check_load_job_status`
- `analyze_table_import_frequency`
- `check_stream_load_tasks`
- `check_routine_load_config`
- `analyze_reached_timeout`
- `analyze_load_channel_profile`

### Cache Expert (3 个) ⭐
- **`analyze_cache_performance`** - 分析缓存性能
- **`analyze_cache_jitter`** - 分析缓存抖动
- **`analyze_metadata_cache`** - 分析元数据缓存

### Transaction Expert (1 个)
- `analyze_transactions`

### Log Expert (1 个)
- `analyze_logs`

### Memory Expert (1 个)
- `analyze_memory`

### Query Performance Expert (3 个)
- `get_recent_slow_queries`
- `analyze_query_latency`
- `get_query_profile`

### Operate Expert (4 个)
- `install_audit_log`
- `check_audit_log_status`
- `uninstall_audit_log`
- `set_compact_threads`

### Table Schema Expert (1 个)
- `analyze_table_schema`

### Coordinator (5 个)
- `expert_analysis` - 多专家协调分析
- `storage_expert_analysis`
- `compaction_expert_analysis`
- `ingestion_expert_analysis`
- `get_available_experts`

## 🔍 工作原理详解

### 文件结构

```
/home/disk5/dingkai/github/gemini-cli/
├── .gemini/settings.json           # MCP 配置
├── .env                             # DeepSeek API Key
├── start-with-central-api.sh       # 启动脚本
└── mcp-example/
    ├── index-expert-api-complete.js   # 中心 API 服务器 ⭐
    └── experts/
        ├── expert-coordinator.js      # 协调所有 expert
        ├── cache-expert.js            # Cache Expert
        ├── storage-expert.js
        └── ... (其他 expert)

/home/disk1/dingkai/.starrocks-mcp/
└── thin-mcp-server.js              # Thin MCP Server ⭐
```

### 配置文件

#### `.gemini/settings.json`
```json
{
  "mcpServers": {
    "starrocks-expert": {
      "command": "node",
      "args": ["/home/disk1/dingkai/.starrocks-mcp/thin-mcp-server.js"],
      "env": {
        "CENTRAL_API": "http://localhost:3002",
        "CENTRAL_API_TOKEN": "demo-key",
        "SR_HOST": "localhost",
        "SR_USER": "root",
        "SR_PASSWORD": "",
        "SR_DATABASE": "information_schema",
        "SR_PORT": "9030"
      }
    }
  }
}
```

**关键配置**:
- `CENTRAL_API`: 中心 API 服务器地址
- `CENTRAL_API_TOKEN`: API Key
- `SR_HOST/USER/PASSWORD`: 本地数据库配置（thin-mcp-server 用于占位，实际执行在服务器端）

## 🔧 故障排查

### 问题 1: 中心 API 未运行

**症状**: Thin MCP Server 报错 "Failed to fetch tools from API"

**检查**:
```bash
curl http://localhost:3002/health -H "X-API-Key: demo-key"
```

**解决**: 启动中心 API 服务器

### 问题 2: API Key 错误

**症状**: 401 Unauthorized

**检查**:
- 中心 API 服务器的 `API_KEY` 环境变量
- Thin MCP Server 的 `CENTRAL_API_TOKEN` 配置

确保两者一致。

### 问题 3: 数据库连接失败

**症状**: Tool execution failed: Connection refused

**检查**:
```bash
mysql -h localhost -P 9030 -u root -p
```

**解决**: 确保 StarRocks 运行正常，检查服务器端的数据库配置。

### 问题 4: 工具列表为空

**症状**: `/mcp list` 显示没有工具

**检查**:
```bash
# 检查 thin-mcp-server 是否能获取工具列表
curl http://localhost:3002/api/tools -H "X-API-Key: demo-key"
```

**解决**: 确认 API Key 配置正确。

## 💡 与其他方案对比

### vs. 本地 MCP 模式 (index-expert-enhanced.js)

| 特性 | Solution C | 本地 MCP |
|------|-----------|---------|
| 工具数量 | 33 个 | 33 个 |
| 启动步骤 | 2 步 | 1 步 |
| 升级方式 | 只需更新服务器 | 更新客户端 |
| 数据库密码 | 服务器端 | 客户端 |
| 网络要求 | 需要访问 API | 无 |
| 适用场景 | 多用户/生产环境 | 单用户/开发环境 |

### vs. HTTP MCP 模式 (index-expert-http.js)

| 特性 | Solution C | HTTP MCP |
|------|-----------|----------|
| MCP 传输 | Stdio (客户端) | HTTP/SSE |
| 兼容性 | Gemini CLI 原生支持 | 需要特殊客户端 |
| 架构 | Thin Client + API | Thick Server |
| 数据流 | 3 层 | 2 层 |

## 📝 总结

### ✅ 你现在拥有

1. **中心 API 服务器**
   - 文件: `index-expert-api-complete.js`
   - 端口: 3002
   - 工具: 所有 33 个（包括 cache-expert）

2. **Thin MCP Server**
   - 文件: `/home/disk1/dingkai/.starrocks-mcp/thin-mcp-server.js`
   - 作用: 协调 Gemini CLI 和中心 API

3. **Gemini CLI**
   - 脚本: `./start-with-central-api.sh`
   - 模型: DeepSeek

### 🚀 快速开始

```bash
# 终端 1: 启动中心 API
cd /home/disk5/dingkai/github/gemini-cli/mcp-example
export API_PORT=3002 API_KEY=demo-key SR_HOST=localhost SR_USER=root SR_PASSWORD=""
node index-expert-api-complete.js

# 终端 2: 启动 Gemini CLI
cd /home/disk5/dingkai/github/gemini-cli
./start-with-central-api.sh
```

### 🎉 完成！

**所有 33 个工具（包括 cache-expert）现在可以通过 Solution C 架构使用！**
