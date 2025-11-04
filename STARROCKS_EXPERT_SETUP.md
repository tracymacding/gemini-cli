# StarRocks Expert + Gemini CLI 配置指南

> 一键配置 Gemini CLI 连接到 StarRocks Expert 中心服务器

## 📋 架构说明

```
Gemini CLI (本地)
   ↓
MCP Client (starrocks-mcp.js)
   ↓ HTTP
中心服务器 (StarRocks Expert API)
   ↓
StarRocks 数据库 (本地)
```

**工作流程:**

1. 你在 Gemini CLI 输入问题（如"分析存储健康"）
2. MCP Client 请求中心服务器获取需要执行的 SQL
3. MCP Client 在本地执行 SQL
4. MCP Client 将结果发送给中心服务器分析
5. 中心服务器返回分析报告
6. Gemini CLI 显示结果

**优势:**

- ✅ 数据不离开本地（SQL 在本地执行）
- ✅ 分析逻辑集中管理（只需更新中心服务器）
- ✅ 34 个专业诊断工具开箱即用

## 🚀 快速开始（3 步）

### 第 1 步: 启动中心服务器

在**一个终端**运行：

```bash
cd /home/disk5/dingkai/github/operation-experts/starrocks-expert

# 方式 1: 使用 PM2（推荐，后台运行）
npm install -g pm2
pm2 start src/server-solutionc.js --name starrocks-expert

# 方式 2: 直接启动（前台运行）
npm start
```

验证服务器运行：

```bash
curl http://localhost/health
# 应该返回: {"status":"healthy",...}
```

### 第 2 步: 配置 Gemini CLI

**唯一需要配置的文件:** `~/.gemini/settings.json`

```bash
cat > ~/.gemini/settings.json <<'EOF'
{
  "mcpServers": {
    "starrocks-expert": {
      "command": "node",
      "args": [
        "/home/disk5/dingkai/github/gemini-cli/starrocks-mcp-server/starrocks-mcp.js"
      ],
      "env": {
        "SR_HOST": "localhost",
        "SR_USER": "root",
        "SR_PASSWORD": "",
        "SR_PORT": "9030",
        "CENTRAL_API": "http://127.0.0.1:80",
        "CENTRAL_API_TOKEN": "5e4e3dfd350d6bd685472327fcf00036fcb4e0ea6129e9d5f4bf17de5a6692d7"
      }
    }
  }
}
EOF
```

**配置说明:**

| 参数                | 说明                 | 默认值              |
| ------------------- | -------------------- | ------------------- |
| `SR_HOST`           | StarRocks 数据库地址 | localhost           |
| `SR_USER`           | 数据库用户名         | root                |
| `SR_PASSWORD`       | 数据库密码           | (空)                |
| `SR_PORT`           | 数据库端口           | 9030                |
| `CENTRAL_API`       | 中心服务器地址       | http://127.0.0.1:80 |
| `CENTRAL_API_TOKEN` | API 认证密钥         | 部署时生成的密钥    |

### 第 3 步: 启动 Gemini CLI

```bash
cd /home/disk5/dingkai/github/gemini-cli
./start-gemini-cli.sh
```

## 🧪 验证安装

### 1. 检查 MCP 服务器

```
> /mcp list
```

应该看到：

```
✓ starrocks-expert: node .../starrocks-mcp.js (stdio) - Connected
  Tools: 34
```

### 2. 查看可用工具

```
> /tools
```

应该看到 34 个 StarRocks Expert 工具。

### 3. 测试分析功能

```
> 请帮我分析 StarRocks 的存储健康状况
```

Gemini 会自动调用 `analyze_storage_amplification` 工具进行分析。

## 📚 可用工具列表（34 个）

### 存储分析 (1 个)

- `analyze_storage_amplification` - 存储空间放大分析

### Compaction (7 个)

- `get_table_partitions_compaction_score` - 查询分区 Compaction Score
- `get_high_compaction_partitions` - 查找高 CS 分区
- `get_compaction_threads` - 查询 Compaction 线程配置
- `set_compaction_threads` - 设置 Compaction 线程数
- `get_running_compaction_tasks` - 查询运行中的任务
- `analyze_high_compaction_score` - 深度分析高 CS 问题
- `analyze_slow_compaction_tasks` - 分析慢任务

### 数据导入 (6 个)

- `check_load_job_status` - 导入任务状态查询
- `analyze_table_import_frequency` - 表级导入频率分析
- `check_stream_load_tasks` - Stream Load 任务检查
- `check_routine_load_config` - Routine Load 配置检查
- `analyze_reached_timeout` - Reached Timeout 问题分析
- `analyze_load_channel_profile` - LoadChannel Profile 分析

### 缓存分析 (3 个)

- `analyze_cache_performance` - Data Cache 性能分析
- `analyze_cache_jitter` - Data Cache 抖动分析
- `analyze_metadata_cache` - Metadata Cache 使用率分析

### 查询性能 (3 个)

- `get_recent_slow_queries` - 慢查询分析
- `analyze_query_latency` - Query 性能分析
- `get_query_profile` - 获取查询 Profile
- `analyze_query_profile` - Profile 深度分析

### 其他 (14 个)

- 事务分析、日志分析、内存分析、表结构分析、运维工具等

完整工具列表和说明: 运行 `/tools` 命令查看

## 💡 使用示例

### 示例 1: 分析存储健康

```
> 请分析一下存储空间放大情况
```

### 示例 2: 检查 Compaction 状态

```
> 查看有哪些分区的 compaction score 比较高
```

### 示例 3: 分析导入任务

```
> 查询 label 为 "load_20250104" 的导入任务状态
```

### 示例 4: 分析慢查询

```
> 帮我找出最近 1 小时的慢查询
```

### 示例 5: 分析 Query Profile

```
> 分析这个 profile 文件: /tmp/query_profile.txt
```

## 🔧 高级配置

### 远程中心服务器

如果中心服务器部署在其他机器上，修改配置：

```json
{
  "env": {
    "CENTRAL_API": "http://YOUR_SERVER_IP:80",
    "CENTRAL_API_TOKEN": "your-api-key"
  }
}
```

### 自定义数据库配置

如果 StarRocks 数据库不在本地或使用不同端口：

```json
{
  "env": {
    "SR_HOST": "192.168.1.100",
    "SR_PORT": "9030",
    "SR_USER": "admin",
    "SR_PASSWORD": "your-password"
  }
}
```

### 使用 PM2 管理中心服务器

```bash
# 启动
pm2 start src/server-solutionc.js --name starrocks-expert

# 查看状态
pm2 status

# 查看日志
pm2 logs starrocks-expert

# 重启
pm2 restart starrocks-expert

# 停止
pm2 stop starrocks-expert

# 开机自启动
pm2 startup
pm2 save
```

## 🐛 故障排查

### 问题 1: 工具列表为空

**症状:** `/tools` 命令看不到任何工具

**检查:**

```bash
# 1. 检查中心服务器是否运行
curl http://localhost/health

# 2. 检查配置文件
cat ~/.gemini/settings.json

# 3. 测试 MCP 连接
echo '{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}' | \
  node /home/disk5/dingkai/github/gemini-cli/starrocks-mcp-server/starrocks-mcp.js
```

**解决:**

- 确保中心服务器正在运行
- 检查 `CENTRAL_API` 地址是否正确
- 检查 `CENTRAL_API_TOKEN` 是否匹配

### 问题 2: MCP 服务器显示 Disconnected

**症状:** `/mcp list` 显示 starrocks-expert 未连接

**检查:**

```bash
# 检查 MCP 文件是否存在
ls -la /home/disk5/dingkai/github/gemini-cli/starrocks-mcp-server/starrocks-mcp.js

# 检查 Node.js 版本（需要 >= 18）
node --version
```

**解决:**

- 确保 MCP 文件路径正确
- 重启 Gemini CLI

### 问题 3: 工具执行失败

**症状:** 工具调用失败，报错 "Connection refused"

**检查:**

```bash
# 检查数据库连接
mysql -h localhost -P 9030 -u root -e "SELECT 1"
```

**解决:**

- 确保 StarRocks 数据库正在运行
- 检查数据库配置（SR_HOST, SR_PORT, SR_USER）

### 问题 4: API Key 认证失败

**症状:** 401 Unauthorized

**检查:**

```bash
# 查看中心服务器配置的 API Key
cd /home/disk5/dingkai/github/operation-experts/starrocks-expert
cat .env | grep API_KEY
```

**解决:**

- 确保 `settings.json` 中的 `CENTRAL_API_TOKEN` 与中心服务器的 `API_KEY` 一致

## 📖 相关文档

- [中心服务器部署指南](/home/disk5/dingkai/github/operation-experts/starrocks-expert/DEPLOYMENT.md)
- [StarRocks Expert API 文档](/home/disk5/dingkai/github/operation-experts/starrocks-expert/README.md)
- [MCP 协议说明](https://modelcontextprotocol.io)

## 🎯 配置文件总结

**只需要配置 1 个文件:**

- `~/.gemini/settings.json` - Gemini CLI + MCP 配置（唯一配置文件）

**不需要配置:**

- ~~`~/.starrocks-mcp/.env`~~ - 已废弃
- ~~`/home/disk5/dingkai/github/gemini-cli/starrocks-mcp-server/.env`~~ - 不需要

**其他文件:**

- `/home/disk5/dingkai/github/gemini-cli/.env` - 仅用于 DeepSeek API Key（与 StarRocks Expert 无关）

## 🚀 一键配置脚本

如果嫌麻烦，使用自动配置脚本：

```bash
cd /home/disk5/dingkai/github/gemini-cli/starrocks-mcp-server
./configure-client.sh
```

脚本会自动：

1. 测试中心服务器连接
2. 测试数据库连接
3. 生成配置文件
4. 验证配置

## 📞 获取帮助

遇到问题? 运行诊断脚本：

```bash
cd /home/disk5/dingkai/github/gemini-cli
./test-starrocks-expert.sh
```

这会自动检查：

- ✅ 中心 API 服务器状态
- ✅ 数据库连接
- ✅ MCP 客户端状态
- ✅ Gemini CLI 配置

---

**最后更新:** 2025-01-04
**维护者:** StarRocks Team
