# StarRocks Expert + Gemini CLI 完全安装指南

> 从零开始，手把手教你配置 Gemini CLI 访问 StarRocks Expert 中心服务器

**目标读者**: 完全没有使用过 Gemini CLI 的新手

**完成时间**: 约 15-20 分钟

**使用的技术栈**:

- **Gemini CLI**: Google 官方命令行工具
- **DeepSeek**: 高性价比的 LLM 服务（代替 Google Gemini）
- **MCP (Model Context Protocol)**: 工具协议，连接 StarRocks Expert
- **StarRocks Expert**: 中心服务器，提供 34 个诊断工具

---

## 📖 目录

- [系统要求](#系统要求)
- [第一部分：安装 Gemini CLI](#第一部分安装-gemini-cli)
- [第二部分：配置 MCP 连接](#第二部分配置-mcp-连接)
- [第三部分：验证安装](#第三部分验证安装)
- [第四部分：开始使用](#第四部分开始使用)
- [常见问题](#常见问题)
- [故障排查](#故障排查)

---

## 🎯 系统要求

在开始之前，请确保你的系统满足以下要求：

### 必需条件

- **操作系统**: Linux (推荐 Ubuntu 20.04+) 或 macOS
- **Node.js**: 版本 >= 18.0.0
- **DeepSeek API Key**: 从 [DeepSeek Platform](https://platform.deepseek.com/) 获取
- **StarRocks Expert 中心服务器**: 已部署并运行（联系管理员获取服务器地址和 API Key）
- **StarRocks 数据库**: 正在运行且可访问（本地或远程）
- **网络**: 能够访问中心服务器和 StarRocks 数据库

### 检查 Node.js 版本

```bash
node --version
# 应该显示 v18.x.x 或更高版本
```

如果版本过低或未安装，请先安装 Node.js：

```bash
# Ubuntu/Debian
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# 或使用 nvm (推荐)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
source ~/.bashrc
nvm install 20
nvm use 20
```

### 检查 StarRocks 数据库

```bash
mysql -h localhost -P 9030 -u root -e "SELECT 1"
# 应该能够成功连接并返回结果
```

### 准备信息

在开始之前，请准备以下信息：

1. **DeepSeek API Key**: 从 [https://platform.deepseek.com/](https://platform.deepseek.com/) 获取
   - 示例: `sk-76b76db43c374097afe868e928f993ac`

2. **中心服务器信息**（向管理员索取）:
   - 服务器地址: 例如 `http://192.168.1.100:3002`
   - API Key: 例如 `5e4e3dfd350d6bd685472327fcf00036fcb4e0ea6129e9d5f4bf17de5a6692d7`

3. **StarRocks 数据库连接信息**:
   - 主机地址 (SR_HOST)
   - 端口 (SR_PORT, 默认 9030)
   - 用户名 (SR_USER)
   - 密码 (SR_PASSWORD)

---

## 第一部分：安装 Gemini CLI

### 步骤 1.1: 安装 Gemini CLI

请直接克隆本项目代码：

```bash
# 克隆项目
git clone git@github.com:tracymacding/gemini-cli.git

# 进入项目目录
cd gemini-cli

# 安装依赖
npm install

# 安装 MCP Server 依赖
cd starrocks-mcp-server
npm install
cd ..

# 构建项目
npm run build

# 链接到全局 (方便直接使用 gemini 命令)
npm link
```

### 步骤 1.2: 验证安装

```bash
gemini --version
# 应该显示版本号，例如: 0.8.0
```

### 步骤 1.3: 配置 DeepSeek API Key

**本项目使用 DeepSeek 作为 LLM 提供商**，相比 Google Gemini 更加灵活且成本更低。

#### 1.3.1 获取 DeepSeek API Key

1. 访问 [DeepSeek Platform](https://platform.deepseek.com/)
2. 注册/登录账号
3. 进入 API Keys 页面
4. 创建新的 API Key 并复制

#### 1.3.2 配置 API Key

**方式 A: 使用项目 .env 文件（推荐）**

```bash
cd gemini-cli

# 创建 .env 文件
cat > .env <<'EOF'
# DeepSeek API Key
# 获取地址: https://platform.deepseek.com/
DEEPSEEK_API_KEY=sk-your-deepseek-api-key-here
EOF
```

**方式 B: 设置环境变量**

```bash
# 临时设置（当前终端有效）
export DEEPSEEK_API_KEY="sk-your-deepseek-api-key-here"

# 永久设置（添加到 shell 配置）
echo 'export DEEPSEEK_API_KEY="sk-your-deepseek-api-key-here"' >> ~/.bashrc
source ~/.bashrc
```

### 步骤 1.4: 测试 DeepSeek 连接

```bash
# 方式 1: 使用启动脚本（推荐，已配置好 DeepSeek）
cd gemini-cli
./start-gemini-cli.sh

# 方式 2: 直接使用 gemini 命令
gemini --provider deepseek -m deepseek-chat -p "你好"
```

**预期输出**:

```
🤖 启动 Gemini CLI (DeepSeek + MCP)
====================================

✅ 已加载 .env 配置
✅ DeepSeek API Key: sk-76b76...
📡 检查中心 API 服务器...
   ✅ API 服务器运行正常
🔧 检查 MCP 配置...
   ✅ MCP 服务器已连接

🚀 启动 Gemini CLI...

💡 使用的功能:
   • DeepSeek 模型 (deepseek-chat)
   • MCP 工具 (StarRocks 诊断)

> 你好

[DeepSeek 的响应...]
```

✅ 如果看到响应，说明 Gemini CLI + DeepSeek 配置成功！

---

## 第二部分：配置 MCP 连接

### 步骤 2.1: 理解架构

```
你的问题 → Gemini CLI → MCP Client → 中心服务器 → StarRocks 数据库
                              ↓
                         执行 SQL
                              ↓
                         返回结果 → 中心服务器分析 → 显示报告
```

**关键点**:

- MCP Client 是一个桥接程序，位于 `gemini-cli/starrocks-mcp-server/` 目录
- 它从中心服务器获取 SQL，在本地执行，然后发送结果回服务器分析
- **数据不离开本地**，只有分析结果返回

### 步骤 2.2: 配置 Gemini CLI 连接 MCP

创建或编辑 `~/.gemini/settings.json` 文件：

```bash
mkdir -p ~/.gemini
nano ~/.gemini/settings.json
```

**复制以下内容（根据实际情况修改）**:

```json
{
  "mcpServers": {
    "starrocks-expert": {
      "command": "node",
      "args": [
        "/path/to/your/gemini-cli/starrocks-mcp-server/starrocks-mcp.js"
      ],
      "env": {
        "SR_HOST": "localhost",
        "SR_USER": "root",
        "SR_PASSWORD": "",
        "SR_PORT": "9030",
        "CENTRAL_API": "http://127.0.0.1:3002",
        "CENTRAL_API_TOKEN": "5e4e3dfd350d6bd685472327fcf00036fcb4e0ea6129e9d5f4bf17de5a6692d7"
      }
    }
  }
}
```

**🔧 需要修改的地方**:

1. **`args` 数组中的路径**: 改为你的 gemini-cli 实际安装路径

   ```bash
   # 查找 gemini-cli 目录
   find ~ -name "starrocks-mcp.js" 2>/dev/null
   ```

2. **`SR_HOST`**: StarRocks 数据库地址（通常是 localhost）

3. **`SR_PORT`**: StarRocks 查询端口（默认 9030）

4. **`SR_USER` 和 `SR_PASSWORD`**: 数据库用户名和密码

5. **`CENTRAL_API`**: 中心服务器地址
   - 如果服务器在本机：`http://127.0.0.1:3002`
   - 如果服务器在其他机器：`http://服务器IP:3002`

6. **`CENTRAL_API_TOKEN`**: 必须与中心服务器 `.env` 文件中的 `API_KEY` 一致

### 步骤 2.3: 验证配置文件

```bash
# 检查 JSON 格式是否正确
cat ~/.gemini/settings.json | jq .

# 应该能正常解析并显示格式化的 JSON
```

如果提示 `jq` 未安装：

```bash
sudo apt install jq  # Ubuntu/Debian
```

---

## 第三部分：验证安装

### 步骤 3.1: 启动 Gemini CLI

**推荐使用启动脚本**（已配置好 DeepSeek + MCP）:

```bash
cd gemini-cli
./start-gemini-cli.sh
```

或者手动启动：

```bash
gemini --provider deepseek -m deepseek-chat
```

### 步骤 3.2: 检查 MCP 服务器连接

在 Gemini CLI 中输入：

```
/mcp list
```

**预期输出**:

```
✓ starrocks-expert: node .../starrocks-mcp.js (stdio) - Connected
  Tools: 34
```

✅ 如果看到 "Connected" 和 "Tools: 34"，说明 MCP 连接成功！

❌ 如果显示 "Disconnected"，请查看 [故障排查](#故障排查) 部分。

### 步骤 3.3: 查看可用工具

在 Gemini CLI 中输入：

```
/tools
```

你应该看到 34 个 StarRocks Expert 工具，包括：

- `analyze_storage_amplification` - 存储空间放大分析
- `get_high_compaction_partitions` - 查找高 Compaction Score 分区
- `check_load_job_status` - 导入任务状态查询
- `get_recent_slow_queries` - 慢查询分析
- 等等...

### 步骤 3.4: 测试一个工具

在 Gemini CLI 中输入：

```
请帮我分析 StarRocks 的存储健康状况
```

Gemini 应该会：

1. 理解你的请求
2. 调用 `analyze_storage_amplification` 工具
3. 执行 SQL 查询
4. 返回详细的分析报告

✅ 如果看到分析报告，**恭喜你！安装完全成功！**

---

## 第四部分：开始使用

### 4.1 基本使用方式

**方式 1: 自然语言提问**

```
> 帮我找出最近 1 小时的慢查询

> 查看有哪些分区的 compaction score 比较高

> 分析一下数据导入任务的状态
```

Gemini 会自动选择合适的工具并执行。

**方式 2: 直接指定工具**

```
> 使用 get_recent_slow_queries 工具查询慢查询
```

**方式 3: 查看工具详情**

```
/tools

# 选择一个工具查看详情
```

### 4.2 常用场景示例

#### 场景 1: 存储健康检查

```
> 请分析存储空间放大情况
```

#### 场景 2: Compaction 问题诊断

```
> 查找 compaction score 超过 100 的分区

> 分析为什么 compaction score 这么高
```

#### 场景 3: 导入任务监控

```
> 查询 label 为 "load_20250104_001" 的导入任务状态

> 帮我分析最近的导入任务失败原因
```

#### 场景 4: 查询性能分析

```
> 找出最近 2 小时执行时间超过 30 秒的查询

> 分析 query_id 为 "abc123" 的查询 profile
```

### 4.3 高级功能

#### 多轮对话

Gemini CLI 支持上下文记忆，你可以进行连续对话：

```
> 查询最近的慢查询
> [Gemini 返回结果]

> 帮我分析第一个查询为什么慢
> [Gemini 会记住上一个结果，并进行深入分析]

> 给我优化建议
```

#### 保存对话

```
# 保存当前对话
/save my-analysis-session

# 稍后恢复
gemini --checkpoint my-analysis-session
```

#### 退出

```
/exit
# 或按 Ctrl+D
```

---

## 📚 所有可用工具列表

StarRocks Expert 提供 **34 个**专业诊断工具：

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

### 查询性能 (4 个)

- `get_recent_slow_queries` - 慢查询分析
- `analyze_query_latency` - Query 性能分析
- `get_query_profile` - 获取查询 Profile
- `analyze_query_profile` - Profile 深度分析

### 其他诊断工具 (13 个)

- 事务分析
- 日志分析
- 内存分析
- 表结构分析
- BE 节点监控
- 副本健康检查
- 等等...

**提示**: 在 Gemini CLI 中输入 `/tools` 可以看到完整列表和每个工具的详细说明。

---

## 常见问题

### Q1: 为什么 `/tools` 命令看不到 StarRocks Expert 的工具？

**可能原因**:

1. **MCP 服务器未连接**
   - 运行 `/mcp list` 检查连接状态
   - 如果显示 "Disconnected"，检查 `~/.gemini/settings.json` 配置

2. **中心服务器未运行**
   - 检查服务器状态：`curl http://服务器地址:3002/health`
   - 如果无响应，联系管理员检查服务器状态

3. **路径配置错误**
   - 确认 `settings.json` 中的 MCP 文件路径正确
   - 确认文件存在：`ls -la /path/to/starrocks-mcp.js`

### Q2: 提示 "Connection refused" 或 "ECONNREFUSED"

**原因**: 无法连接到中心服务器或 StarRocks 数据库

**解决方法**:

1. 检查中心服务器是否运行：

   ```bash
   curl http://服务器地址:3002/health
   ```

   如果无响应，联系管理员

2. 检查数据库连接：

   ```bash
   mysql -h 数据库地址 -P 9030 -u root -e "SELECT 1"
   ```

3. 检查防火墙设置和网络连通性

### Q3: 提示 "401 Unauthorized" 或 API Key 错误

**原因**: API Key 不匹配

**解决方法**:

1. 联系管理员获取正确的 API Key

2. 确保 `~/.gemini/settings.json` 中的 `CENTRAL_API_TOKEN` 与管理员提供的 API Key 一致

### Q4: 工具执行失败，提示 SQL 错误

**原因**: 可能是数据库权限不足或数据库版本不兼容

**解决方法**:

1. 确认数据库用户有足够权限：

   ```sql
   SHOW GRANTS FOR 'root'@'%';
   ```

2. 确认 StarRocks 版本（推荐 3.0+）：
   ```sql
   SELECT VERSION();
   ```

### Q5: 如何更新到最新版本？

**更新 Gemini CLI**:

```bash
# 进入项目目录
cd gemini-cli

# 拉取最新代码
git pull

# 重新安装依赖并构建
npm install
cd starrocks-mcp-server && npm install && cd ..
npm run build
```

**更新 StarRocks Expert 工具**:
中心服务器由管理员统一更新，客户端无需操作。更新后重启 Gemini CLI 即可使用新工具。

### Q6: 如何在多台机器上使用同一个中心服务器？

1. 在每台客户端机器上安装 Gemini CLI（按照本文档步骤 1）
2. 使用相同的 `~/.gemini/settings.json` 配置，确保：
   - `CENTRAL_API` 指向中心服务器地址
   - `CENTRAL_API_TOKEN` 使用管理员提供的统一 API Key
3. 确保网络可达（能 ping 通服务器并访问 3002 端口）

### Q7: 为什么使用 DeepSeek 而不是 Google Gemini？

**DeepSeek 的优势**:

- ✅ **更低成本**: 比 Google Gemini 便宜约 90%
- ✅ **更灵活**: 支持自定义配置和本地部署
- ✅ **性能优秀**: DeepSeek-V3 在多项基准测试中表现出色
- ✅ **中文友好**: 对中文支持更好，适合国内用户

**费用对比** (截至 2025-01):

- DeepSeek: ¥1/百万 tokens (输入), ¥2/百万 tokens (输出)
- Google Gemini: $0.075/百万 tokens (约 ¥0.54)，但有配额限制

### Q8: DeepSeek API Key 如何充值？

1. 登录 [DeepSeek Platform](https://platform.deepseek.com/)
2. 进入"账户"页面
3. 选择"充值"
4. 支持支付宝、微信支付
5. 最低充值 ¥10，推荐先充值 ¥50 测试

**费用估算**: 日常诊断使用，¥50 大约可以用 1-2 个月。

---

## 故障排查

### 检查清单

如果遇到问题，按顺序检查：

#### 1. 检查 Node.js 版本

```bash
node --version
# 必须 >= 18.0.0
```

#### 2. 检查 Gemini CLI 安装

```bash
gemini --version
gemini -p "test"
```

#### 3. 检查中心服务器状态

```bash
curl http://服务器地址:3002/health
# 应该返回 {"status":"healthy",...}
# 如果无响应，联系管理员
```

#### 4. 检查 MCP 配置文件

```bash
cat ~/.gemini/settings.json | jq .
# 确认 JSON 格式正确
# 确认路径存在
# 确认 API Key 匹配
```

#### 5. 检查 MCP Client 文件

```bash
ls -la gemini-cli/starrocks-mcp-server/starrocks-mcp.js
# 确认文件存在且可执行
```

#### 6. 检查数据库连接

```bash
mysql -h localhost -P 9030 -u root -e "SELECT 1"
```

#### 7. 手动测试 MCP Client

```bash
export SR_HOST=localhost
export SR_PORT=9030
export SR_USER=root
export SR_PASSWORD=
export CENTRAL_API=http://127.0.0.1:3002
export CENTRAL_API_TOKEN=your-api-key

node /path/to/starrocks-mcp.js
# 应该启动并等待输入
```

### 查看日志

**Gemini CLI 调试模式**:

```bash
# 在 Gemini CLI 中启用调试
/debug on

# 查看详细的工具调用日志
```

### 联系管理员

如果以上步骤都无法解决问题，请联系管理员并提供：

1. 错误信息截图
2. `/mcp list` 命令输出
3. `~/.gemini/settings.json` 配置（注意隐藏密码）
4. 你的操作系统和 Node.js 版本

---

## 🎉 完成！

恭喜你完成了全部安装和配置！现在你可以：

1. ✅ 使用 Gemini CLI 以自然语言方式诊断 StarRocks 问题
2. ✅ 访问 34 个专业的诊断工具
3. ✅ 进行多轮对话式的问题分析
4. ✅ 保存和恢复分析会话

### 下一步

- 查看 [完整工具列表](#所有可用工具列表)
- 尝试 [常用场景示例](#52-常用场景示例)
- 阅读 [Gemini CLI 官方文档](https://github.com/google-gemini/gemini-cli)

### 获取帮助

如果遇到问题：

1. 查看 [常见问题](#常见问题) 和 [故障排查](#故障排查)
2. 运行诊断脚本：`./test-starrocks-expert.sh`
3. 查看详细配置指南：`STARROCKS_EXPERT_SETUP.md`
4. 在 Gemini CLI 中使用 `/bug` 命令报告问题

---

**最后更新**: 2025-01-04
**维护者**: StarRocks Team
**版本**: v1.0
