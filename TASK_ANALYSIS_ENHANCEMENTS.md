# Task Analysis Enhancements for Compaction Expert

## 概述

在 `analyzeUnfinishedCompactionJobTasks` 函数中增加了对未完成子任务的详细分析功能。

## 新增功能

### 1. 正在运行的任务 (Running Tasks) 进度跟踪

**条件**: `START_TIME IS NOT NULL` 且 `PROGRESS < 100`

**新增字段**:

- `running_task_details`: 正在运行的任务详情数组（最多显示10个）
  - `be_id`: BE 节点 ID
  - `tablet_id`: Tablet ID
  - `runs`: 重试次数
  - `start_time`: 开始时间
  - `progress`: 进度百分比 (0-100)
  - `progress_display`: 格式化的进度显示 (如 "45%")
  - `running_time_min`: 运行时长（分钟，数值）
  - `running_time_display`: 格式化的运行时长显示 (如 "12.5 分钟")

- `running_task_stats`: 运行任务的聚合统计
  - `count`: 正在运行的任务数量
  - `avg_progress`: 平均进度百分比 (如 "45.3%")
  - `avg_running_time_min`: 平均运行时长（分钟）
  - `max_running_time_min`: 最长运行时长（分钟）

### 2. 等待中的任务 (Pending Tasks) 排队时间计算

**条件**: `START_TIME IS NULL`

**新增字段**:

- `pending_task_details`: 等待中的任务详情数组（最多显示10个）
  - `be_id`: BE 节点 ID
  - `tablet_id`: Tablet ID
  - `runs`: 重试次数
  - `wait_time_min`: 等待时长（分钟，数值）
  - `wait_time_display`: 格式化的等待时长显示 (如 "25.0 分钟")

- `pending_task_stats`: 等待任务的聚合统计
  - `count`: 等待中的任务数量
  - `avg_wait_time_min`: 平均等待时长（分钟）
  - `max_wait_time_min`: 最长等待时长（分钟）

## 实现细节

### 排队等待时间计算方法

等待时间 = 当前时间 - Job 开始时间 (`job.start_time`)

```javascript
if (job.start_time) {
  const jobStartTime = new Date(job.start_time);
  const now = new Date();
  const waitTimeMs = now - jobStartTime;
  const waitTimeMin = waitTimeMs / 1000 / 60;
}
```

### 运行时长计算方法

运行时间 = 当前时间 - 子任务开始时间 (`task.START_TIME`)

```javascript
const startTimeDate = new Date(startTime);
const now = new Date();
const runningTimeMs = now - startTimeDate;
const runningTimeMin = runningTimeMs / 1000 / 60;
```

## 使用示例

### 通过 MCP 工具调用

```javascript
const result = await expert.analyzeSlowCompactionTasks(connection, {
  database_name: 'tpcds_1t',
  table_name: 'catalog_returns',
  min_duration_hours: 0.05,
  include_task_details: true,
});

// 访问新增字段
const job = result.slow_jobs[0];
const taskAnalysis = job.task_analysis;

// 运行中的任务
console.log(taskAnalysis.running_task_stats);
console.log(taskAnalysis.running_task_details);

// 等待中的任务
console.log(taskAnalysis.pending_task_stats);
console.log(taskAnalysis.pending_task_details);
```

### 输出示例

```
⏳ 等待中的任务统计:
   数量: 32
   平均等待时间: 25.0 分钟
   最长等待时间: 25.0 分钟

📋 等待中的任务详情 (前5个):
   1. BE 441015, Tablet 938358, 等待 25.0 分钟
   2. BE 441015, Tablet 938359, 等待 25.0 分钟
   3. BE 441015, Tablet 938360, 等待 25.0 分钟
   4. BE 441015, Tablet 938361, 等待 25.0 分钟
   5. BE 441015, Tablet 938362, 等待 25.0 分钟

📈 运行中的任务统计:
   数量: 15
   平均进度: 45.3%
   平均运行时长: 12.5 分钟
   最长运行时长: 28.3 分钟

📋 运行中的任务详情 (前5个):
   1. BE 441015, Tablet 123456, 进度 45%, 运行 12.5 分钟
   2. BE 441015, Tablet 123457, 进度 67%, 运行 18.2 分钟
   3. BE 441015, Tablet 123458, 进度 23%, 运行 5.8 分钟
   4. BE 441015, Tablet 123459, 进度 89%, 运行 28.3 分钟
   5. BE 441015, Tablet 123460, 进度 34%, 运行 9.7 分钟
```

## 向后兼容性

- 保留了原有的 `unfinished_task_samples` 字段
- 所有原有字段保持不变
- 新增字段不影响现有代码

## 测试

运行测试脚本验证功能:

```bash
node test-task-analysis-enhancements.js
```

## 相关文件

- `/home/disk5/dingkai/github/gemini-cli/mcp-example/experts/compaction-expert-integrated.js` - 主实现文件
  - 函数: `analyzeUnfinishedCompactionJobTasks` (line 3604-3900)
- `/home/disk5/dingkai/github/gemini-cli/test-task-analysis-enhancements.js` - 测试脚本

## 应用场景

1. **排查 Compaction 任务长时间不启动**: 通过 `pending_task_stats` 查看任务在队列中等待了多久
2. **监控运行中任务的进度**: 通过 `running_task_details` 查看每个任务的实时进度
3. **识别卡住的任务**: 如果某个任务运行时间很长但进度很低，可能存在问题
4. **容量规划**: 根据等待时间和运行时间评估是否需要增加 `compact_threads` 或 `lake_compaction_max_tasks`
