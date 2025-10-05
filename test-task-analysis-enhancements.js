#!/usr/bin/env node

/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 测试新增的任务分析功能:
 * 1. running_task_details - 正在运行的子任务(START_TIME 不为 NULL, PROGRESS < 100)的进度信息
 * 2. pending_task_details - 未开始的子任务(START_TIME 为 NULL)的排队等待时间
 */

/* eslint-disable no-undef */

import mysql from 'mysql2/promise';
import { StarRocksCompactionExpert } from './mcp-example/experts/compaction-expert-integrated.js';

async function main() {
  // 创建数据库连接
  const connection = await mysql.createConnection({
    host: '127.0.0.1',
    port: 9030,
    user: 'root',
    password: '',
    database: 'information_schema',
  });

  console.error('✅ 数据库连接成功\n');

  const expert = new StarRocksCompactionExpert();

  console.error('🔍 分析 tpcds_1t.catalog_returns 表的慢 compaction 任务...\n');

  const result = await expert.analyzeSlowCompactionTasks(connection, {
    database_name: 'tpcds_1t',
    table_name: 'catalog_returns',
    min_duration_hours: 0.05,
    include_task_details: true,
    check_system_metrics: true,
  });

  // 检查是否有慢任务
  if (result.slow_jobs && result.slow_jobs.length > 0) {
    console.log('\n📊 慢任务分析结果:\n');
    console.log('='.repeat(80));

    for (const job of result.slow_jobs.slice(0, 3)) {
      // 只显示前3个
      console.log(`\n🔹 Job TXN_ID: ${job.txn_id}`);
      console.log(
        `   状态: ${job.status}, 运行时长: ${job.duration_hours.toFixed(2)} 小时`,
      );

      if (job.task_analysis) {
        const ta = job.task_analysis;

        console.log(`   总任务: ${ta.total_tasks}`);
        console.log(
          `   已完成: ${ta.completed_tasks}, 运行中: ${ta.running_tasks}, 待开始: ${ta.pending_tasks}`,
        );
        console.log(`   完成率: ${ta.completion_ratio}`);

        // 显示正在运行的任务统计
        if (ta.running_task_stats) {
          console.log(`\n   📈 运行中的任务统计:`);
          console.log(`      数量: ${ta.running_task_stats.count}`);
          console.log(`      平均进度: ${ta.running_task_stats.avg_progress}`);
          console.log(
            `      平均运行时长: ${ta.running_task_stats.avg_running_time_min} 分钟`,
          );
          console.log(
            `      最长运行时长: ${ta.running_task_stats.max_running_time_min} 分钟`,
          );

          // 显示详细任务样本
          if (ta.running_task_details && ta.running_task_details.length > 0) {
            console.log(`\n   📋 运行中的任务详情 (前5个):`);
            ta.running_task_details.slice(0, 5).forEach((task, idx) => {
              console.log(
                `      ${idx + 1}. BE ${task.be_id}, Tablet ${task.tablet_id}, 进度 ${task.progress_display}, 运行 ${task.running_time_display}`,
              );
            });
          }
        }

        // 显示等待中的任务统计
        if (ta.pending_task_stats) {
          console.log(`\n   ⏳ 等待中的任务统计:`);
          console.log(`      数量: ${ta.pending_task_stats.count}`);
          console.log(
            `      平均等待时间: ${ta.pending_task_stats.avg_wait_time_min} 分钟`,
          );
          console.log(
            `      最长等待时间: ${ta.pending_task_stats.max_wait_time_min} 分钟`,
          );

          // 显示详细任务样本
          if (ta.pending_task_details && ta.pending_task_details.length > 0) {
            console.log(`\n   📋 等待中的任务详情 (前5个):`);
            ta.pending_task_details.slice(0, 5).forEach((task, idx) => {
              console.log(
                `      ${idx + 1}. BE ${task.be_id}, Tablet ${task.tablet_id}, 等待 ${task.wait_time_display}`,
              );
            });
          }
        }

        // 显示 Profile 分析
        if (ta.profile_analysis) {
          console.log(`\n   🔍 Profile 分析:`);
          console.log(
            `      已完成任务数: ${ta.profile_analysis.completed_task_count}`,
          );
          console.log(
            `      平均排队时间: ${ta.profile_analysis.avg_in_queue_sec} 秒`,
          );
          console.log(
            `      缓存命中率: ${ta.profile_analysis.cache_hit_ratio}`,
          );

          if (
            ta.profile_analysis.issues &&
            ta.profile_analysis.issues.length > 0
          ) {
            console.log(`\n      ⚠️  发现问题:`);
            ta.profile_analysis.issues.forEach((issue) => {
              console.log(
                `         - [${issue.severity}] ${issue.description}`,
              );
              console.log(`           建议: ${issue.suggestion}`);
            });
          }
        }

        // 显示 BE RUNS 分析
        if (ta.be_runs_analysis && ta.be_runs_analysis.length > 0) {
          console.log(`\n   🖥️  BE 节点 RUNS 分析:`);
          ta.be_runs_analysis.forEach((be) => {
            console.log(`      - ${be.description}`);
          });
        }
      }

      console.log('\n' + '-'.repeat(80));
    }
  } else {
    console.log('\n✅ 未发现慢 compaction 任务');
  }

  // 显示诊断结果
  if (result.diagnosis) {
    console.log('\n\n🎯 诊断摘要:\n');
    console.log('='.repeat(80));

    if (result.diagnosis.issues && result.diagnosis.issues.length > 0) {
      console.log('\n⚠️  发现的问题:\n');
      result.diagnosis.issues.forEach((issue, idx) => {
        console.log(`${idx + 1}. [${issue.severity}] ${issue.type}`);
        console.log(`   ${issue.description}`);
        if (issue.impact) {
          console.log(`   影响: ${issue.impact}`);
        }
      });
    }

    if (
      result.diagnosis.recommendations &&
      result.diagnosis.recommendations.length > 0
    ) {
      console.log('\n\n💡 优化建议:\n');
      result.diagnosis.recommendations.forEach((rec, idx) => {
        console.log(`${idx + 1}. [${rec.priority}] ${rec.title}`);
        console.log(`   ${rec.description}`);
        if (rec.example_command) {
          console.log(`   示例命令: ${rec.example_command}`);
        }
      });
    }
  }

  await connection.end();
}

main().catch(console.error);
