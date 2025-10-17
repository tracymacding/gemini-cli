#!/usr/bin/env node

/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * StarRocks Thin MCP Server
 *
 * 轻量级客户端 MCP Server，用于方案 C (本地 Stdio MCP + 中心 API)
 *
 * 职责：
 * 1. 作为 Stdio MCP Server 被 Gemini CLI 调用
 * 2. 调用中心 API 获取需要执行的 SQL
 * 3. 连接本地 StarRocks 执行 SQL
 * 4. 将结果发送给中心 API 进行分析
 * 5. 返回分析报告给 Gemini CLI
 *
 * 优势：
 * - 极简（~150 行）
 * - 无业务逻辑（SQL 逻辑在中心 API）
 * - 基本不需要升级
 */

/* eslint-disable no-undef */

import 'dotenv/config';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import mysql from 'mysql2/promise';

class ThinMCPServer {
  constructor() {
    // 中心 API 配置
    this.centralAPI = process.env.CENTRAL_API || 'http://localhost:3002';
    this.apiToken = process.env.CENTRAL_API_TOKEN || '';

    // 本地数据库配置
    this.dbConfig = {
      host: process.env.SR_HOST || 'localhost',
      user: process.env.SR_USER || 'root',
      password: process.env.SR_PASSWORD || '',
      port: parseInt(process.env.SR_PORT) || 9030,
    };

    // 工具缓存（避免重复请求 API）
    this.toolsCache = null;
    this.cacheTime = null;
    this.cacheTTL = 3600000; // 1小时缓存

    console.error('🤖 Thin MCP Server initialized');
    console.error(`   Central API: ${this.centralAPI}`);
    console.error(`   Database: ${this.dbConfig.host}:${this.dbConfig.port}`);
  }

  /**
   * 从中心 API 获取工具列表
   */
  async getToolsFromAPI() {
    // 检查缓存
    if (this.toolsCache && Date.now() - this.cacheTime < this.cacheTTL) {
      return this.toolsCache;
    }

    try {
      const url = `${this.centralAPI}/api/tools`;
      const headers = {};
      if (this.apiToken) {
        headers['X-API-Key'] = this.apiToken;
      }

      const response = await fetch(url, { headers });

      if (!response.ok) {
        throw new Error(
          `API returned ${response.status}: ${response.statusText}`,
        );
      }

      const data = await response.json();

      // 更新缓存
      this.toolsCache = data.tools;
      this.cacheTime = Date.now();

      return data.tools;
    } catch (error) {
      console.error('Failed to fetch tools from API:', error.message);

      // 如果有缓存，返回缓存
      if (this.toolsCache) {
        console.error('Using cached tools due to API error');
        return this.toolsCache;
      }

      // 返回空列表
      return [];
    }
  }

  /**
   * 从中心 API 获取 SQL 查询定义
   */
  async getQueriesFromAPI(toolName, args = {}) {
    try {
      // 构建 URL，将 args 作为 query parameters
      const url = new URL(`${this.centralAPI}/api/queries/${toolName}`);

      // 将 args 添加到 query string
      Object.keys(args).forEach((key) => {
        if (args[key] !== undefined && args[key] !== null) {
          url.searchParams.append(key, args[key]);
        }
      });

      const headers = {};
      if (this.apiToken) {
        headers['X-API-Key'] = this.apiToken;
      }

      console.error(`   Fetching queries from: ${url.toString()}`);

      const response = await fetch(url.toString(), { headers });

      if (!response.ok) {
        throw new Error(
          `API returned ${response.status}: ${response.statusText}`,
        );
      }

      return await response.json();
    } catch (error) {
      throw new Error(
        `Failed to get queries for ${toolName}: ${error.message}`,
      );
    }
  }

  /**
   * 执行 SQL 查询
   */
  async executeQueries(queries) {
    const connection = await mysql.createConnection(this.dbConfig);
    const results = {};

    try {
      for (const query of queries) {
        try {
          console.error(`Executing query: ${query.id}`);
          const [rows] = await connection.query(query.sql);
          results[query.id] = rows;
        } catch (error) {
          console.error(`Query ${query.id} failed:`, error.message);
          results[query.id] = {
            error: error.message,
            sql: query.sql.substring(0, 100) + '...',
          };
        }
      }
    } finally {
      await connection.end();
    }

    return results;
  }

  /**
   * 发送结果给中心 API 进行分析
   */
  async analyzeResultsWithAPI(toolName, results, args = {}) {
    try {
      const url = `${this.centralAPI}/api/analyze/${toolName}`;
      const headers = {
        'Content-Type': 'application/json',
      };
      if (this.apiToken) {
        headers['X-API-Key'] = this.apiToken;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({ results, args }),
      });

      if (!response.ok) {
        throw new Error(
          `API returned ${response.status}: ${response.statusText}`,
        );
      }

      return await response.json();
    } catch (error) {
      throw new Error(`Failed to analyze results: ${error.message}`);
    }
  }

  /**
   * 格式化分析报告
   */
  formatAnalysisReport(analysis) {
    const {
      expert,
      storage_health,
      compaction_health,
      import_health,
      diagnosis_results,
      status,
      architecture_type,
      report,
    } = analysis;

    // 如果 analysis 已经包含格式化的 report，直接使用
    if (report && typeof report === 'string') {
      return report;
    }

    let formattedReport = '';

    // 处理特殊工具：存储放大分析
    if (status === 'not_applicable') {
      formattedReport = '⚠️  ' + analysis.message + '\n';
      formattedReport += '\n📋 详细数据请查看 JSON 输出部分';
      return formattedReport;
    }

    if (status === 'error') {
      formattedReport =
        '❌ 分析失败: ' + (analysis.error || analysis.message) + '\n';
      return formattedReport;
    }

    // 处理存储放大分析
    if (analysis.storage_amplification) {
      formattedReport = '📊 StarRocks 存储空间放大分析报告\n';
      if (architecture_type) {
        formattedReport += `🏗️  架构类型: ${architecture_type === 'shared_data' ? '存算分离' : '存算一体'}\n\n`;
      }

      const amp = analysis.storage_amplification;
      if (amp.amplification_ratio && amp.amplification_ratio !== '0') {
        const ratio = parseFloat(amp.amplification_ratio);
        const ampEmoji = ratio > 2.0 ? '🔴' : ratio > 1.5 ? '🟡' : '🟢';
        formattedReport += `${ampEmoji} 存储放大率: ${amp.amplification_ratio}x\n`;
        formattedReport += `   用户数据: ${amp.total_data_size_gb} GB\n`;
        formattedReport += `   对象存储: ${amp.total_storage_size_gb} GB\n\n`;
      }

      // 问题
      if (analysis.issues && analysis.issues.length > 0) {
        formattedReport += '⚠️  发现的问题:\n';
        analysis.issues.forEach((issue, index) => {
          const emoji = issue.severity === 'critical' ? '🔴' : '🟡';
          formattedReport += `  ${emoji} ${index + 1}. ${issue.message}\n`;
        });
        formattedReport += '\n';
      }

      // 建议
      if (analysis.recommendations && analysis.recommendations.length > 0) {
        formattedReport += '💡 优化建议:\n';
        analysis.recommendations.slice(0, 3).forEach((rec, index) => {
          formattedReport += `  ${index + 1}. [${rec.priority}] ${rec.title}\n`;
        });
      }

      formattedReport += '\n📋 详细数据请查看 JSON 输出部分';
      return formattedReport;
    }

    // 标题 - 健康分析类工具（增强防御性检查）
    if (expert === 'storage' && storage_health && storage_health.level) {
      formattedReport = '💾 StarRocks 存储专家分析报告\n';
      const health = storage_health;
      const healthEmoji =
        health.level === 'EXCELLENT'
          ? '🟢'
          : health.level === 'GOOD'
            ? '🟡'
            : '🔴';
      formattedReport += `${healthEmoji} 健康分数: ${health.score || 0}/100 (${health.level})\n`;
      formattedReport += `📊 状态: ${health.status || 'UNKNOWN'}\n\n`;
    } else if (
      expert === 'compaction' &&
      compaction_health &&
      compaction_health.level
    ) {
      formattedReport = '🗜️ StarRocks Compaction 专家分析报告\n';
      const health = compaction_health;
      const healthEmoji =
        health.level === 'EXCELLENT'
          ? '🟢'
          : health.level === 'GOOD'
            ? '🟡'
            : '🔴';
      formattedReport += `${healthEmoji} 健康分数: ${health.score || 0}/100 (${health.level})\n`;
      formattedReport += `📊 状态: ${health.status || 'UNKNOWN'}\n\n`;
    } else if (expert === 'ingestion' && import_health && import_health.level) {
      formattedReport = '📥 StarRocks 数据摄取专家分析报告\n';
      const health = import_health;
      const healthEmoji =
        health.level === 'EXCELLENT'
          ? '🟢'
          : health.level === 'GOOD'
            ? '🟡'
            : '🔴';
      formattedReport += `${healthEmoji} 健康分数: ${health.score || 0}/100 (${health.level})\n`;
      formattedReport += `📊 状态: ${health.status || 'UNKNOWN'}\n\n`;
    }

    // 诊断摘要
    if (diagnosis_results) {
      formattedReport += `📋 诊断摘要: ${diagnosis_results.summary}\n`;
      formattedReport += `🔍 发现问题: ${diagnosis_results.total_issues || diagnosis_results.total_jobs || 0}个\n\n`;
    }

    // 关键问题
    if (
      diagnosis_results &&
      diagnosis_results.criticals &&
      diagnosis_results.criticals.length > 0
    ) {
      formattedReport += '🔴 严重问题:\n';
      diagnosis_results.criticals.slice(0, 3).forEach((issue, index) => {
        formattedReport += `  ${index + 1}. ${issue.message}\n`;
      });
      formattedReport += '\n';
    }

    if (
      diagnosis_results &&
      diagnosis_results.warnings &&
      diagnosis_results.warnings.length > 0
    ) {
      formattedReport += '🟡 警告:\n';
      diagnosis_results.warnings.slice(0, 3).forEach((issue, index) => {
        formattedReport += `  ${index + 1}. ${issue.message}\n`;
      });
      formattedReport += '\n';
    }

    // 其他信息（包含分区详情等）
    if (
      diagnosis_results &&
      diagnosis_results.issues &&
      diagnosis_results.issues.length > 0
    ) {
      formattedReport += 'ℹ️  详细信息:\n';
      diagnosis_results.issues.forEach((issue, index) => {
        formattedReport += `  ${index + 1}. ${issue.message}\n`;
      });
      formattedReport += '\n';
    }

    // 建议
    if (
      analysis.professional_recommendations &&
      analysis.professional_recommendations.length > 0
    ) {
      formattedReport += '💡 专业建议 (前3条):\n';
      analysis.professional_recommendations
        .slice(0, 3)
        .forEach((rec, index) => {
          formattedReport += `  ${index + 1}. [${rec.priority}] ${rec.title}\n`;
        });
    }

    formattedReport += '\n📋 详细数据请查看 JSON 输出部分';

    return formattedReport;
  }

  /**
   * 启动服务器
   */
  async start() {
    const server = new Server(
      {
        name: 'starrocks-expert-thin',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      },
    );

    // 列出工具
    server.setRequestHandler(ListToolsRequestSchema, async () => {
      const tools = await this.getToolsFromAPI();
      return { tools };
    });

    // 执行工具
    server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name: toolName, arguments: args } = request.params;

      try {
        console.error(`\n🔧 Executing tool: ${toolName}`);
        console.error(`   Arguments:`, JSON.stringify(args).substring(0, 200));

        // 1. 从 API 获取需要执行的 SQL（传递 args 参数）
        console.error('   Step 1: Fetching SQL queries from Central API...');
        const queryDef = await this.getQueriesFromAPI(toolName, args);
        console.error(`   Got ${queryDef.queries.length} queries to execute`);

        let results = {};

        // 2. 执行 SQL（如果有的话）
        if (queryDef.queries.length > 0) {
          console.error('   Step 2: Executing SQL queries locally...');
          results = await this.executeQueries(queryDef.queries);
          console.error('   SQL execution completed');
        } else {
          console.error(
            '   Step 2: No SQL queries to execute (args-only tool)',
          );
        }

        // 3. 发送给 API 分析
        console.error(
          '   Step 3: Sending results to Central API for analysis...',
        );
        const analysis = await this.analyzeResultsWithAPI(
          toolName,
          results,
          args,
        );
        console.error('   Analysis completed\n');

        // 4. 格式化报告
        const report = this.formatAnalysisReport(analysis);

        return {
          content: [
            {
              type: 'text',
              text: report,
            },
            {
              type: 'text',
              text: JSON.stringify(analysis, null, 2),
            },
          ],
        };
      } catch (error) {
        console.error('Tool execution error:', error);

        return {
          content: [
            {
              type: 'text',
              text: `❌ 工具执行失败: ${error.message}\n\n请检查:\n1. 中心 API 是否运行 (${this.centralAPI})\n2. 数据库连接是否正常 (${this.dbConfig.host}:${this.dbConfig.port})\n3. API Token 是否正确`,
            },
          ],
          isError: true,
        };
      }
    });

    // 启动 Stdio 传输
    const transport = new StdioServerTransport();
    await server.connect(transport);

    console.error('✅ Thin MCP Server started successfully');
    console.error('   Waiting for requests from Gemini CLI...\n');
  }
}

// 启动服务器
const server = new ThinMCPServer();
server.start().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
