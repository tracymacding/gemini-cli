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
import fs from 'node:fs';

class ThinMCPServer {
  constructor() {
    // 中心 API 配置
    this.centralAPI = process.env.CENTRAL_API || 'http://localhost:80';
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
      // 使用 POST 请求，将 args 放在请求体中避免 URL 过长
      const url = `${this.centralAPI}/api/queries/${toolName}`;

      const headers = {
        'Content-Type': 'application/json',
      };
      if (this.apiToken) {
        headers['X-API-Key'] = this.apiToken;
      }

      console.error(`   Fetching queries from: ${url}`);
      console.error(`   Args size: ${JSON.stringify(args).length} characters`);

      const response = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({ args }),
      });

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
   * 处理文件路径参数，读取文件内容
   */
  async processFileArgs(args) {
    const processedArgs = { ...args };

    // 处理 file_path 参数
    if (args.file_path) {
      try {
        console.error(`   Reading file: ${args.file_path}`);
        const content = fs.readFileSync(args.file_path, 'utf-8');
        const fileSizeKB = content.length / 1024;
        console.error(`   File content loaded: ${fileSizeKB.toFixed(2)} KB`);

        // 对于大文件（超过 50KB），不通过 JSON-RPC 传输内容，而是在分析阶段处理
        if (fileSizeKB > 50) {
          console.error(
            `   Large file detected (${fileSizeKB.toFixed(2)} KB > 50 KB), will handle in analysis phase`,
          );
          // 保留路径信息，不传输内容
          processedArgs.large_file_path = args.file_path;
        } else {
          processedArgs.profile = content; // 将文件内容设置为 profile 参数
        }
      } catch (error) {
        console.error(
          `   Failed to read file ${args.file_path}: ${error.message}`,
        );
        throw new Error(
          `Failed to read file ${args.file_path}: ${error.message}`,
        );
      }
    }

    // 处理 table_schema_path 参数
    if (args.table_schema_path) {
      try {
        console.error(
          `   Reading table schema file: ${args.table_schema_path}`,
        );
        const schemaContent = fs.readFileSync(args.table_schema_path, 'utf-8');
        // 如果 table_schemas 是数组，替换第一个，否则创建数组
        if (Array.isArray(processedArgs.table_schemas)) {
          processedArgs.table_schemas[0] = schemaContent;
        } else {
          processedArgs.table_schemas = [schemaContent];
        }
        console.error(
          `   Table schema loaded: ${(schemaContent.length / 1024).toFixed(2)} KB`,
        );
      } catch (error) {
        console.error(
          `   Failed to read table schema file ${args.table_schema_path}: ${error.message}`,
        );
        // 表结构文件是可选的，读取失败不应该中断流程
      }
    }

    return processedArgs;
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

      // 处理大文件：在这里读取内容而不是通过 JSON-RPC 传输
      const processedArgs = { ...args };
      if (args.large_file_path) {
        console.error(
          `   Loading large file for analysis: ${args.large_file_path}`,
        );
        try {
          const content = fs.readFileSync(args.large_file_path, 'utf-8');
          processedArgs.profile = content;
          processedArgs.file_path = args.large_file_path; // 保持原始路径信息
          delete processedArgs.large_file_path; // 清理临时字段
          console.error(
            `   Large file loaded: ${(content.length / 1024).toFixed(2)} KB`,
          );
        } catch (error) {
          console.error(
            `   Failed to read large file ${args.large_file_path}: ${error.message}`,
          );
          throw new Error(
            `Failed to read large file ${args.large_file_path}: ${error.message}`,
          );
        }
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({ results, args: processedArgs }),
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
    // 如果分析对象为空或无法识别结构，返回错误信息
    if (!analysis || typeof analysis !== 'object') {
      return '❌ 分析结果格式错误或为空';
    }

    // 处理 HTML 报告响应（generate_html_report 工具）- 需要在其他检查之前处理
    if (analysis.html_content || analysis.output_path) {
      return `📊 StarRocks HTML 性能分析报告生成完成!\n\n${analysis.message || 'HTML 报告生成成功'}\n\n📋 详细分析请查看 HTML 文件: ${analysis.output_path || '/tmp/profile_analysis_report.html'}`;
    }

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

    // 关键问题 - 加强防御性检查
    if (
      diagnosis_results &&
      diagnosis_results.criticals &&
      Array.isArray(diagnosis_results.criticals) &&
      diagnosis_results.criticals.length > 0
    ) {
      formattedReport += '🔴 严重问题:\n';
      diagnosis_results.criticals.slice(0, 3).forEach((issue, index) => {
        if (issue && issue.message) {
          formattedReport += `  ${index + 1}. ${issue.message}\n`;
        }
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

        // 0. 处理文件路径参数（如果有的话）
        console.error('   Step 0: Processing file arguments...');
        const processedArgs = await this.processFileArgs(args);
        console.error('   File processing completed');

        // 1. 从 API 获取需要执行的 SQL（传递处理后的 args 参数）
        console.error('   Step 1: Fetching SQL queries from Central API...');
        const queryDef = await this.getQueriesFromAPI(toolName, processedArgs);
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
          processedArgs,
        );
        console.error('   Analysis completed\n');

        // 4. 格式化报告
        const report = this.formatAnalysisReport(analysis);

        // 对于 HTML 报告，移除大文件内容避免传输阻塞
        const analysisForJson = { ...analysis };
        if (analysis.html_content && analysis.output_path) {
          // 移除大的 HTML 内容，只保留关键信息
          analysisForJson.html_content = `[HTML Content Removed - ${Math.round(analysis.html_content.length / 1024)}KB]`;
          console.error(
            `   Removed large HTML content (${Math.round(analysis.html_content.length / 1024)}KB) from JSON response`,
          );
        }

        return {
          content: [
            {
              type: 'text',
              text: report,
            },
            {
              type: 'text',
              text: JSON.stringify(analysisForJson, null, 2),
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
