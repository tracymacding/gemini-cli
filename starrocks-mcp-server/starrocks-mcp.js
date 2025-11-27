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

    // Prometheus 配置
    this.prometheusConfig = {
      protocol: process.env.PROMETHEUS_PROTOCOL || 'http',
      host: process.env.PROMETHEUS_HOST || 'localhost',
      port: parseInt(process.env.PROMETHEUS_PORT) || 9090,
    };

    // 工具缓存（避免重复请求 API）
    this.toolsCache = null;
    this.cacheTime = null;
    this.cacheTTL = 3600000; // 1小时缓存

    console.error('🤖 Thin MCP Server initialized');
    console.error(`   Central API: ${this.centralAPI}`);
    console.error(`   Database: ${this.dbConfig.host}:${this.dbConfig.port}`);
    console.error(
      `   Prometheus: ${this.prometheusConfig.protocol}://${this.prometheusConfig.host}:${this.prometheusConfig.port}`,
    );
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
   * 执行查询（SQL + Prometheus）
   */
  async executeQueries(queries) {
    const results = {};
    let connection = null;

    // 分离 SQL 查询和 Prometheus 查询
    const sqlQueries = queries.filter((q) => q.type === 'sql' || !q.type);
    const prometheusQueries = queries.filter(
      (q) => q.type === 'prometheus_range' || q.type === 'prometheus_instant',
    );

    // 执行 SQL 查询
    if (sqlQueries.length > 0) {
      try {
        connection = await mysql.createConnection(this.dbConfig);
        // 禁用当前 session 的 profile 记录，避免系统查询挤掉用户查询的 profile
        await connection.query('SET enable_profile = false');
        console.error('   Disabled profile recording for this session');
        for (const query of sqlQueries) {
          try {
            console.error(`Executing SQL query: ${query.id}`);
            const [rows] = await connection.query(query.sql);
            results[query.id] = rows;
          } catch (error) {
            console.error(`SQL Query ${query.id} failed:`, error.message);
            results[query.id] = {
              error: error.message,
              sql: query.sql ? query.sql.substring(0, 100) + '...' : 'N/A',
            };
          }
        }
      } finally {
        if (connection) await connection.end();
      }
    }

    // 执行 Prometheus 查询
    for (const query of prometheusQueries) {
      try {
        console.error(
          `Executing Prometheus query: ${query.id} (${query.type})`,
        );
        if (query.type === 'prometheus_range') {
          results[query.id] = await this.queryPrometheusRange(query);
        } else {
          results[query.id] = await this.queryPrometheusInstant(query);
        }
      } catch (error) {
        console.error(`Prometheus Query ${query.id} failed:`, error.message);
        results[query.id] = {
          error: error.message,
          query: query.query ? query.query.substring(0, 100) + '...' : 'N/A',
        };
      }
    }

    return results;
  }

  /**
   * 查询 Prometheus 即时数据
   */
  async queryPrometheusInstant(queryDef) {
    const baseUrl = `${this.prometheusConfig.protocol}://${this.prometheusConfig.host}:${this.prometheusConfig.port}`;
    const url = `${baseUrl}/api/v1/query`;

    const params = new URLSearchParams({
      query: queryDef.query,
    });

    const response = await fetch(`${url}?${params}`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (!response.ok) {
      throw new Error(
        `Prometheus API error: ${response.status} ${response.statusText}`,
      );
    }

    const data = await response.json();
    if (data.status !== 'success') {
      throw new Error(
        `Prometheus query failed: ${data.error || 'unknown error'}`,
      );
    }

    return data.data;
  }

  /**
   * 查询 Prometheus 范围数据
   */
  async queryPrometheusRange(queryDef) {
    const baseUrl = `${this.prometheusConfig.protocol}://${this.prometheusConfig.host}:${this.prometheusConfig.port}`;
    const url = `${baseUrl}/api/v1/query_range`;

    // 解析时间范围
    const now = Math.floor(Date.now() / 1000);
    let startTime = now - 3600; // 默认 1 小时

    const timeRange = queryDef.start || '1h';
    const rangeMatch = timeRange.match(/^(\d+)([hmd])$/);
    if (rangeMatch) {
      const value = parseInt(rangeMatch[1]);
      const unit = rangeMatch[2];
      switch (unit) {
        case 'h':
          startTime = now - value * 3600;
          break;
        case 'm':
          startTime = now - value * 60;
          break;
        case 'd':
          startTime = now - value * 86400;
          break;
      }
    }

    const params = new URLSearchParams({
      query: queryDef.query,
      start: startTime.toString(),
      end: now.toString(),
      step: queryDef.step || '1m',
    });

    const response = await fetch(`${url}?${params}`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (!response.ok) {
      throw new Error(
        `Prometheus API error: ${response.status} ${response.statusText}`,
      );
    }

    const data = await response.json();
    if (data.status !== 'success') {
      throw new Error(
        `Prometheus query failed: ${data.error || 'unknown error'}`,
      );
    }

    return data.data;
  }

  /**
   * 执行 CLI 命令（用于对象存储空间查询等场景）
   * @param {Array} commands - CLI 命令列表
   * @returns {Object} 执行结果
   */
  async executeCliCommands(commands) {
    const { exec } = await import('child_process');
    const { promisify } = await import('util');
    const execAsync = promisify(exec);

    const results = {
      cli_results: [],
      cli_summary: {
        total: commands.length,
        successful: 0,
        failed: 0,
        execution_time_ms: 0
      }
    };

    const startTime = Date.now();
    const maxConcurrency = 10;
    const commandTimeoutMs = 30000; // 30 秒超时

    // 分批并发执行
    for (let i = 0; i < commands.length; i += maxConcurrency) {
      const batch = commands.slice(i, i + maxConcurrency);

      const batchResults = await Promise.all(
        batch.map(async (cmd) => {
          try {
            console.error(`   Executing CLI: ${cmd.command.substring(0, 80)}...`);
            const cmdStartTime = Date.now();

            const { stdout, stderr } = await execAsync(cmd.command, {
              timeout: commandTimeoutMs,
              maxBuffer: 10 * 1024 * 1024 // 10MB
            });

            const duration = Date.now() - cmdStartTime;

            // 根据命令类型返回不同格式的结果
            const cmdType = cmd.type || '';

            if (cmdType === 'ossutil_ls' || cmdType === 'aws_s3_ls') {
              // 列目录命令：返回原始输出
              return {
                table_key: cmd.table_key,
                table_path: cmd.table_path,
                storage_type: cmd.storage_type,
                type: cmdType,
                success: true,
                output: stdout,
                execution_time_ms: duration
              };
            } else if (cmdType === 'get_size') {
              // 获取大小命令：返回原始输出供 expert 解析
              return {
                table_key: cmd.table_key,
                partition_id: cmd.partition_id,
                path: cmd.path,
                storage_type: cmd.storage_type,
                success: true,
                output: stdout.trim(),
                execution_time_ms: duration
              };
            } else {
              // 存储空间查询命令（默认）：解析大小
              const sizeBytes = this.parseStorageCliOutput(cmd.storage_type || cmd.actual_storage_type, stdout);
              return {
                partition_key: cmd.partition_key,
                path: cmd.path,
                storage_type: cmd.storage_type,
                success: sizeBytes !== null,
                size_bytes: sizeBytes,
                execution_time_ms: duration
              };
            }
          } catch (error) {
            const cmdType = cmd.type || '';
            console.error(`   CLI failed for ${cmd.partition_key || cmd.table_key}: ${error.message}`);

            if (cmdType === 'ossutil_ls' || cmdType === 'aws_s3_ls') {
              return {
                table_key: cmd.table_key,
                table_path: cmd.table_path,
                storage_type: cmd.storage_type,
                type: cmdType,
                success: false,
                error: error.message
              };
            } else if (cmdType === 'get_size') {
              return {
                table_key: cmd.table_key,
                partition_id: cmd.partition_id,
                path: cmd.path,
                storage_type: cmd.storage_type,
                success: false,
                error: error.message
              };
            } else {
              return {
                partition_key: cmd.partition_key,
                path: cmd.path,
                storage_type: cmd.storage_type,
                success: false,
                error: error.message
              };
            }
          }
        })
      );

      for (const result of batchResults) {
        results.cli_results.push(result);
        if (result.success) {
          results.cli_summary.successful++;
        } else {
          results.cli_summary.failed++;
        }
      }
    }

    results.cli_summary.execution_time_ms = Date.now() - startTime;
    console.error(`   CLI execution completed: ${results.cli_summary.successful} success, ${results.cli_summary.failed} failed`);

    return results;
  }

  /**
   * 解析存储 CLI 输出获取大小（字节数）
   */
  parseStorageCliOutput(storageType, stdout) {
    try {
      switch (storageType) {
        case 's3':
        case 's3a':
        case 's3n': {
          // AWS S3: "Total Size: 1234567890 Bytes"
          const match = stdout.match(/Total Size:\s*([\d,]+)\s*Bytes/i);
          if (match) return parseInt(match[1].replace(/,/g, ''), 10);
          if (stdout.includes('Total Objects: 0')) return 0;
          break;
        }
        case 'oss': {
          // OSS: "total object sum size: 1234567890"
          const match = stdout.match(/total object sum size:\s*([\d]+)/i);
          if (match) return parseInt(match[1], 10);
          if (stdout.includes('total object count: 0')) return 0;
          break;
        }
        case 's3cmd': {
          // s3cmd du 输出格式: "1234567890   123 objects s3://bucket/path/"
          const match = stdout.match(/^(\d+)\s+\d+\s+objects?/m);
          if (match) return parseInt(match[1], 10);
          // 空目录情况
          if (stdout.includes('0 objects')) return 0;
          break;
        }
        case 'cos':
        case 'cosn': {
          // COS: "(1234567890 Bytes)" or "Total Size: 1.23 GB"
          const bytesMatch = stdout.match(/\((\d+)\s*Bytes?\)/i);
          if (bytesMatch) return parseInt(bytesMatch[1], 10);
          break;
        }
        case 'hdfs': {
          // HDFS: "1234567890  path"
          const match = stdout.match(/^(\d+)/);
          if (match) return parseInt(match[1], 10);
          break;
        }
        case 'gs': {
          // GCS: "1234567890  gs://bucket/path"
          const match = stdout.match(/^(\d+)/);
          if (match) return parseInt(match[1], 10);
          break;
        }
        case 'azblob': {
          // Azure: 直接是数字
          const num = parseInt(stdout.trim(), 10);
          if (!isNaN(num)) return num;
          break;
        }
      }
    } catch (e) {
      console.error(`   Failed to parse CLI output for ${storageType}: ${e.message}`);
    }
    return null;
  }

  /**
   * 获取多个查询的详细 Profile
   * @param {Array} profileList - SHOW PROFILELIST 返回的结果
   * @param {Object} options - 过滤选项
   * @param {string} options.timeRange - 时间范围，如 "1h", "30m", "1d"
   * @param {number} options.minDurationMs - 最小查询时长（毫秒）
   */
  async fetchQueryProfiles(profileList, options = {}) {
    const profiles = {};
    const connection = await mysql.createConnection(this.dbConfig);

    try {
      // 禁用当前 session 的 profile 记录，避免 get_query_profile 查询挤掉用户查询的 profile
      await connection.query('SET enable_profile = false');

      // 1. 先过滤系统查询
      let filteredQueries = this.filterUserQueries(profileList);
      console.error(
        `   Filtered ${profileList.length} queries to ${filteredQueries.length} user queries`,
      );

      // 2. 按时间范围过滤
      const timeRange = options.timeRange || '1h';
      const cutoffTime = this.calculateCutoffTime(timeRange);
      filteredQueries = filteredQueries.filter((item) => {
        if (!item.StartTime) return false;
        const queryTime = new Date(item.StartTime);
        return queryTime >= cutoffTime;
      });
      console.error(
        `   After time filter (${timeRange}): ${filteredQueries.length} queries`,
      );

      // 3. 按最小时长过滤
      const minDurationMs = options.minDurationMs || 100;
      filteredQueries = filteredQueries.filter((item) => {
        const durationMs = this.parseDuration(item.Time);
        return durationMs >= minDurationMs;
      });
      console.error(
        `   After duration filter (>=${minDurationMs}ms): ${filteredQueries.length} queries`,
      );

      // 获取所有符合条件的查询的 profile
      for (const item of filteredQueries) {
        const queryId = item.QueryId;
        if (!queryId) continue;

        try {
          console.error(`   Fetching profile for query: ${queryId}`);
          const [rows] = await connection.query(
            `SELECT get_query_profile('${queryId}') as profile`,
          );
          if (rows && rows[0] && rows[0].profile) {
            profiles[queryId] = {
              profile: rows[0].profile,
              startTime: item.StartTime,
              duration: item.Time,
              state: item.State,
              statement: item.Statement || '',
            };
          }
        } catch (error) {
          console.error(
            `   Failed to fetch profile for ${queryId}: ${error.message}`,
          );
          profiles[queryId] = { error: error.message };
        }
      }
    } finally {
      await connection.end();
    }

    return profiles;
  }

  /**
   * 根据时间范围计算截止时间
   * @param {string} timeRange - 时间范围，如 "1h", "30m", "1d"
   * @returns {Date} 截止时间
   */
  calculateCutoffTime(timeRange) {
    const now = new Date();
    const match = timeRange.match(/^(\d+)([hmd])$/);
    if (!match) {
      // 默认 1 小时
      return new Date(now.getTime() - 60 * 60 * 1000);
    }

    const value = parseInt(match[1], 10);
    const unit = match[2];

    let milliseconds;
    switch (unit) {
      case 'm':
        milliseconds = value * 60 * 1000;
        break;
      case 'h':
        milliseconds = value * 60 * 60 * 1000;
        break;
      case 'd':
        milliseconds = value * 24 * 60 * 60 * 1000;
        break;
      default:
        milliseconds = 60 * 60 * 1000;
    }

    return new Date(now.getTime() - milliseconds);
  }

  /**
   * 解析时长字符串为毫秒
   * @param {string} duration - 时长字符串，如 "5s489ms", "831ms", "9s139ms"
   * @returns {number} 毫秒数
   */
  parseDuration(duration) {
    if (!duration) return 0;

    let totalMs = 0;

    // 匹配秒
    const secMatch = duration.match(/(\d+)s/);
    if (secMatch) {
      totalMs += parseInt(secMatch[1], 10) * 1000;
    }

    // 匹配毫秒
    const msMatch = duration.match(/(\d+)ms/);
    if (msMatch) {
      totalMs += parseInt(msMatch[1], 10);
    }

    // 匹配分钟
    const minMatch = duration.match(/(\d+)m(?!s)/);
    if (minMatch) {
      totalMs += parseInt(minMatch[1], 10) * 60 * 1000;
    }

    return totalMs;
  }

  /**
   * 从 profile 数据中提取有 cache miss 的表名
   * 只提取 CompressedBytesReadRemote > 0 或 IOCountRemote > 0 的表
   */
  extractTableNamesFromProfiles(queryProfiles) {
    const tableNames = new Set();

    for (const [, profileData] of Object.entries(queryProfiles)) {
      if (profileData.error || !profileData.profile) continue;

      // 提取每个表及其对应的 cache 指标
      const tablesWithCacheMiss = this.extractTablesWithCacheMiss(
        profileData.profile,
      );
      for (const tableName of tablesWithCacheMiss) {
        tableNames.add(tableName);
      }
    }

    return tableNames;
  }

  /**
   * 从单个 profile 中提取有 cache miss 的表
   * 解析 IOStatistics 块中的 CompressedBytesReadRemote 和 IOCountRemote
   */
  extractTablesWithCacheMiss(profileText) {
    const tablesWithCacheMiss = [];
    const lines = profileText.split('\n');
    let currentTable = null;
    let inIOStatistics = false;
    let currentTableHasCacheMiss = false;

    for (const line of lines) {
      // 检测 Table: xxx
      const tableMatch = line.match(/-\s*Table:\s*(\S+)/);
      if (tableMatch) {
        // 保存上一个表的结果
        if (
          currentTable &&
          currentTableHasCacheMiss &&
          !tablesWithCacheMiss.includes(currentTable)
        ) {
          tablesWithCacheMiss.push(currentTable);
        }
        currentTable = tableMatch[1].trim();
        inIOStatistics = false;
        currentTableHasCacheMiss = false;
        continue;
      }

      // 检测是否进入 IOStatistics 块
      if (line.includes('- IOStatistics:')) {
        inIOStatistics = true;
        continue;
      }

      // 在 IOStatistics 块内检查 cache miss
      if (currentTable && inIOStatistics) {
        // CompressedBytesReadRemote > 0
        const remoteBytesMatch = line.match(
          /CompressedBytesReadRemote:\s*([\d.]+)\s*([KMGTP]?B)/i,
        );
        if (remoteBytesMatch) {
          const value = parseFloat(remoteBytesMatch[1]);
          if (value > 0) currentTableHasCacheMiss = true;
        }

        // IOCountRemote > 0
        const remoteIOMatch = line.match(/IOCountRemote:\s*([\d.,]+)/i);
        if (remoteIOMatch) {
          const value = parseInt(remoteIOMatch[1].replace(/,/g, ''), 10);
          if (value > 0) currentTableHasCacheMiss = true;
        }
      }
    }

    // 保存最后一个表的结果
    if (
      currentTable &&
      currentTableHasCacheMiss &&
      !tablesWithCacheMiss.includes(currentTable)
    ) {
      tablesWithCacheMiss.push(currentTable);
    }

    return tablesWithCacheMiss;
  }

  /**
   * 获取表的 schema 信息，检查 data_cache.enable 属性
   */
  async fetchTableSchemas(tableNames) {
    const schemas = {};
    const connection = await mysql.createConnection(this.dbConfig);

    try {
      // 禁用当前 session 的 profile 记录
      await connection.query('SET enable_profile = false');

      for (const fullTableName of tableNames) {
        const [dbName, tableName] = fullTableName.split('.');
        if (!dbName || !tableName) continue;

        try {
          const [rows] = await connection.query(
            `SHOW CREATE TABLE ${dbName}.${tableName}`,
          );
          if (rows && rows[0]) {
            const createStatement =
              rows[0]['Create Table'] || rows[0]['create_statement'] || '';
            schemas[fullTableName] = {
              create_statement: createStatement,
              data_cache_enabled: this.checkDataCacheEnabled(createStatement),
            };
          }
        } catch (error) {
          console.error(
            `   Failed to fetch schema for ${fullTableName}: ${error.message}`,
          );
          schemas[fullTableName] = { error: error.message };
        }
      }
    } finally {
      await connection.end();
    }

    return schemas;
  }

  /**
   * 检查建表语句中 data_cache.enable 是否为 true
   */
  checkDataCacheEnabled(createStatement) {
    if (!createStatement) return null;

    // 检查 "datacache.enable" = "false" 或 'datacache.enable' = 'false'
    const disabledMatch = createStatement.match(
      /["']datacache\.enable["']\s*=\s*["']false["']/i,
    );
    if (disabledMatch) {
      return false;
    }

    // 检查 "datacache.enable" = "true" 或存在 datacache 相关配置
    const enabledMatch = createStatement.match(
      /["']datacache\.enable["']\s*=\s*["']true["']/i,
    );
    if (enabledMatch) {
      return true;
    }

    // 默认为开启（如果没有显式设置）
    return null;
  }

  /**
   * 过滤出真正的用户查询，排除系统查询
   */
  filterUserQueries(profileList) {
    const systemPatterns = [
      /^\s*select\s+last_query_id\s*\(/i,
      /^\s*select\s+get_query_profile\s*\(/i,
      /^\s*select\s+@@/i,
      /^\s*show\s+/i,
      /^\s*admin\s+show\s+/i,
      /^\s*desc\s+/i,
      /^\s*describe\s+/i,
      /^\s*explain\s+/i,
      /^\s*set\s+/i,
      /^\s*use\s+/i,
      /information_schema/i,
      /_statistics_/i,
      /^\s*select\s+version\s*\(\)/i,
      /^\s*select\s+current_user\s*\(\)/i,
      /^\s*select\s+database\s*\(\)/i,
      /^\s*select\s+connection_id\s*\(\)/i,
    ];

    return profileList.filter((item) => {
      const sql = (item.Statement || '').trim();
      if (!sql) return false;

      for (const pattern of systemPatterns) {
        if (pattern.test(sql)) {
          return false;
        }
      }

      // 处理 SQL 中的换行符，将其替换为空格再检查
      const sqlNormalized = sql.toLowerCase().replace(/\n/g, ' ');
      // 排除没有 FROM 子句的纯 SELECT 语句（如 select 1+1, select @@var）
      if (
        sqlNormalized.startsWith('select') &&
        !sqlNormalized.includes(' from ')
      ) {
        return false;
      }

      return true;
    });
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

        // 检查是否需要两阶段 profile 获取
        const metaQuery = queryDef.queries.find(
          (q) => q.type === 'meta' && q.requires_profile_fetch,
        );
        const regularQueries = queryDef.queries.filter(
          (q) => q.type !== 'meta',
        );

        // 2. 执行 SQL（如果有的话）
        if (regularQueries.length > 0) {
          console.error('   Step 2: Executing SQL queries locally...');
          results = await this.executeQueries(regularQueries);
          console.error('   SQL execution completed');
        } else {
          console.error(
            '   Step 2: No SQL queries to execute (args-only tool)',
          );
        }

        // 2.5 如果需要获取详细 profile，执行第二阶段查询
        if (
          metaQuery &&
          results.profile_list &&
          Array.isArray(results.profile_list)
        ) {
          console.error(
            '   Step 2.5: Fetching detailed profiles for each query...',
          );
          const fetchOptions = {
            timeRange: metaQuery.time_range || '1h',
            minDurationMs: metaQuery.min_duration_ms || 100,
          };
          results.query_profiles = await this.fetchQueryProfiles(
            results.profile_list,
            fetchOptions,
          );
          console.error(
            `   Fetched ${Object.keys(results.query_profiles).length} query profiles`,
          );

          // 2.6 如果需要获取表 schema，从 profile 中提取表名并查询
          if (metaQuery.requires_table_schema_fetch) {
            console.error(
              '   Step 2.6: Fetching table schemas for cache miss analysis...',
            );
            const tableNames = this.extractTableNamesFromProfiles(
              results.query_profiles,
            );
            console.error(
              `   Found ${tableNames.size} unique tables: ${[...tableNames].slice(0, 5).join(', ')}${tableNames.size > 5 ? '...' : ''}`,
            );
            if (tableNames.size > 0) {
              results.table_schemas = await this.fetchTableSchemas(tableNames);
              console.error(
                `   Fetched schemas for ${Object.keys(results.table_schemas).length} tables`,
              );
            }
          }
        }

        // 3. 发送给 API 分析（支持多阶段查询）
        console.error(
          '   Step 3: Sending results to Central API for analysis...',
        );
        let analysis = await this.analyzeResultsWithAPI(
          toolName,
          results,
          processedArgs,
        );

        // 3.5 处理多阶段查询（如存储放大分析的 schema 检测）
        let phaseCount = 1;
        const maxPhases = 5; // 防止无限循环
        while (analysis.status === 'needs_more_queries' && phaseCount < maxPhases) {
          phaseCount++;
          console.error(`   Step 3.${phaseCount}: Multi-phase query detected (${analysis.phase})`);
          console.error(`   Message: ${analysis.message}`);

          // 检查是否需要执行 CLI 命令
          if (analysis.requires_cli_execution && analysis.cli_commands) {
            console.error(`   Executing ${analysis.cli_commands.length} CLI commands...`);
            const cliResults = await this.executeCliCommands(analysis.cli_commands);

            // 根据 phase 使用不同的结果键名
            if (analysis.phase === 'list_table_directories') {
              results.dir_listing_results = cliResults.cli_results;
              results.dir_listing_summary = cliResults.cli_summary;
              console.error(`   Directory listing completed: ${cliResults.cli_summary.successful} success, ${cliResults.cli_summary.failed} failed`);
            } else if (analysis.phase === 'get_garbage_sizes') {
              results.garbage_size_results = cliResults.cli_results;
              results.garbage_size_summary = cliResults.cli_summary;
              console.error(`   Garbage size query completed: ${cliResults.cli_summary.successful} success, ${cliResults.cli_summary.failed} failed`);
            } else {
              // 默认使用 cli_results/cli_summary
              results = { ...results, ...cliResults };
            }
          }

          // 执行下一阶段的 SQL 查询
          if (analysis.next_queries && analysis.next_queries.length > 0) {
            console.error(`   Executing ${analysis.next_queries.length} additional queries...`);
            const additionalResults = await this.executeQueries(analysis.next_queries);

            // 特殊处理 desc_storage_volumes phase：将 desc_volume_<name> 结果转换为 storage_volume_details 格式
            if (analysis.phase === 'desc_storage_volumes') {
              const storageVolumeDetails = {};
              for (const [key, value] of Object.entries(additionalResults)) {
                if (key.startsWith('desc_volume_')) {
                  const volumeName = key.replace('desc_volume_', '');
                  storageVolumeDetails[volumeName] = value;
                }
              }
              if (Object.keys(storageVolumeDetails).length > 0) {
                results.storage_volume_details = storageVolumeDetails;
                console.error(`   Converted ${Object.keys(storageVolumeDetails).length} volume details to storage_volume_details format`);
              }
            } else {
              results = { ...results, ...additionalResults };
            }
          }

          // 使用更新后的参数再次调用分析 API
          const nextArgs = analysis.next_args || processedArgs;
          console.error(`   Re-analyzing with updated args...`);
          analysis = await this.analyzeResultsWithAPI(toolName, results, nextArgs);
        }

        if (phaseCount >= maxPhases) {
          console.error('   Warning: Max phases reached, analysis may be incomplete');
        }

        // 显示分析方式（便于用户确认是否使用了 CLI 扫描）
        if (analysis.calculation_method) {
          const methodNames = {
            'object_storage_cli': '对象存储 CLI 扫描',
            'direct_query': '直接查询 STORAGE_SIZE',
            'cli_fallback': 'CLI 回退模式'
          };
          const methodName = methodNames[analysis.calculation_method] || analysis.calculation_method;
          console.error(`   📊 数据获取方式: ${methodName}`);

          if (analysis.cli_execution_summary) {
            const s = analysis.cli_execution_summary;
            console.error(`   📈 CLI 执行统计: 总计 ${s.total}, 成功 ${s.successful}, 失败 ${s.failed}, 耗时 ${s.execution_time_ms}ms`);
          }
        }
        console.error('   Analysis completed\n');

        // 4. 格式化报告
        const report = this.formatAnalysisReport(analysis);

        // 对于 HTML 报告，写入文件并移除大内容避免传输阻塞
        const analysisForJson = { ...analysis };
        if (analysis.html_content && analysis.output_path) {
          try {
            fs.writeFileSync(
              analysis.output_path,
              analysis.html_content,
              'utf-8',
            );
            console.error(`   HTML report written to: ${analysis.output_path}`);
          } catch (writeErr) {
            console.error(
              `   Failed to write HTML report: ${writeErr.message}`,
            );
          }
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
