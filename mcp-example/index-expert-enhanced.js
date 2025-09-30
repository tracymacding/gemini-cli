#!/usr/bin/env node

/**
 * StarRocks MCP Server - Expert Enhanced Version
 * 集成多专家系统的增强版本
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import mysql from 'mysql2/promise';

// 导入专家系统
import { StarRocksExpertCoordinator } from './experts/expert-coordinator.js';

class StarRocksMcpServerExpert {
  constructor() {
    this.server = new Server(
      {
        name: 'starrocks-mcp-server-expert',
        version: '2.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    // 初始化专家协调器
    this.expertCoordinator = new StarRocksExpertCoordinator();

    this.setupHandlers();
  }

  setupHandlers() {
    // List available tools
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          // === 原有工具 (保持兼容性) ===
          {
            name: 'get_starrocks_backends',
            description: 'Checks the status of all StarRocks Backend (BE/Compute Node) nodes and returns their liveness and other details.',
            inputSchema: {
              type: 'object',
              properties: {},
              required: []
            }
          },
          // DISABLED: smart_health_check - 已注释以简化工具选择
          /*
          {
            name: 'smart_health_check',
            description: '🧠 **系统全面健康检查** - 执行跨模块的综合诊断，包括存储、压缩、导入等所有子系统的整体状况分析。适用于系统整体评估，不适用于查找特定表或分区的问题。',
            inputSchema: {
              type: 'object',
              properties: {
                include_recommendations: {
                  type: 'boolean',
                  description: 'Whether to include detailed optimization recommendations (default: true)',
                  default: true
                }
              },
              required: []
            }
          },
          */

          // === 新的专家系统工具 ===
          {
            name: 'expert_analysis',
            description: '🎯 多专家协调分析 - 执行存储、Compaction等专家模块的深度诊断和跨模块影响分析',
            inputSchema: {
              type: 'object',
              properties: {
                expert_scope: {
                  type: 'array',
                  items: {
                    type: 'string',
                    enum: ['storage', 'compaction']
                  },
                  description: '指定要执行的专家分析范围 (默认: 全部专家)',
                  default: ['storage', 'compaction']
                },
                include_details: {
                  type: 'boolean',
                  description: '是否包含详细的原始数据 (默认: false)',
                  default: false
                },
                include_cross_analysis: {
                  type: 'boolean',
                  description: '是否执行跨模块影响分析 (默认: true)',
                  default: true
                }
              },
              required: []
            }
          },
          {
            name: 'storage_expert_analysis',
            description: '💾 存储专家深度分析 - 专门分析磁盘使用、Tablet健康、数据分布等存储相关问题',
            inputSchema: {
              type: 'object',
              properties: {
                include_details: {
                  type: 'boolean',
                  description: '是否包含详细分析数据',
                  default: true
                }
              },
              required: []
            }
          },
          {
            name: 'compaction_expert_analysis',
            description: '🗜️ **Compaction 系统深度分析** - 全面诊断压缩系统状态，包括整体性能评估、配置优化建议、任务执行分析等。适用于了解 Compaction 系统整体状况，不适用于查找特定高分数表。',
            inputSchema: {
              type: 'object',
              properties: {
                include_details: {
                  type: 'boolean',
                  description: '是否包含详细分析数据',
                  default: true
                }
              },
              required: []
            }
          },
          {
            name: 'import_expert_analysis',
            description: '📥 专门分析**系统导入问题**的专家工具 - 深度诊断数据导入错误、Stream Load性能问题、导入频率异常、导入失败原因等所有与数据导入相关的问题。当用户询问"导入问题"、"导入失败"、"数据导入"时优先使用此工具。',
            inputSchema: {
              type: 'object',
              properties: {
                include_details: {
                  type: 'boolean',
                  description: '是否包含详细分析数据',
                  default: true
                }
              },
              required: []
            }
          },
          {
            name: 'analyze_table_import_frequency',
            description: '🔍 表级导入频率分析 - 深度分析指定表的导入模式、性能和频率特征',
            inputSchema: {
              type: 'object',
              properties: {
                database_name: {
                  type: 'string',
                  description: '数据库名称'
                },
                table_name: {
                  type: 'string',
                  description: '表名称'
                },
                include_details: {
                  type: 'boolean',
                  description: '是否包含详细的时间分布数据',
                  default: true
                }
              },
              required: ['database_name', 'table_name']
            }
          },
          {
            name: 'get_available_experts',
            description: '📋 获取可用专家列表和能力说明',
            inputSchema: {
              type: 'object',
              properties: {},
              required: []
            }
          },

          // === Compaction 专家集成工具 ===
          {
            name: 'get_table_partitions_compaction_score',
            description: 'Retrieves compaction scores for all partitions of a specific table, with optional filtering and sorting.',
            inputSchema: {
              type: 'object',
              properties: {
                database: {
                  type: 'string',
                  description: 'The database name'
                },
                table: {
                  type: 'string',
                  description: 'The table name'
                },
                score_threshold: {
                  type: 'number',
                  description: 'Only show partitions with CS >= this threshold (optional)',
                  default: 0
                }
              },
              required: ['database', 'table']
            }
          },
          // DISABLED: get_high_compaction_partitions - 已注释以简化工具选择
          {
            name: 'get_high_compaction_partitions',
            description: '🔍 **查找高 Compaction Score 的表/分区** - 快速定位系统中 compaction score 最高的表和分区。当用户询问"哪些表 compaction score 高"、"找出需要压缩的表"、"compaction score 排行"时使用此工具。',
            inputSchema: {
              type: 'object',
              properties: {
                limit: {
                  type: 'number',
                  description: 'Maximum number of high-CS partitions to return',
                  default: 10
                },
                min_score: {
                  type: 'number',
                  description: 'Minimum compaction score threshold',
                  default: 100
                }
              },
              required: []
            }
          },
          {
            name: 'get_compaction_threads',
            description: 'Checks the current compaction thread configuration on all BE nodes.',
            inputSchema: {
              type: 'object',
              properties: {},
              required: []
            }
          },
          {
            name: 'set_compaction_threads',
            description: 'Sets the number of compaction threads on all BE nodes.',
            inputSchema: {
              type: 'object',
              properties: {
                thread_count: {
                  type: 'number',
                  description: 'The number of compaction threads to set (recommended: 2-8 based on CPU cores)'
                }
              },
              required: ['thread_count']
            }
          },
          {
            name: 'get_running_compaction_tasks',
            description: 'Retrieves information about currently running compaction tasks.',
            inputSchema: {
              type: 'object',
              properties: {
                include_details: {
                  type: 'boolean',
                  description: 'Whether to include detailed task information',
                  default: true
                }
              },
              required: []
            }
          },
          {
            name: 'analyze_high_compaction_score',
            description: '📊 **分析高 Compaction Score 原因** - 深入分析为什么某些表/分区有高 compaction score，提供根因分析和优化建议。注意：此工具不是用于查找高分数表，如需查找请使用 get_high_compaction_partitions 工具。',
            inputSchema: {
              type: 'object',
              properties: {
                target_database: {
                  type: 'string',
                  description: 'Focus analysis on specific database (optional)'
                },
                min_score: {
                  type: 'number',
                  description: 'Minimum compaction score to analyze',
                  default: 100
                }
              },
              required: []
            }
          },
          {
            name: 'compact_partition',
            description: 'Manually triggers compaction for a specific partition to reduce its compaction score.',
            inputSchema: {
              type: 'object',
              properties: {
                database: {
                  type: 'string',
                  description: 'The database name'
                },
                table: {
                  type: 'string',
                  description: 'The table name'
                },
                partition: {
                  type: 'string',
                  description: 'The partition name to compact'
                }
              },
              required: ['database', 'table', 'partition']
            }
          }
        ]
      };
    });

    // Execute tool calls
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        // === 新的专家系统工具处理 ===
        if (name === 'expert_analysis') {
          return this.handleExpertAnalysis(args);
        } else if (name === 'storage_expert_analysis') {
          return this.handleStorageExpertAnalysis(args);
        } else if (name === 'compaction_expert_analysis') {
          return this.handleCompactionExpertAnalysis(args);
        } else if (name === 'import_expert_analysis') {
          return this.handleImportExpertAnalysis(args);
        } else if (name === 'analyze_table_import_frequency') {
          return this.handleAnalyzeTableImportFrequency(args);
        } else if (name === 'get_available_experts') {
          return this.handleGetAvailableExperts(args);

        // === Compaction 专家集成工具处理 ===
        } else if (name === 'get_table_partitions_compaction_score') {
          return this.handleGetTablePartitionsCompactionScore(args);
        } else if (name === 'get_high_compaction_partitions') {
          return this.handleGetHighCompactionPartitions(args);
        } else if (name === 'get_compaction_threads') {
          return this.handleGetCompactionThreads(args);
        } else if (name === 'set_compaction_threads') {
          return this.handleSetCompactionThreads(args);
        } else if (name === 'get_running_compaction_tasks') {
          return this.handleGetRunningCompactionTasks(args);
        } else if (name === 'analyze_high_compaction_score') {
          return this.handleAnalyzeHighCompactionScore(args);

        // === 原有工具处理 (保持兼容) ===
        } else if (name === 'get_starrocks_backends') {
          return this.handleGetStarrocksBackends(args);
        // DISABLED: smart_health_check
        /*
        } else if (name === 'smart_health_check') {
          return this.handleSmartHealthCheck(args);
        */
        } else if (name === 'compact_partition') {
          return this.handleCompactPartition(args);
        } else {
          throw new McpError(
            ErrorCode.MethodNotFound,
            `Unknown tool: ${name}`
          );
        }
      } catch (error) {
        if (error instanceof McpError) {
          throw error;
        }
        throw new McpError(
          ErrorCode.InternalError,
          `Tool execution failed: ${error.message}`
        );
      }
    });
  }

  /**
   * 多专家协调分析
   */
  async handleExpertAnalysis(args) {
    const connection = await this.createConnection();

    try {
      const options = {
        includeDetails: args.include_details || false,
        expertScope: args.expert_scope || ['storage', 'compaction'],
        includeCrossAnalysis: args.include_cross_analysis !== false
      };

      console.error('🚀 启动多专家协调分析...');
      const analysis = await this.expertCoordinator.performCoordinatedAnalysis(connection, options);

      const report = this.formatExpertAnalysisReport(analysis);

      return {
        content: [
          {
            type: 'text',
            text: report
          },
          {
            type: 'text',
            text: JSON.stringify(analysis, null, 2)
          }
        ]
      };

    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * 存储专家单独分析
   */
  async handleStorageExpertAnalysis(args) {
    const connection = await this.createConnection();

    try {
      const storageExpert = this.expertCoordinator.experts.storage;
      const result = await storageExpert.diagnose(connection, args.include_details);

      const report = this.formatSingleExpertReport(result, 'storage');

      return {
        content: [
          {
            type: 'text',
            text: report
          },
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };

    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Compaction专家单独分析
   */
  async handleCompactionExpertAnalysis(args) {
    const connection = await this.createConnection();

    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.diagnose(connection, args.include_details);

      const report = this.formatSingleExpertReport(result, 'compaction');

      return {
        content: [
          {
            type: 'text',
            text: report
          },
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };

    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Import专家单独分析
   */
  async handleImportExpertAnalysis(args) {
    const connection = await this.createConnection();

    try {
      const importExpert = this.expertCoordinator.experts.import;
      const result = await importExpert.diagnose(connection, args.include_details);

      const report = this.formatSingleExpertReport(result, 'import');

      return {
        content: [
          {
            type: 'text',
            text: report
          },
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };

    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * 分析表级导入频率
   */
  async handleAnalyzeTableImportFrequency(args) {
    const connection = await this.createConnection();

    try {
      const importExpert = this.expertCoordinator.experts.import;
      const result = await importExpert.analyzeTableImportFrequency(
        connection,
        args.database_name,
        args.table_name,
        args.include_details !== false
      );

      let report;
      if (result.status === 'completed') {
        report = importExpert.formatTableFrequencyReport(result);
      } else {
        report = `❌ 表 ${args.database_name}.${args.table_name} 导入频率分析失败\n`;
        report += `状态: ${result.status}\n`;
        report += `原因: ${result.error || result.message}\n`;
        report += `耗时: ${result.analysis_duration_ms}ms`;
      }

      return {
        content: [
          {
            type: 'text',
            text: report
          },
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };

    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * 获取可用专家列表
   */
  async handleGetAvailableExperts(args) {
    const experts = this.expertCoordinator.getAvailableExperts();

    let report = '🧠 StarRocks 专家系统 - 可用专家列表\n';
    report += '=========================================\n\n';

    experts.forEach((expert, index) => {
      const emoji = expert.name === 'storage' ? '💾' :
                   expert.name === 'compaction' ? '🗜️' :
                   expert.name === 'import' ? '📥' : '🔧';
      report += `${emoji} **${expert.name.toUpperCase()} 专家**\n`;
      report += `   版本: ${expert.version}\n`;
      report += `   职责: ${expert.description}\n\n`;
    });

    report += '💡 **使用方法:**\n';
    report += '• `expert_analysis` - 多专家协调分析（推荐）\n';
    report += '• `storage_expert_analysis` - 单独存储分析\n';
    report += '• `compaction_expert_analysis` - 单独Compaction分析\n';
    report += '• `import_expert_analysis` - 单独Import分析\n';
    report += '• `analyze_table_import_frequency` - 表级导入频率深度分析\n';

    return {
      content: [
        {
          type: 'text',
          text: report
        },
        {
          type: 'text',
          text: JSON.stringify({ available_experts: experts }, null, 2)
        }
      ]
    };
  }

  /**
   * 格式化多专家分析报告
   */
  formatExpertAnalysisReport(analysis) {
    const assessment = analysis.comprehensive_assessment;
    const metadata = analysis.analysis_metadata;

    let report = '🎯 StarRocks 多专家协调分析报告\n';
    report += '=====================================\n\n';

    // 综合评估
    const healthEmoji = assessment.health_level === 'EXCELLENT' ? '🟢' :
                       assessment.health_level === 'GOOD' ? '🟡' :
                       assessment.health_level === 'FAIR' ? '🟠' : '🔴';

    report += `${healthEmoji} **综合健康评估**: ${assessment.overall_health_score}/100 (${assessment.health_level})\n`;
    report += `📊 **系统状态**: ${assessment.overall_status}\n`;
    report += `🔍 **分析范围**: ${metadata.experts_count}个专家模块\n`;
    report += `⚠️ **发现问题**: ${metadata.total_issues_found}个\n`;

    if (metadata.cross_impacts_found > 0) {
      report += `🔄 **跨模块影响**: ${metadata.cross_impacts_found}个\n`;
    }

    report += `\n${assessment.summary}\n\n`;

    // 各专家健康状态
    report += '📋 **各专家模块状态**:\n';
    Object.entries(assessment.expert_scores).forEach(([expertName, scores]) => {
      const emoji = expertName === 'storage' ? '💾' :
                   expertName === 'compaction' ? '🗜️' : '🔧';
      const statusEmoji = scores.status === 'HEALTHY' ? '✅' :
                         scores.status === 'WARNING' ? '⚠️' : '🚨';

      report += `  ${emoji} ${expertName.toUpperCase()}: ${scores.score}/100 ${statusEmoji}\n`;
    });

    // 风险评估
    if (assessment.system_risk_assessment.total_risks > 0) {
      report += '\n🔥 **系统风险评估**:\n';
      report += `  • 风险等级: ${assessment.system_risk_assessment.overall_risk_level}\n`;
      report += `  • 风险项目: ${assessment.system_risk_assessment.total_risks}个\n`;
    }

    // 跨模块影响
    if (analysis.cross_module_analysis && analysis.cross_module_analysis.impacts.length > 0) {
      report += '\n🔗 **跨模块影响分析**:\n';
      analysis.cross_module_analysis.impacts.forEach(impact => {
        report += `  • ${impact.explanation} [${impact.impact_level}]\n`;
      });
    }

    // 优化建议
    if (analysis.prioritized_recommendations.length > 0) {
      report += '\n💡 **优化建议** (按优先级排序):\n';
      analysis.prioritized_recommendations.slice(0, 5).forEach(rec => {
        const coordNote = rec.coordination_notes ? ' 🔄' : '';
        report += `  ${rec.execution_order}. [${rec.priority}] ${rec.title}${coordNote}\n`;
        if (rec.source_expert === 'coordinator') {
          report += `     ↳ 跨模块协调建议\n`;
        }
      });

      if (analysis.prioritized_recommendations.length > 5) {
        report += `  ... 还有${analysis.prioritized_recommendations.length - 5}个建议，请查看详细JSON输出\n`;
      }
    }

    report += '\n📋 详细分析数据请查看JSON输出部分';

    return report;
  }

  /**
   * 格式化单专家报告
   */
  formatSingleExpertReport(result, expertType) {
    const emoji = expertType === 'storage' ? '💾' :
                 expertType === 'compaction' ? '🗜️' :
                 expertType === 'import' ? '📥' : '🔧';
    const healthKey = expertType === 'storage' ? 'storage_health' :
                     expertType === 'compaction' ? 'compaction_health' :
                     expertType === 'import' ? 'import_health' : 'system_health';
    const health = result[healthKey];

    let report = `${emoji} StarRocks ${expertType.toUpperCase()} 专家分析报告\n`;
    report += '=====================================\n\n';

    const healthEmoji = health.level === 'EXCELLENT' ? '🟢' :
                       health.level === 'GOOD' ? '🟡' :
                       health.level === 'FAIR' ? '🟠' : '🔴';

    report += `${healthEmoji} **${expertType}健康分数**: ${health.score}/100 (${health.level})\n`;
    report += `📊 **状态**: ${health.status}\n`;
    report += `⏱️ **分析耗时**: ${result.analysis_duration_ms}ms\n\n`;

    const diagnosis = result.diagnosis_results;

    // 问题摘要
    report += `📋 **问题摘要**: ${diagnosis.summary}\n`;
    report += `🔍 **问题统计**: ${diagnosis.total_issues}个 (严重: ${diagnosis.criticals.length}, 警告: ${diagnosis.warnings.length})\n\n`;

    // 严重问题
    if (diagnosis.criticals.length > 0) {
      report += '🚨 **严重问题**:\n';
      diagnosis.criticals.forEach(issue => {
        report += `  • ${issue.message} [${issue.urgency}]\n`;
        if (issue.impact) {
          report += `    影响: ${issue.impact}\n`;
        }
      });
      report += '\n';
    }

    // 警告问题
    if (diagnosis.warnings.length > 0) {
      report += '⚠️ **警告问题**:\n';
      diagnosis.warnings.slice(0, 3).forEach(warning => {
        report += `  • ${warning.message}\n`;
      });
      if (diagnosis.warnings.length > 3) {
        report += `  ... 还有${diagnosis.warnings.length - 3}个警告\n`;
      }
      report += '\n';
    }

    // 专业建议
    if (result.professional_recommendations && result.professional_recommendations.length > 0) {
      report += '💡 **专业建议**:\n';
      result.professional_recommendations.slice(0, 3).forEach((rec, index) => {
        report += `  ${index + 1}. [${rec.priority}] ${rec.title}\n`;
      });
      if (result.professional_recommendations.length > 3) {
        report += `  ... 还有${result.professional_recommendations.length - 3}个建议\n`;
      }
      report += '\n';
    }

    report += '📋 详细诊断数据请查看JSON输出部分';

    return report;
  }

  /**
   * 创建数据库连接
   */
  async createConnection() {
    const dbConfig = {
      host: process.env.SR_HOST,
      user: process.env.SR_USER,
      password: process.env.SR_PASSWORD,
      database: process.env.SR_DATABASE || 'information_schema',
      port: process.env.SR_PORT || 9030,
    };

    if (!dbConfig.host || !dbConfig.user || dbConfig.password === undefined) {
      throw new McpError(ErrorCode.InvalidParams, 'Missing StarRocks connection details. Please set SR_HOST, SR_USER, and SR_PASSWORD environment variables.');
    }

    return await mysql.createConnection(dbConfig);
  }

  // === 保留原有方法 (简化版本，保持兼容性) ===

  async handleGetStarrocksBackends(args) {
    const connection = await this.createConnection();

    try {
      const [rows] = await connection.query('SHOW BACKENDS;');
      const backends = rows.map(row => ({
        backendId: row.BackendId,
        ip: row.IP,
        isAlive: row.Alive,
        lastHeartbeat: row.LastHeartbeat,
        diskUsedPercent: row.MaxDiskUsedPct,
        memUsedPercent: row.MemUsedPct,
      }));

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(backends, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  // DISABLED: handleSmartHealthCheck - 已注释
  /*
  async handleSmartHealthCheck(args) {
    // 使用多专家分析作为智能健康检查的后端
    return this.handleExpertAnalysis({
      include_details: false,
      expert_scope: ['storage', 'compaction'],
      include_cross_analysis: true
    });
  }
  */

  /**
   * === Compaction 专家集成工具实现 ===
   * 委托给 Compaction 专家处理
   */

  async handleGetTablePartitionsCompactionScore(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;

      // 使用 collectTableSpecificData 方法来获取表分区的 compaction score
      const data = {};
      await compactionExpert.collectTableSpecificData(connection, data, {
        targetDatabase: args.database,
        targetTable: args.table
      });

      // 提取并过滤分区数据
      const partitions = data.target_table_analysis?.partitions || [];
      const scoreThreshold = args.score_threshold || 0;

      const filteredPartitions = partitions.filter(partition =>
        partition.max_cs >= scoreThreshold
      );

      const result = {
        database: args.database,
        table: args.table,
        score_threshold: scoreThreshold,
        total_partitions: partitions.length,
        filtered_partitions: filteredPartitions.length,
        partitions: filteredPartitions.map(partition => ({
          partition_name: partition.partition,
          max_compaction_score: partition.max_cs,
          avg_compaction_score: partition.avg_cs,
          p50_compaction_score: partition.p50_cs,
          row_count: partition.row_count,
          data_size: partition.data_size,
          storage_size: partition.storage_size,
          buckets: partition.buckets,
          replication_num: partition.replication_num
        }))
      };

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleGetHighCompactionPartitions(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.getHighCompactionPartitions(
        connection, args.limit, args.min_score
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleGetCompactionThreads(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.getCompactionThreads(connection);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleSetCompactionThreads(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.setCompactionThreads(connection, args.thread_count);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleGetRunningCompactionTasks(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.getRunningCompactionTasks(
        connection, args.include_details
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleAnalyzeHighCompactionScore(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.analyzeHighCompactionScore(
        connection, args.target_database, args.min_score
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async handleCompactPartition(args) {
    const connection = await this.createConnection();
    try {
      const compactionExpert = this.expertCoordinator.experts.compaction;
      const result = await compactionExpert.compactPartition(
        connection, args.database, args.table, args.partition
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } finally {
      await connection.end();
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
  }
}

const server = new StarRocksMcpServerExpert();
server.run().catch(console.error);