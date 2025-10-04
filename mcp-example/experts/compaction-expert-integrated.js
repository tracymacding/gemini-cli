/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * StarRocks Compaction 专家模块 - 完整集成版
 * 集成了MCP server中所有Compaction相关的现有功能
 */

/* eslint-disable no-undef, @typescript-eslint/no-unused-vars */

import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import { detectArchitectureType } from './common-utils.js';

class StarRocksCompactionExpert {
  constructor() {
    this.name = 'compaction';
    // 不设置 this.version 和 this.description，因为下面有 getter 方法

    // Compaction专业知识规则库
    this.rules = {
      // Compaction Score 分级规则
      compaction_score: {
        excellent: 10, // CS < 10 优秀
        normal: 50, // CS < 50 正常
        warning: 100, // CS >= 100 警告
        critical: 500, // CS >= 500 严重
        emergency: 1000, // CS >= 1000 紧急
      },

      // 线程配置规则
      thread_config: {
        min_threads_per_core: 0.25, // 最少线程数/CPU核心
        max_threads_per_core: 0.5, // 最多线程数/CPU核心
        absolute_min_threads: 4, // 绝对最小线程数
        absolute_max_threads: 64, // 绝对最大线程数
        recommended_base: 8, // 推荐基础线程数
      },

      // 任务执行规则
      task_execution: {
        max_healthy_tasks_per_node: 8, // 单节点健康任务数上限
        task_timeout_hours: 4, // 任务超时时间（小时）
        slow_task_threshold_hours: 2, // 慢任务阈值（小时）
        max_retry_count: 5, // 最大重试次数
        healthy_success_rate: 90, // 健康成功率阈值(%)
      },

      // FE配置规则
      fe_config: {
        lake_compaction_disabled: 0, // 禁用值
        lake_compaction_adaptive: -1, // 自适应值
        min_recommended_max_tasks: 64, // 最小推荐最大任务数
        adaptive_multiplier: 16, // 自适应模式下的倍数（节点数*16）
      },
    };

    // 专业术语解释
    this.terminology = {
      compaction_score:
        'Compaction Score (CS) - 衡量数据文件碎片化程度，分数越高碎片越严重',
      base_compaction: '基础压缩 - 将多个小文件合并成大文件',
      cumulative_compaction: '累积压缩 - 合并增量数据到基础文件',
      lake_compaction_max_tasks: 'FE参数，控制集群最大并发Compaction任务数',
      compact_threads: 'BE参数，控制单个BE节点的Compaction线程数',
    };
  }

  /**
   * 检查集群是否为存算分离架构
   * 如果不是，抛出错误
   */
  async checkSharedDataArchitecture(connection) {
    const archInfo = await detectArchitectureType(connection);

    if (archInfo.type !== 'shared_data') {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `❌ Compaction 专家仅支持存算分离 (Shared-Data) 集群\n\n` +
          `当前集群架构: ${archInfo.type === 'shared_nothing' ? '存算一体 (Shared-Nothing)' : '未知'}\n` +
          `Run Mode: ${archInfo.run_mode || 'N/A'}\n\n` +
          `💡 说明:\n` +
          `  存算分离架构使用云原生存储 (如 S3)，Compaction 由独立的 Compaction 服务管理。\n` +
          `  存算一体架构的 Compaction 机制不同，不适用此专家系统。`,
      );
    }

    return archInfo;
  }

  /**
   * 执行完整的Compaction专家诊断
   */
  async performComprehensiveAnalysis(connection, options = {}) {
    const {
      includeDetailedData = false,
      targetDatabase = null,
      targetTable = null,
      analysisScope = 'full', // 'full', 'quick', 'deep'
    } = options;

    try {
      const startTime = new Date();
      console.error('🗜️ 启动Compaction专家全面分析...');

      // 1. 收集所有Compaction相关数据
      const compactionData = await this.collectAllCompactionData(connection, {
        targetDatabase,
        targetTable,
        includeDetailedData,
      });

      // 2. 执行多维度专业诊断
      const diagnosis = await this.performMultiDimensionalDiagnosis(
        compactionData,
        analysisScope,
      );

      // 3. 生成专业优化建议
      const recommendations = this.generateComprehensiveRecommendations(
        diagnosis,
        compactionData,
      );

      // 4. 计算Compaction系统健康分数
      const healthAssessment = this.calculateCompactionHealth(
        diagnosis,
        compactionData,
      );

      // 5. 生成可执行的操作计划
      const actionPlans = this.generateActionPlans(diagnosis, recommendations);

      const endTime = new Date();
      const analysisTime = endTime - startTime;

      console.error(`✅ Compaction专家分析完成，耗时 ${analysisTime}ms`);

      return {
        expert: this.name,
        version: this.version,
        analysis_timestamp: new Date().toISOString(),
        analysis_duration_ms: analysisTime,
        analysis_scope: analysisScope,

        // 核心分析结果
        compaction_health: healthAssessment,
        diagnosis_results: diagnosis,
        comprehensive_recommendations: recommendations,
        executable_action_plans: actionPlans,

        // 详细数据（可选）
        collected_data: includeDetailedData ? compactionData : null,

        // 专家洞察
        expert_insights: this.generateExpertInsights(compactionData, diagnosis),
        optimization_opportunities: this.identifyOptimizationOpportunities(
          compactionData,
          diagnosis,
        ),
      };
    } catch (error) {
      throw new McpError(
        ErrorCode.InternalError,
        `Compaction专家分析失败: ${error.message}`,
      );
    }
  }

  /**
   * 收集所有Compaction相关数据
   */
  async collectAllCompactionData(connection, options) {
    const data = {
      collection_timestamp: new Date().toISOString(),
    };

    console.error('📊 收集Compaction相关数据...');

    // 1. 获取高CS分区信息（集成get_table_partitions_compaction_score功能）
    await this.collectHighCSPartitions(connection, data, options);

    // 2. 获取Compaction线程配置（集成get_compaction_threads功能）
    await this.collectCompactionThreadConfig(connection, data);

    // 3. 获取正在运行的任务（集成get_running_compaction_tasks功能）
    await this.collectRunningTasks(connection, data);

    // 4. 获取FE配置参数（集成analyze_high_compaction_score功能）
    await this.collectFEConfiguration(connection, data);

    // 5. 收集BE节点信息
    await this.collectBENodeInfo(connection, data);

    // 6. 收集历史任务信息（如果可用）
    await this.collectHistoricalTasks(connection, data);

    // 7. 收集系统资源数据
    await this.collectSystemResources(connection, data);

    // 8. 收集参数配置数据
    await this.collectParameterConfiguration(connection, data);

    // 9. 收集数据导入模式数据
    await this.collectDataIngestionPatterns(connection, data);

    // 10. 如果指定了特定表，收集详细信息
    if (options.targetDatabase && options.targetTable) {
      await this.collectTableSpecificData(connection, data, options);
    }

    console.error(`✅ 数据收集完成，共收集${Object.keys(data).length}项数据`);
    return data;
  }

  /**
   * 收集高CS分区信息
   */
  async collectHighCSPartitions(connection, data, options) {
    try {
      let query = `
        SELECT DB_NAME, TABLE_NAME, PARTITION_NAME,
               MAX_CS, AVG_CS, P50_CS, ROW_COUNT,
               DATA_SIZE, STORAGE_SIZE, BUCKETS
        FROM information_schema.partitions_meta
        WHERE MAX_CS > ${this.rules.compaction_score.warning}
      `;

      // 如果指定了特定表，只查询该表
      if (options.targetDatabase && options.targetTable) {
        query += ` AND DB_NAME = '${options.targetDatabase}' AND TABLE_NAME = '${options.targetTable}'`;
      }

      query += ' ORDER BY MAX_CS DESC LIMIT 200;';

      const [rows] = await connection.query(query);

      data.high_cs_partitions = rows.map((row) => ({
        database: row.DB_NAME,
        table: row.TABLE_NAME,
        partition: row.PARTITION_NAME,
        max_cs: row.MAX_CS,
        avg_cs: row.AVG_CS,
        p50_cs: row.P50_CS,
        row_count: row.ROW_COUNT,
        data_size: row.DATA_SIZE,
        storage_size: row.STORAGE_SIZE,
        buckets: row.BUCKETS,
        severity: this.categorizeCSScore(row.MAX_CS),
      }));

      // 统计CS分布
      data.cs_statistics = this.analyzeCSDistribution(data.high_cs_partitions);

      console.error(`   → 收集到${data.high_cs_partitions.length}个高CS分区`);
    } catch (error) {
      console.warn('收集高CS分区信息失败:', error.message);
      data.high_cs_partitions = [];
      data.cs_statistics = this.getEmptyCSStatistics();
    }
  }

  /**
   * 收集Compaction线程配置
   */
  async collectCompactionThreadConfig(connection, data) {
    try {
      const [rows] = await connection.query(`
        SELECT * FROM information_schema.be_configs WHERE name = 'compact_threads';
      `);

      data.thread_configuration = rows.map((row) => ({
        node_id: row.BE_ID,
        node_name: row.NAME || 'compact_threads',
        current_threads: parseInt(row.VALUE) || 0,
        default_threads: parseInt(row.DEFAULT) || 4,
        is_mutable: row.MUTABLE === 1,
        description: row.DESCRIPTION || 'Compaction thread count',
      }));

      // 计算线程统计信息
      data.thread_statistics = this.calculateThreadStatistics(
        data.thread_configuration,
      );

      console.error(
        `   → 收集到${data.thread_configuration.length}个节点的线程配置`,
      );
    } catch (error) {
      console.warn('收集线程配置失败:', error.message);
      data.thread_configuration = [];
      data.thread_statistics = this.getEmptyThreadStatistics();
    }
  }

  /**
   * 收集正在运行的任务
   */
  async collectRunningTasks(connection, data) {
    try {
      const [rows] = await connection.query(`
        SELECT BE_ID, TXN_ID, TABLET_ID, VERSION, START_TIME, FINISH_TIME,
               PROGRESS, STATUS, RUNS, SKIPPED
        FROM information_schema.be_cloud_native_compactions
        WHERE START_TIME IS NOT NULL AND FINISH_TIME IS NULL
        ORDER BY START_TIME;
      `);

      const now = new Date();
      data.running_tasks = rows.map((row) => ({
        be_id: row.BE_ID,
        txn_id: row.TXN_ID,
        tablet_id: row.TABLET_ID,
        version: row.VERSION,
        start_time: row.START_TIME,
        progress: row.PROGRESS || 0,
        status: row.STATUS,
        retry_count: row.RUNS || 0,
        skipped: row.SKIPPED || false,
        duration_hours: row.START_TIME
          ? (now - new Date(row.START_TIME)) / (1000 * 60 * 60)
          : 0,
        is_slow: row.START_TIME
          ? (now - new Date(row.START_TIME)) / (1000 * 60 * 60) >
            this.rules.task_execution.slow_task_threshold_hours
          : false,
        is_stalled:
          (row.PROGRESS || 0) < 50 &&
          (row.RUNS || 0) > this.rules.task_execution.max_retry_count,
      }));

      // 按BE节点分组任务
      data.tasks_by_be = this.groupTasksByBE(data.running_tasks);

      // 任务执行统计
      data.task_execution_stats = this.calculateTaskExecutionStats(
        data.running_tasks,
        data.tasks_by_be,
      );

      console.error(`   → 收集到${data.running_tasks.length}个正在运行的任务`);
    } catch (error) {
      console.warn('收集运行任务失败:', error.message);
      data.running_tasks = [];
      data.tasks_by_be = {};
      data.task_execution_stats = this.getEmptyTaskStats();
    }
  }

  /**
   * 收集FE配置参数
   */
  async collectFEConfiguration(connection, data) {
    try {
      // 尝试获取lake_compaction_max_tasks配置
      try {
        const [feRows] = await connection.query(`
          ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
        `);

        if (feRows.length > 0) {
          const maxTasks = parseInt(feRows[0].Value) || 0;
          data.fe_configuration = {
            lake_compaction_max_tasks: maxTasks,
            mode:
              maxTasks === -1
                ? 'ADAPTIVE'
                : maxTasks === 0
                  ? 'DISABLED'
                  : 'FIXED',
            is_adaptive: maxTasks === -1,
            is_disabled: maxTasks === 0,
          };
        } else {
          throw new Error('lake_compaction_max_tasks not found');
        }
      } catch (feError) {
        console.warn('无法获取FE配置，可能是权限不足:', feError.message);
        data.fe_configuration = {
          lake_compaction_max_tasks: null,
          mode: 'UNKNOWN',
          is_adaptive: false,
          is_disabled: false,
          error: feError.message,
        };
      }

      console.error('   → 收集FE配置完成');
    } catch (error) {
      console.warn('收集FE配置失败:', error.message);
      data.fe_configuration = this.getDefaultFEConfig();
    }
  }

  /**
   * 收集BE节点信息
   */
  async collectBENodeInfo(connection, data) {
    try {
      const [rows] = await connection.query('SHOW BACKENDS;');

      data.be_nodes = rows.map((row) => ({
        backend_id: row.BackendId,
        ip: row.IP,
        is_alive: row.Alive === 'true',
        cpu_cores: parseInt(row.CpuCores) || 1,
        mem_used_pct: parseFloat(row.MemUsedPct?.replace('%', '')) || 0,
        disk_used_pct: parseFloat(row.MaxDiskUsedPct?.replace('%', '')) || 0,
        last_heartbeat: row.LastHeartbeat,
      }));

      // 计算集群统计信息
      data.cluster_stats = {
        total_nodes: data.be_nodes.length,
        alive_nodes: data.be_nodes.filter((be) => be.is_alive).length,
        total_cpu_cores: data.be_nodes.reduce(
          (sum, be) => sum + be.cpu_cores,
          0,
        ),
        avg_cpu_cores:
          data.be_nodes.length > 0
            ? Math.round(
                data.be_nodes.reduce((sum, be) => sum + be.cpu_cores, 0) /
                  data.be_nodes.length,
              )
            : 0,
      };

      console.error(`   → 收集到${data.be_nodes.length}个BE节点信息`);
    } catch (error) {
      console.warn('收集BE节点信息失败:', error.message);
      data.be_nodes = [];
      data.cluster_stats = this.getEmptyClusterStats();
    }
  }

  /**
   * 收集历史任务信息
   */
  async collectHistoricalTasks(connection, data) {
    try {
      const [rows] = await connection.query(`
        SELECT BE_ID, TXN_ID, TABLET_ID, START_TIME, FINISH_TIME,
               PROGRESS, STATUS, RUNS
        FROM information_schema.be_cloud_native_compactions
        WHERE FINISH_TIME IS NOT NULL
        ORDER BY FINISH_TIME DESC
        LIMIT 100;
      `);

      data.recent_completed_tasks = rows.map((row) => {
        const duration =
          row.START_TIME && row.FINISH_TIME
            ? (new Date(row.FINISH_TIME) - new Date(row.START_TIME)) /
              (1000 * 60 * 60)
            : 0;

        return {
          be_id: row.BE_ID,
          txn_id: row.TXN_ID,
          tablet_id: row.TABLET_ID,
          start_time: row.START_TIME,
          finish_time: row.FINISH_TIME,
          progress: row.PROGRESS || 0,
          status: row.STATUS,
          retry_count: row.RUNS || 0,
          duration_hours: duration,
          is_successful: (row.PROGRESS || 0) >= 100 && row.STATUS !== 'FAILED',
        };
      });

      // 计算成功率和平均执行时间
      data.historical_performance = this.calculateHistoricalPerformance(
        data.recent_completed_tasks,
      );

      console.error(
        `   → 收集到${data.recent_completed_tasks.length}个历史任务`,
      );
    } catch (error) {
      console.warn('收集历史任务失败:', error.message);
      data.recent_completed_tasks = [];
      data.historical_performance = this.getEmptyHistoricalPerformance();
    }
  }

  /**
   * 收集特定表的详细数据
   */
  async collectTableSpecificData(connection, data, options) {
    try {
      const { targetDatabase, targetTable } = options;

      const [rows] = await connection.query(`
        SELECT DB_NAME, TABLE_NAME, PARTITION_NAME,
               MAX_CS, AVG_CS, P50_CS, ROW_COUNT,
               DATA_SIZE, STORAGE_SIZE, BUCKETS, REPLICATION_NUM
        FROM information_schema.partitions_meta
        WHERE DB_NAME = '${targetDatabase}' AND TABLE_NAME = '${targetTable}'
        ORDER BY MAX_CS DESC;
      `);

      data.target_table_analysis = {
        database: targetDatabase,
        table: targetTable,
        total_partitions: rows.length,
        partitions: rows.map((row) => ({
          partition: row.PARTITION_NAME,
          max_cs: row.MAX_CS,
          avg_cs: row.AVG_CS,
          p50_cs: row.P50_CS,
          row_count: row.ROW_COUNT,
          data_size: row.DATA_SIZE,
          storage_size: row.STORAGE_SIZE,
          buckets: row.BUCKETS,
          replication_num: row.REPLICATION_NUM,
          severity: this.categorizeCSScore(row.MAX_CS),
        })),
        cs_distribution: this.analyzeCSDistribution(
          rows.map((row) => ({ max_cs: row.MAX_CS })),
        ),
        optimization_priority: this.calculateTableOptimizationPriority(rows),
      };

      console.error(
        `   → 收集到表 ${targetDatabase}.${targetTable} 的${rows.length}个分区`,
      );
    } catch (error) {
      console.warn('收集表特定数据失败:', error.message);
      data.target_table_analysis = null;
    }
  }

  /**
   * 执行多维度专业诊断
   */
  async performMultiDimensionalDiagnosis(compactionData, analysisScope) {
    console.error('🔍 执行多维度Compaction诊断...');

    const diagnosis = {
      criticals: [],
      warnings: [],
      issues: [],
      insights: [],
    };

    // 1. CS分数诊断
    this.diagnoseCompactionScores(compactionData, diagnosis);

    // 2. 线程配置诊断
    this.diagnoseThreadConfiguration(compactionData, diagnosis);

    // 3. 任务执行效率诊断
    this.diagnoseTaskExecution(compactionData, diagnosis);

    // 4. FE配置诊断
    this.diagnoseFEConfiguration(compactionData, diagnosis);

    // 5. 系统级压力诊断
    this.diagnoseSystemPressure(compactionData, diagnosis);

    // 6. 系统资源诊断
    this.diagnoseSystemResources(compactionData, diagnosis);

    // 7. 参数配置诊断
    this.diagnoseParameterConfiguration(compactionData, diagnosis);

    // 8. 导入模式诊断
    this.diagnoseIngestionPatterns(compactionData, diagnosis);

    // 9. 如果是深度分析，执行高级诊断
    if (analysisScope === 'deep') {
      this.performAdvancedDiagnosis(compactionData, diagnosis);
    }

    // 10. 跨维度关联分析
    this.performCrossDimensionalAnalysis(compactionData, diagnosis);

    // 计算诊断统计
    diagnosis.total_issues =
      diagnosis.criticals.length +
      diagnosis.warnings.length +
      diagnosis.issues.length;
    diagnosis.summary = this.generateDiagnosisSummary(diagnosis);

    console.error(
      `✅ 诊断完成: ${diagnosis.criticals.length}个严重问题, ${diagnosis.warnings.length}个警告`,
    );

    return diagnosis;
  }

  /**
   * CS分数专业诊断
   */
  diagnoseCompactionScores(data, diagnosis) {
    const highCSPartitions = data.high_cs_partitions || [];
    const csStats = data.cs_statistics || {};

    // 紧急CS问题
    const emergencyPartitions = highCSPartitions.filter(
      (p) => p.max_cs >= this.rules.compaction_score.emergency,
    );
    if (emergencyPartitions.length > 0) {
      diagnosis.criticals.push({
        type: 'emergency_compaction_score',
        severity: 'CRITICAL',
        priority: 'IMMEDIATE',
        message: `发现 ${emergencyPartitions.length} 个紧急高CS分区 (CS ≥ 1000)`,
        affected_partitions: emergencyPartitions.slice(0, 10).map((p) => ({
          partition: `${p.database}.${p.table}.${p.partition}`,
          cs: p.max_cs,
          data_size: p.data_size,
          row_count: p.row_count,
        })),
        impact: {
          performance: '严重影响查询性能，可能导致查询超时',
          storage: '存储空间利用率低，碎片化严重',
          business: '直接影响用户体验和业务连续性',
        },
        urgency_reason: 'CS超过1000表示数据碎片化极其严重，必须立即处理',
        estimated_impact_scope:
          emergencyPartitions.length > 10 ? 'CLUSTER_WIDE' : 'LOCALIZED',
      });
    }

    // 严重CS问题
    const criticalPartitions = highCSPartitions.filter(
      (p) =>
        p.max_cs >= this.rules.compaction_score.critical &&
        p.max_cs < this.rules.compaction_score.emergency,
    );
    if (criticalPartitions.length > 0) {
      diagnosis.criticals.push({
        type: 'critical_compaction_score',
        severity: 'CRITICAL',
        priority: 'HIGH',
        message: `发现 ${criticalPartitions.length} 个严重高CS分区 (500 ≤ CS < 1000)`,
        affected_count: criticalPartitions.length,
        max_cs_in_group: Math.max(...criticalPartitions.map((p) => p.max_cs)),
        impact: {
          performance: '显著影响查询性能',
          storage: '存储效率低下',
          resource: '占用过多系统资源',
        },
        recommended_batch_size: Math.min(5, criticalPartitions.length),
        processing_strategy: 'batch_compaction_with_monitoring',
      });
    }

    // 警告级CS问题
    const warningPartitions = highCSPartitions.filter(
      (p) =>
        p.max_cs >= this.rules.compaction_score.warning &&
        p.max_cs < this.rules.compaction_score.critical,
    );
    if (warningPartitions.length > 0) {
      diagnosis.warnings.push({
        type: 'warning_compaction_score',
        severity: 'WARNING',
        priority: 'MEDIUM',
        message: `发现 ${warningPartitions.length} 个警告级高CS分区 (100 ≤ CS < 500)`,
        affected_count: warningPartitions.length,
        trend_analysis: this.analyzeCSGrowthTrend(warningPartitions),
        prevention_focus: true,
      });
    }

    // CS分布洞察
    if (csStats.total_partitions > 0) {
      diagnosis.insights.push({
        type: 'cs_distribution_analysis',
        message: 'Compaction Score 分布分析',
        statistics: csStats,
        health_indicators: {
          excellent_ratio: (
            ((csStats.excellent_partitions || 0) / csStats.total_partitions) *
            100
          ).toFixed(1),
          problematic_ratio: (
            ((csStats.critical_partitions + csStats.emergency_partitions) /
              csStats.total_partitions) *
            100
          ).toFixed(1),
        },
        recommendations: this.generateCSDistributionRecommendations(csStats),
      });
    }
  }

  /**
   * 线程配置诊断
   */
  diagnoseThreadConfiguration(data, diagnosis) {
    const threadConfig = data.thread_configuration || [];
    const threadStats = data.thread_statistics || {};
    const beNodes = data.be_nodes || [];

    if (threadConfig.length === 0) {
      diagnosis.warnings.push({
        type: 'thread_config_unavailable',
        severity: 'WARNING',
        message: '无法获取Compaction线程配置信息',
        impact: '无法评估线程配置合理性',
        suggestions: [
          '检查数据库连接权限',
          '确认StarRocks版本支持线程配置查询',
        ],
      });
      return;
    }

    // 分析每个节点的线程配置
    threadConfig.forEach((config) => {
      const beNode = beNodes.find((be) => be.backend_id == config.node_id);
      const cpuCores = beNode ? beNode.cpu_cores : 4; // 默认4核

      const minRecommended = Math.max(
        this.rules.thread_config.absolute_min_threads,
        Math.ceil(cpuCores * this.rules.thread_config.min_threads_per_core),
      );

      const maxRecommended = Math.min(
        this.rules.thread_config.absolute_max_threads,
        Math.ceil(cpuCores * this.rules.thread_config.max_threads_per_core),
      );

      const currentThreads = config.current_threads;
      const nodeIP = beNode ? beNode.ip : 'Unknown';

      // 线程数过低
      if (currentThreads < minRecommended) {
        const severity =
          currentThreads < this.rules.thread_config.recommended_base
            ? 'CRITICAL'
            : 'WARNING';
        (severity === 'CRITICAL'
          ? diagnosis.criticals
          : diagnosis.warnings
        ).push({
          type: 'low_compaction_threads',
          node_id: config.node_id,
          node_ip: nodeIP,
          severity: severity,
          priority: severity === 'CRITICAL' ? 'HIGH' : 'MEDIUM',
          message: `节点 ${nodeIP} Compaction线程数过低 (${currentThreads}/${cpuCores}核)`,
          current_config: {
            threads: currentThreads,
            cpu_cores: cpuCores,
            threads_per_core: (currentThreads / cpuCores).toFixed(2),
          },
          recommendations: {
            min_threads: minRecommended,
            optimal_threads: Math.ceil(cpuCores * 0.375), // 中间值
            max_threads: maxRecommended,
          },
          impact:
            severity === 'CRITICAL'
              ? 'Compaction处理能力严重不足，CS积累加速'
              : 'Compaction效率偏低，可能导致CS增长',
          adjustment_command: this.generateThreadAdjustmentCommand(
            config.node_id,
            nodeIP,
            minRecommended,
          ),
        });
      }

      // 线程数过高
      else if (currentThreads > maxRecommended) {
        diagnosis.warnings.push({
          type: 'high_compaction_threads',
          node_id: config.node_id,
          node_ip: nodeIP,
          severity: 'WARNING',
          priority: 'LOW',
          message: `节点 ${nodeIP} Compaction线程数偏高 (${currentThreads}/${cpuCores}核)`,
          current_config: {
            threads: currentThreads,
            cpu_cores: cpuCores,
            threads_per_core: (currentThreads / cpuCores).toFixed(2),
          },
          impact: '可能过度消耗CPU资源，影响其他操作',
          risk_assessment: 'MEDIUM',
          suggested_adjustment: maxRecommended,
        });
      }
    });

    // 集群级线程配置洞察
    if (threadStats.total_threads > 0) {
      diagnosis.insights.push({
        type: 'cluster_thread_analysis',
        message: '集群Compaction线程配置分析',
        cluster_metrics: {
          total_threads: threadStats.total_threads,
          average_threads_per_node: threadStats.avg_threads_per_node,
          threads_per_core_ratio: threadStats.avg_threads_per_core,
          thread_utilization: this.calculateThreadUtilization(data),
        },
        optimization_potential: this.assessThreadOptimizationPotential(
          threadStats,
          data.cluster_stats,
        ),
        best_practices: [
          '线程数应根据CPU核心数动态配置',
          '监控线程使用率避免资源浪费',
          '定期评估线程配置与工作负载的匹配度',
        ],
      });
    }
  }

  /**
   * 任务执行效率诊断
   */
  diagnoseTaskExecution(data, diagnosis) {
    const runningTasks = data.running_tasks || [];
    const taskStats = data.task_execution_stats || {};
    const historicalPerf = data.historical_performance || {};

    // 检查停滞任务
    const stalledTasks = runningTasks.filter((task) => task.is_stalled);
    if (stalledTasks.length > 0) {
      diagnosis.criticals.push({
        type: 'stalled_compaction_tasks',
        severity: 'CRITICAL',
        priority: 'IMMEDIATE',
        message: `发现 ${stalledTasks.length} 个停滞的Compaction任务`,
        stalled_tasks: stalledTasks.map((task) => ({
          be_id: task.be_id,
          tablet_id: task.tablet_id,
          progress: task.progress,
          retry_count: task.retry_count,
          duration_hours: task.duration_hours.toFixed(1),
          status: task.status,
        })),
        impact: '停滞任务阻塞Compaction队列，影响整体效率',
        root_cause_analysis: [
          '检查BE节点资源使用情况',
          '验证磁盘IO性能',
          '检查网络连接稳定性',
          '查看BE日志中的错误信息',
        ],
        recovery_actions: this.generateStalledTaskRecoveryActions(stalledTasks),
      });
    }

    // 检查长时间运行任务
    const slowTasks = runningTasks.filter(
      (task) => task.is_slow && !task.is_stalled,
    );
    if (slowTasks.length > 0) {
      diagnosis.warnings.push({
        type: 'slow_compaction_tasks',
        severity: 'WARNING',
        priority: 'MEDIUM',
        message: `发现 ${slowTasks.length} 个长时间运行的任务`,
        slow_tasks: slowTasks.slice(0, 5).map((task) => ({
          be_id: task.be_id,
          tablet_id: task.tablet_id,
          duration_hours: task.duration_hours.toFixed(1),
          progress: task.progress,
        })),
        impact: '可能表示系统负载过高或数据复杂度高',
        monitoring_suggestion: '建议持续监控这些任务的进度',
      });
    }

    // 检查单节点任务过载
    const tasksByBE = data.tasks_by_be || {};
    const overloadedNodes = Object.entries(tasksByBE).filter(
      ([beId, tasks]) =>
        tasks.length > this.rules.task_execution.max_healthy_tasks_per_node,
    );

    if (overloadedNodes.length > 0) {
      diagnosis.warnings.push({
        type: 'node_task_overload',
        severity: 'WARNING',
        priority: 'MEDIUM',
        message: `${overloadedNodes.length} 个节点任务负载过高`,
        overloaded_nodes: overloadedNodes.map(([beId, tasks]) => ({
          be_id: beId,
          task_count: tasks.length,
          max_recommended: this.rules.task_execution.max_healthy_tasks_per_node,
        })),
        impact: '可能导致任务执行缓慢和资源竞争',
        load_balancing_needed: true,
      });
    }

    // 历史性能分析
    if (historicalPerf.total_tasks > 0) {
      const successRate = historicalPerf.success_rate;
      if (successRate < this.rules.task_execution.healthy_success_rate) {
        diagnosis.warnings.push({
          type: 'low_task_success_rate',
          severity: 'WARNING',
          priority: 'MEDIUM',
          message: `Compaction任务成功率偏低 (${successRate.toFixed(1)}%)`,
          historical_metrics: {
            total_tasks: historicalPerf.total_tasks,
            successful_tasks: historicalPerf.successful_tasks,
            success_rate: successRate,
            avg_duration_hours: historicalPerf.avg_duration_hours,
          },
          impact: '频繁的任务失败可能导致CS持续积累',
          investigation_areas: [
            '检查磁盘空间是否充足',
            '验证网络连接稳定性',
            '分析BE节点性能指标',
            '查看错误日志模式',
          ],
        });
      }
    }

    // 任务执行洞察
    if (runningTasks.length > 0 || historicalPerf.total_tasks > 0) {
      diagnosis.insights.push({
        type: 'task_execution_analysis',
        message: 'Compaction任务执行分析',
        current_load: {
          running_tasks: runningTasks.length,
          tasks_per_node: taskStats.avg_tasks_per_node,
          cluster_utilization: this.calculateClusterTaskUtilization(data),
        },
        performance_trends: {
          success_rate: historicalPerf.success_rate,
          avg_duration: historicalPerf.avg_duration_hours,
          efficiency_rating: this.calculateTaskEfficiencyRating(historicalPerf),
        },
        optimization_suggestions: this.generateTaskOptimizationSuggestions(
          taskStats,
          historicalPerf,
        ),
      });
    }
  }

  /**
   * FE配置诊断
   */
  diagnoseFEConfiguration(data, diagnosis) {
    const feConfig = data.fe_configuration || {};
    const clusterStats = data.cluster_stats || {};
    const csStats = data.cs_statistics || {};

    if (feConfig.error) {
      diagnosis.warnings.push({
        type: 'fe_config_access_error',
        severity: 'WARNING',
        priority: 'LOW',
        message: '无法获取FE配置参数',
        error_details: feConfig.error,
        impact: '无法评估lake_compaction_max_tasks配置合理性',
        suggestions: [
          '检查是否有ADMIN权限',
          '确认StarRocks版本支持该配置项',
          '手动检查FE配置文件',
        ],
      });
      return;
    }

    const maxTasks = feConfig.lake_compaction_max_tasks;
    const totalNodes = clusterStats.alive_nodes || 1;
    const highCSPartitions =
      csStats.critical_partitions + csStats.emergency_partitions || 0;

    // Compaction被禁用
    if (feConfig.is_disabled) {
      diagnosis.criticals.push({
        type: 'compaction_disabled',
        severity: 'CRITICAL',
        priority: 'HIGH',
        message: 'Compaction功能已被禁用 (lake_compaction_max_tasks = 0)',
        impact: {
          immediate: 'CS将持续增长，无法自动压缩',
          long_term: '存储效率严重下降，查询性能恶化',
        },
        business_risk: 'HIGH',
        recommended_value: Math.max(
          totalNodes * this.rules.fe_config.adaptive_multiplier,
          this.rules.fe_config.min_recommended_max_tasks,
        ),
        enable_command:
          'ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");', // 建议使用自适应模式
      });
    }

    // 自适应模式评估
    else if (feConfig.is_adaptive) {
      const adaptiveMaxTasks =
        totalNodes * this.rules.fe_config.adaptive_multiplier;

      diagnosis.insights.push({
        type: 'adaptive_compaction_config',
        message: 'Compaction自适应模式评估',
        current_config: {
          mode: 'ADAPTIVE',
          calculated_max_tasks: adaptiveMaxTasks,
          node_count: totalNodes,
        },
        effectiveness_assessment: this.assessAdaptiveModeEffectiveness(
          adaptiveMaxTasks,
          highCSPartitions,
        ),
        pros: ['自动根据集群规模调整', '适应集群扩缩容', '减少手动配置维护'],
        cons: [
          '可能不适应特定工作负载',
          '无法精细化控制',
          '突发负载时可能不够灵活',
        ],
        recommendation:
          highCSPartitions > adaptiveMaxTasks / 2
            ? 'CONSIDER_FIXED_VALUE'
            : 'KEEP_ADAPTIVE',
      });
    }

    // 固定值模式评估
    else {
      const recommendedMinTasks = Math.max(
        totalNodes * 8, // 每个节点至少8个任务
        highCSPartitions / 10, // 高CS分区数的1/10
        this.rules.fe_config.min_recommended_max_tasks,
      );

      if (maxTasks < recommendedMinTasks) {
        const severity =
          maxTasks < recommendedMinTasks / 2 ? 'CRITICAL' : 'WARNING';

        (severity === 'CRITICAL'
          ? diagnosis.criticals
          : diagnosis.warnings
        ).push({
          type: 'low_max_compaction_tasks',
          severity: severity,
          priority: severity === 'CRITICAL' ? 'HIGH' : 'MEDIUM',
          message: `lake_compaction_max_tasks设置过低 (${maxTasks})`,
          current_vs_recommended: {
            current: maxTasks,
            recommended_min: recommendedMinTasks,
            gap_ratio: (recommendedMinTasks / maxTasks).toFixed(1),
          },
          impact:
            severity === 'CRITICAL'
              ? 'Compaction处理能力严重不足，CS快速积累'
              : 'Compaction处理能力有限，可能无法及时处理高CS分区',
          tuning_suggestion: {
            immediate_value: Math.min(recommendedMinTasks, maxTasks * 2), // 先翻倍，避免激进调整
            target_value: recommendedMinTasks,
            adjustment_command: `ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "${recommendedMinTasks}");`,
          },
        });
      } else if (maxTasks > recommendedMinTasks * 3) {
        diagnosis.warnings.push({
          type: 'high_max_compaction_tasks',
          severity: 'WARNING',
          priority: 'LOW',
          message: `lake_compaction_max_tasks设置过高 (${maxTasks})`,
          impact: '可能过度占用系统资源',
          resource_risk: 'MEDIUM',
          optimization_opportunity: true,
          suggested_value: Math.ceil(recommendedMinTasks * 1.5),
        });
      }
    }
  }

  /**
   * 系统级压力诊断
   */
  diagnoseSystemPressure(data, diagnosis) {
    const runningTasks = data.running_tasks || [];
    const beNodes = data.be_nodes || [];
    const threadConfig = data.thread_configuration || [];
    const csStats = data.cs_statistics || {};

    const aliveNodes = beNodes.filter((be) => be.is_alive).length;
    const totalRunningTasks = runningTasks.length;
    const totalThreads = threadConfig.reduce(
      (sum, config) => sum + config.current_threads,
      0,
    );

    // 计算系统压力指标
    const pressureMetrics = {
      tasks_per_node: aliveNodes > 0 ? totalRunningTasks / aliveNodes : 0,
      thread_utilization:
        totalThreads > 0 ? totalRunningTasks / totalThreads : 0,
      high_cs_density:
        (csStats.critical_partitions + csStats.emergency_partitions) /
        Math.max(aliveNodes, 1),
      cluster_load_level: this.calculateClusterLoadLevel(
        totalRunningTasks,
        aliveNodes,
        csStats,
      ),
    };

    // 高系统压力诊断
    if (pressureMetrics.cluster_load_level === 'HIGH') {
      diagnosis.criticals.push({
        type: 'high_system_compaction_pressure',
        severity: 'CRITICAL',
        priority: 'HIGH',
        message: '系统Compaction压力过高',
        pressure_indicators: {
          tasks_per_node: pressureMetrics.tasks_per_node.toFixed(1),
          thread_utilization: `${(pressureMetrics.thread_utilization * 100).toFixed(1)}%`,
          high_cs_partitions:
            csStats.critical_partitions + csStats.emergency_partitions,
          load_level: pressureMetrics.cluster_load_level,
        },
        impact: {
          performance: '系统响应能力下降',
          stability: '可能导致任务积压和系统不稳定',
          business: '影响数据实时性和查询性能',
        },
        immediate_actions: [
          '暂停非关键数据导入',
          '手动清理最高CS分区',
          '考虑增加处理线程',
          '监控系统资源使用',
        ],
      });
    } else if (pressureMetrics.cluster_load_level === 'MEDIUM') {
      diagnosis.warnings.push({
        type: 'elevated_compaction_pressure',
        severity: 'WARNING',
        priority: 'MEDIUM',
        message: '系统Compaction压力偏高',
        trend_warning: true,
        monitoring_focus: [
          '密切关注CS增长趋势',
          '监控任务执行效率',
          '评估是否需要扩容',
        ],
      });
    }

    // 资源利用率分析
    diagnosis.insights.push({
      type: 'system_resource_analysis',
      message: 'Compaction系统资源利用分析',
      resource_metrics: pressureMetrics,
      capacity_assessment: {
        current_capacity: totalThreads,
        current_utilization: `${(pressureMetrics.thread_utilization * 100).toFixed(1)}%`,
        bottleneck_analysis: this.identifyCompactionBottlenecks(data),
        scaling_recommendation: this.generateScalingRecommendation(
          pressureMetrics,
          data,
        ),
      },
      efficiency_score: this.calculateCompactionEfficiencyScore(
        pressureMetrics,
        data,
      ),
    });
  }

  /**
   * 生成线程调整命令
   */
  generateThreadAdjustmentCommand(nodeId, nodeIP, recommendedThreads) {
    return {
      description: `调整节点 ${nodeIP} 的Compaction线程数`,
      command: `ADMIN SET be_config ("compact_threads" = "${recommendedThreads}") FOR "${nodeIP}";`,
      alternative_command: `UPDATE information_schema.be_configs SET value = ${recommendedThreads} WHERE name = 'compact_threads' AND BE_ID = ${nodeId};`,
      verification: `SELECT * FROM information_schema.be_configs WHERE name = 'compact_threads' AND BE_ID = ${nodeId};`,
      notes: [
        '建议在低峰期执行配置变更',
        '变更后监控系统资源使用情况',
        '根据实际效果进行微调',
      ],
    };
  }

  /**
   * 生成综合优化建议
   */
  generateComprehensiveRecommendations(diagnosis, compactionData) {
    console.error('💡 生成Compaction专业优化建议...');

    const recommendations = [];

    // 处理严重问题的建议
    diagnosis.criticals.forEach((critical) => {
      const recommendation = this.createCriticalIssueRecommendation(
        critical,
        compactionData,
      );
      if (recommendation) recommendations.push(recommendation);
    });

    // 处理警告问题的建议
    diagnosis.warnings.forEach((warning) => {
      const recommendation = this.createWarningIssueRecommendation(
        warning,
        compactionData,
      );
      if (recommendation) recommendations.push(recommendation);
    });

    // 添加预防性和优化性建议
    recommendations.push(
      ...this.generatePreventiveRecommendations(compactionData, diagnosis),
    );

    return recommendations.sort((a, b) => {
      const priorityOrder = { IMMEDIATE: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };
      return (
        (priorityOrder[b.priority] || 0) - (priorityOrder[a.priority] || 0)
      );
    });
  }

  /**
   * 创建严重问题建议
   */
  createCriticalIssueRecommendation(critical, compactionData) {
    switch (critical.type) {
      case 'emergency_compaction_score':
        return {
          id: 'emergency_cs_handling',
          category: 'critical_performance',
          priority: 'IMMEDIATE',
          title: '🚨 紧急CS处理计划',
          description: `立即处理${critical.affected_partitions.length}个紧急高CS分区`,
          estimated_duration: '2-4小时',
          risk_level: 'LOW',
          business_impact: 'HIGH_POSITIVE',

          action_plan: {
            phase1: {
              name: '紧急处理阶段',
              duration: '30-60分钟',
              actions: [
                {
                  step: 1,
                  action: '识别最高优先级分区',
                  command: `SELECT DB_NAME, TABLE_NAME, PARTITION_NAME, MAX_CS FROM information_schema.partitions_meta WHERE MAX_CS >= 1000 ORDER BY MAX_CS DESC LIMIT 5;`,
                  purpose: '定位需要立即处理的分区',
                },
                {
                  step: 2,
                  action: '执行紧急Compaction',
                  commands: critical.affected_partitions
                    .slice(0, 3)
                    .map(
                      (p) =>
                        `ALTER TABLE \`${p.partition.split('.')[0]}\`.\`${p.partition.split('.')[1]}\` COMPACT \`${p.partition.split('.')[2]}\`;`,
                    ),
                  parallel_execution: false,
                  monitoring_required: true,
                },
              ],
            },
            phase2: {
              name: '批量处理阶段',
              duration: '2-3小时',
              actions: [
                {
                  step: 1,
                  action: '分批处理剩余分区',
                  batch_size: 3,
                  interval_minutes: 15,
                  progress_monitoring: true,
                },
              ],
            },
          },

          monitoring_plan: {
            immediate_metrics: [
              'Compaction任务进度',
              '系统资源使用',
              'CS变化趋势',
            ],
            success_criteria: 'CS降至500以下',
            fallback_plan: '如果Compaction效果不佳，考虑手动数据重建',
          },

          prevention_measures: [
            '设置CS监控告警阈值为300',
            '建立定期Compaction检查机制',
            '优化数据导入策略减少小批量写入',
          ],
        };

      case 'compaction_disabled':
        return {
          id: 'enable_compaction',
          category: 'critical_configuration',
          priority: 'IMMEDIATE',
          title: '🔧 启用Compaction功能',
          description: 'Compaction被禁用，必须立即启用以防止CS无限增长',

          immediate_action: {
            command: critical.enable_command,
            verification:
              'ADMIN SHOW FRONTEND CONFIG LIKE "lake_compaction_max_tasks";',
            expected_result: '配置值应为-1（自适应）或正整数',
          },

          post_enable_monitoring: {
            duration: '24小时',
            key_metrics: [
              '新启动的Compaction任务数',
              'CS变化趋势',
              '系统资源使用',
            ],
            adjustment_threshold:
              '如果24小时内CS无明显下降，需要调整max_tasks值',
          },
        };

      case 'stalled_compaction_tasks':
        return {
          id: 'recover_stalled_tasks',
          category: 'critical_recovery',
          priority: 'IMMEDIATE',
          title: '🔄 停滞任务恢复',
          description: `恢复${critical.stalled_tasks.length}个停滞的Compaction任务`,

          investigation_steps: critical.root_cause_analysis,
          recovery_actions: critical.recovery_actions || [
            '检查BE节点日志获取详细错误信息',
            '验证磁盘空间和权限',
            '考虑重启相关BE进程（谨慎操作）',
          ],

          prevention_strategy: {
            monitoring: '设置任务执行时间监控',
            alerting: '任务进度停滞超过1小时自动告警',
            maintenance: '定期检查任务队列健康状态',
          },
        };

      default:
        return null;
    }
  }

  /**
   * 创建警告问题建议
   */
  createWarningIssueRecommendation(warning, compactionData) {
    switch (warning.type) {
      case 'low_compaction_threads':
        return {
          id: `optimize_threads_${warning.node_id}`,
          category: 'performance_tuning',
          priority: 'MEDIUM',
          title: `🔧 优化节点${warning.node_ip}线程配置`,
          description: `当前${warning.current_config.threads}线程不足，建议调整至${warning.recommendations.optimal_threads}线程`,

          implementation: {
            command: warning.adjustment_command.command,
            verification: warning.adjustment_command.verification,
            rollback_plan: `恢复原配置：${warning.current_config.threads}线程`,
          },

          monitoring_after_change: {
            duration: '48小时',
            metrics: ['Compaction任务完成速度', 'CPU使用率', '新CS产生速度'],
            success_criteria: 'Compaction处理速度提升20%以上',
            adjustment_guideline: '根据实际效果进行微调',
          },
        };

      case 'warning_compaction_score':
        return {
          id: 'preventive_cs_management',
          category: 'preventive_maintenance',
          priority: 'MEDIUM',
          title: '📋 预防性CS管理',
          description: `管理${warning.affected_count}个警告级CS分区，防止恶化`,

          strategy: {
            approach: 'SCHEDULED_MAINTENANCE',
            schedule: '低峰期批量处理',
            batch_size: 10,
            frequency: '每周一次',
          },

          automation_opportunity: {
            description: '可考虑建立自动化Compaction脚本',
            trigger_condition: 'CS > 150',
            safety_checks: ['系统负载检查', '业务影响评估'],
          },
        };

      default:
        return null;
    }
  }

  /**
   * 生成预防性建议
   */
  generatePreventiveRecommendations(compactionData, diagnosis) {
    const recommendations = [];

    // 监控和告警建议
    recommendations.push({
      id: 'monitoring_enhancement',
      category: 'monitoring_alerting',
      priority: 'LOW',
      title: '📊 增强Compaction监控体系',
      description: '建立全面的Compaction监控和告警机制',

      monitoring_framework: {
        key_metrics: [
          'CS分布统计（按严重级别）',
          '任务执行成功率和平均时间',
          '线程利用率和系统负载',
          'FE配置参数跟踪',
        ],
        alert_thresholds: {
          cs_emergency: 'CS > 800',
          task_failure_rate: '成功率 < 85%',
          system_overload: '任务/线程比 > 0.8',
        },
        dashboard_components: [
          'CS趋势图',
          '任务执行状态',
          '资源利用率',
          '配置变更历史',
        ],
      },
    });

    // 容量规划建议
    recommendations.push({
      id: 'capacity_planning',
      category: 'capacity_planning',
      priority: 'LOW',
      title: '📈 Compaction容量规划',
      description: '基于当前数据制定长期容量规划策略',

      planning_framework: {
        growth_projection:
          this.calculateCompactionGrowthProjection(compactionData),
        scaling_triggers: [
          'CS分区数持续增长超过处理能力',
          '任务队列长度超过健康阈值',
          '平均任务执行时间持续上升',
        ],
        scaling_options: [
          '增加BE节点（水平扩展）',
          '调整线程配置（垂直优化）',
          '优化FE参数（系统级优化）',
        ],
      },
    });

    // 最佳实践建议
    recommendations.push({
      id: 'best_practices',
      category: 'best_practices',
      priority: 'LOW',
      title: '🎯 Compaction最佳实践',
      description: '基于专家经验的Compaction管理最佳实践',

      operational_guidelines: {
        daily_operations: [
          '每日检查CS分布情况',
          '监控任务执行状态',
          '验证系统资源使用',
        ],
        weekly_maintenance: [
          '分析CS增长趋势',
          '评估配置参数合理性',
          '审查任务执行效率',
        ],
        monthly_review: [
          '容量规划评估',
          '配置优化机会识别',
          '系统性能基准测试',
        ],
      },

      emergency_procedures: {
        high_cs_response: '发现CS > 500时的响应流程',
        system_overload: '系统过载时的负载减轻策略',
        task_failure_spike: '任务失败率突增时的处理方案',
      },
    });

    return recommendations;
  }

  /**
   * 生成可执行的操作计划
   */
  generateActionPlans(diagnosis, recommendations) {
    const actionPlans = [];

    // 为每个高优先级建议生成详细执行计划
    const highPriorityRecs = recommendations.filter((rec) =>
      ['IMMEDIATE', 'HIGH'].includes(rec.priority),
    );

    highPriorityRecs.forEach((rec) => {
      actionPlans.push({
        recommendation_id: rec.id,
        plan_name: rec.title,
        priority: rec.priority,
        estimated_duration: rec.estimated_duration || '30-60分钟',

        execution_steps: this.generateExecutionSteps(rec),
        prerequisites: this.identifyPrerequisites(rec),
        risk_mitigation: this.createRiskMitigation(rec),
        success_verification: this.defineSuccessVerification(rec),
      });
    });

    return actionPlans;
  }

  /**
   * 生成执行步骤
   */
  generateExecutionSteps(recommendation) {
    if (recommendation.action_plan) {
      // 已有详细计划的情况
      return recommendation.action_plan;
    }

    // 通用执行步骤模板
    return {
      preparation: ['备份当前配置', '确认系统状态稳定', '通知相关团队'],
      execution: ['按计划执行配置变更', '实时监控系统指标', '记录变更过程'],
      verification: ['验证配置生效', '检查目标指标改善', '确认无副作用'],
      cleanup: ['清理临时文件', '更新文档记录', '总结经验教训'],
    };
  }

  /**
   * 识别前置条件
   */
  identifyPrerequisites(recommendation) {
    const commonPrerequisites = [
      '具有ADMIN权限',
      '系统处于稳定状态',
      '已通知相关业务方',
    ];

    switch (recommendation.category) {
      case 'critical_performance':
        return [...commonPrerequisites, '确认磁盘空间充足', '验证网络连接正常'];
      case 'critical_configuration':
        return [...commonPrerequisites, '备份当前FE配置', '准备回滚方案'];
      case 'performance_tuning':
        return [
          ...commonPrerequisites,
          '确认BE节点状态正常',
          '监控系统基准指标',
        ];
      default:
        return commonPrerequisites;
    }
  }

  /**
   * 创建风险缓解措施
   */
  createRiskMitigation(recommendation) {
    return {
      risk_level: recommendation.risk_level || 'MEDIUM',
      potential_risks: [
        '配置变更可能暂时影响性能',
        'Compaction任务可能短暂增加系统负载',
        '操作过程中可能出现意外错误',
      ],
      mitigation_measures: [
        '在低峰期执行变更',
        '分阶段逐步调整',
        '准备快速回滚方案',
        '全程监控关键指标',
      ],
      rollback_plan: {
        trigger_conditions: [
          '系统性能严重下降',
          '错误率显著上升',
          '业务投诉增加',
        ],
        rollback_steps: [
          '停止当前操作',
          '恢复原配置',
          '验证系统恢复',
          '分析失败原因',
        ],
      },
    };
  }

  /**
   * 定义成功验证标准
   */
  defineSuccessVerification(recommendation) {
    const baseVerification = {
      immediate_checks: [
        '配置已正确应用',
        '系统服务正常运行',
        '无错误日志产生',
      ],
      short_term_validation: {
        timeframe: '1-2小时',
        metrics: ['目标指标改善', '系统稳定运行', '无性能回退'],
      },
      long_term_monitoring: {
        timeframe: '24-48小时',
        success_criteria: ['持续改善趋势', '无副作用出现', '业务影响为正向'],
      },
    };

    // 根据建议类型添加特定验证项
    switch (recommendation.category) {
      case 'critical_performance':
        baseVerification.specific_metrics = [
          'CS显著下降',
          'Compaction任务正常执行',
        ];
        break;
      case 'performance_tuning':
        baseVerification.specific_metrics = [
          '任务处理速度提升',
          'CPU使用率合理',
        ];
        break;
    }

    return baseVerification;
  }

  // ============= 辅助方法 =============

  /**
   * 分类CS分数严重程度
   */
  categorizeCSScore(cs) {
    if (cs >= this.rules.compaction_score.emergency) return 'EMERGENCY';
    if (cs >= this.rules.compaction_score.critical) return 'CRITICAL';
    if (cs >= this.rules.compaction_score.warning) return 'WARNING';
    if (cs >= this.rules.compaction_score.normal) return 'NORMAL';
    return 'EXCELLENT';
  }

  /**
   * 分析CS分布
   */
  analyzeCSDistribution(partitions) {
    const distribution = {
      total_partitions: partitions.length,
      excellent_partitions: 0,
      normal_partitions: 0,
      warning_partitions: 0,
      critical_partitions: 0,
      emergency_partitions: 0,
    };

    partitions.forEach((partition) => {
      const cs = partition.max_cs || partition.MAX_CS || 0;
      const category = this.categorizeCSScore(cs);

      switch (category) {
        case 'EXCELLENT':
          distribution.excellent_partitions++;
          break;
        case 'NORMAL':
          distribution.normal_partitions++;
          break;
        case 'WARNING':
          distribution.warning_partitions++;
          break;
        case 'CRITICAL':
          distribution.critical_partitions++;
          break;
        case 'EMERGENCY':
          distribution.emergency_partitions++;
          break;
      }
    });

    // 计算统计指标
    distribution.problematic_ratio =
      distribution.total_partitions > 0
        ? (
            ((distribution.critical_partitions +
              distribution.emergency_partitions) /
              distribution.total_partitions) *
            100
          ).toFixed(1)
        : 0;

    return distribution;
  }

  /**
   * 计算线程统计信息
   */
  calculateThreadStatistics(threadConfig) {
    if (threadConfig.length === 0) return this.getEmptyThreadStatistics();

    const totalThreads = threadConfig.reduce(
      (sum, config) => sum + config.current_threads,
      0,
    );
    const avgThreads = totalThreads / threadConfig.length;

    return {
      total_nodes: threadConfig.length,
      total_threads: totalThreads,
      avg_threads_per_node: Math.round(avgThreads * 10) / 10,
      min_threads: Math.min(...threadConfig.map((c) => c.current_threads)),
      max_threads: Math.max(...threadConfig.map((c) => c.current_threads)),
      thread_variance: this.calculateVariance(
        threadConfig.map((c) => c.current_threads),
      ),
    };
  }

  /**
   * 按BE分组任务
   */
  groupTasksByBE(runningTasks) {
    const tasksByBE = {};

    runningTasks.forEach((task) => {
      if (!tasksByBE[task.be_id]) {
        tasksByBE[task.be_id] = [];
      }
      tasksByBE[task.be_id].push(task);
    });

    return tasksByBE;
  }

  /**
   * 计算任务执行统计
   */
  calculateTaskExecutionStats(runningTasks, tasksByBE) {
    const nodeTaskCounts = Object.values(tasksByBE).map(
      (tasks) => tasks.length,
    );

    return {
      total_running_tasks: runningTasks.length,
      nodes_with_tasks: Object.keys(tasksByBE).length,
      avg_tasks_per_node:
        nodeTaskCounts.length > 0
          ? Math.round(
              (nodeTaskCounts.reduce((sum, count) => sum + count, 0) /
                nodeTaskCounts.length) *
                10,
            ) / 10
          : 0,
      max_tasks_per_node:
        nodeTaskCounts.length > 0 ? Math.max(...nodeTaskCounts) : 0,
      slow_tasks_count: runningTasks.filter((task) => task.is_slow).length,
      stalled_tasks_count: runningTasks.filter((task) => task.is_stalled)
        .length,
    };
  }

  /**
   * 计算历史性能
   */
  calculateHistoricalPerformance(completedTasks) {
    if (completedTasks.length === 0)
      return this.getEmptyHistoricalPerformance();

    const successfulTasks = completedTasks.filter((task) => task.is_successful);
    const durations = completedTasks
      .filter((task) => task.duration_hours > 0)
      .map((task) => task.duration_hours);

    return {
      total_tasks: completedTasks.length,
      successful_tasks: successfulTasks.length,
      success_rate: (
        (successfulTasks.length / completedTasks.length) *
        100
      ).toFixed(1),
      avg_duration_hours:
        durations.length > 0
          ? (
              durations.reduce((sum, d) => sum + d, 0) / durations.length
            ).toFixed(2)
          : 0,
    };
  }

  /**
   * 计算Compaction健康分数
   */
  calculateCompactionHealth(diagnosis, compactionData) {
    let score = 100;

    // 基于问题严重程度扣分
    score -= diagnosis.criticals.length * 25;
    score -= diagnosis.warnings.length * 10;
    score -= diagnosis.issues.length * 5;

    // 基于CS分布扣分
    const csStats = compactionData.cs_statistics || {};
    score -= (csStats.emergency_partitions || 0) * 3;
    score -= (csStats.critical_partitions || 0) * 1;

    // 基于任务执行效率扣分
    const taskStats = compactionData.task_execution_stats || {};
    if (taskStats.stalled_tasks_count > 0)
      score -= taskStats.stalled_tasks_count * 5;

    score = Math.max(0, score);

    let level = 'EXCELLENT';
    if (score < 40) level = 'POOR';
    else if (score < 60) level = 'FAIR';
    else if (score < 80) level = 'GOOD';

    return {
      score: Math.round(score),
      level: level,
      status:
        diagnosis.criticals.length > 0
          ? 'CRITICAL'
          : diagnosis.warnings.length > 0
            ? 'WARNING'
            : 'HEALTHY',
    };
  }

  /**
   * 生成诊断摘要
   */
  generateDiagnosisSummary(diagnosis) {
    const criticals = diagnosis.criticals.length;
    const warnings = diagnosis.warnings.length;
    const issues = diagnosis.issues.length;

    if (criticals > 0) {
      const emergencyIssues = diagnosis.criticals.filter(
        (c) => c.priority === 'IMMEDIATE',
      ).length;
      if (emergencyIssues > 0) {
        return `Compaction系统存在 ${emergencyIssues} 个紧急问题，需立即处理`;
      }
      return `Compaction系统发现 ${criticals} 个严重问题，需要尽快处理`;
    } else if (warnings > 0) {
      return `Compaction系统发现 ${warnings} 个警告问题，建议近期优化`;
    } else if (issues > 0) {
      return `Compaction系统发现 ${issues} 个一般问题，可安排时间处理`;
    } else {
      return 'Compaction系统运行状态良好，压缩效率正常';
    }
  }

  // ============= 空数据结构 =============

  getEmptyCSStatistics() {
    return {
      total_partitions: 0,
      excellent_partitions: 0,
      normal_partitions: 0,
      warning_partitions: 0,
      critical_partitions: 0,
      emergency_partitions: 0,
      problematic_ratio: 0,
    };
  }

  getEmptyThreadStatistics() {
    return {
      total_nodes: 0,
      total_threads: 0,
      avg_threads_per_node: 0,
      min_threads: 0,
      max_threads: 0,
      thread_variance: 0,
    };
  }

  getEmptyTaskStats() {
    return {
      total_running_tasks: 0,
      nodes_with_tasks: 0,
      avg_tasks_per_node: 0,
      max_tasks_per_node: 0,
      slow_tasks_count: 0,
      stalled_tasks_count: 0,
    };
  }

  getEmptyHistoricalPerformance() {
    return {
      total_tasks: 0,
      successful_tasks: 0,
      success_rate: 0,
      avg_duration_hours: 0,
    };
  }

  getEmptyClusterStats() {
    return {
      total_nodes: 0,
      alive_nodes: 0,
      total_cpu_cores: 0,
      avg_cpu_cores: 0,
    };
  }

  getDefaultFEConfig() {
    return {
      lake_compaction_max_tasks: null,
      mode: 'UNKNOWN',
      is_adaptive: false,
      is_disabled: false,
      error: 'Unable to retrieve configuration',
    };
  }

  // ============= 辅助计算方法 =============

  calculateVariance(numbers) {
    if (numbers.length <= 1) return 0;
    const mean = numbers.reduce((sum, n) => sum + n, 0) / numbers.length;
    const variance =
      numbers.reduce((sum, n) => sum + Math.pow(n - mean, 2), 0) /
      numbers.length;
    return Math.round(variance * 100) / 100;
  }

  calculateClusterLoadLevel(runningTasks, aliveNodes, csStats) {
    const tasksPerNode = aliveNodes > 0 ? runningTasks / aliveNodes : 0;
    const highCSPartitions =
      (csStats.critical_partitions || 0) + (csStats.emergency_partitions || 0);

    if (tasksPerNode > 8 || highCSPartitions > 50) return 'HIGH';
    if (tasksPerNode > 5 || highCSPartitions > 20) return 'MEDIUM';
    return 'LOW';
  }

  calculateThreadUtilization(data) {
    // 计算线程使用率
    const runningTasks = data.running_tasks?.tasks?.length || 0;
    const threadStats = data.thread_config?.cluster_stats || {};
    const totalThreads = threadStats.total_threads || 1;

    // 基于正在运行的任务数估算线程使用率
    const utilization = Math.min((runningTasks / totalThreads) * 100, 100);

    return {
      current_utilization_percent: Math.round(utilization * 100) / 100,
      running_tasks: runningTasks,
      total_available_threads: totalThreads,
      efficiency_rating:
        utilization > 80 ? 'HIGH' : utilization > 50 ? 'MEDIUM' : 'LOW',
    };
  }

  assessThreadOptimizationPotential(threadStats, clusterStats) {
    // 评估线程优化潜力
    const avgThreadsPerCore = threadStats.avg_threads_per_core || 0;
    const totalNodes = clusterStats?.total_nodes || 1;
    const avgCoresPerNode = clusterStats?.avg_cores_per_node || 4;

    let optimizationLevel = 'LOW';
    let recommendations = [];

    // 评估当前配置效率
    if (avgThreadsPerCore < 0.25) {
      optimizationLevel = 'HIGH';
      recommendations.push('考虑增加线程数以充分利用CPU资源');
    } else if (avgThreadsPerCore > 0.75) {
      optimizationLevel = 'MEDIUM';
      recommendations.push('线程数可能过高，可考虑适当降低');
    } else {
      optimizationLevel = 'LOW';
      recommendations.push('当前线程配置相对合理');
    }

    return {
      optimization_level: optimizationLevel,
      current_threads_per_core: avgThreadsPerCore,
      recommended_threads_per_core: '0.25-0.5',
      estimated_improvement:
        optimizationLevel === 'HIGH'
          ? '20-40%'
          : optimizationLevel === 'MEDIUM'
            ? '10-20%'
            : '0-5%',
      specific_recommendations: recommendations,
    };
  }

  assessAdaptiveModeEffectiveness(adaptiveMaxTasks, highCSPartitions) {
    // 评估自适应模式的有效性
    const taskToPartitionRatio =
      highCSPartitions > 0 ? adaptiveMaxTasks / highCSPartitions : 1;

    let effectiveness = 'MEDIUM';
    let reasons = [];

    if (taskToPartitionRatio >= 0.5) {
      effectiveness = 'HIGH';
      reasons.push('自适应配置能有效处理当前高CS分区数量');
    } else if (taskToPartitionRatio >= 0.3) {
      effectiveness = 'MEDIUM';
      reasons.push('自适应配置基本满足处理需求');
    } else {
      effectiveness = 'LOW';
      reasons.push('自适应配置可能无法及时处理所有高CS分区');
    }

    return {
      effectiveness_level: effectiveness,
      task_to_partition_ratio: Math.round(taskToPartitionRatio * 100) / 100,
      assessment_reasons: reasons,
      recommended_action:
        effectiveness === 'LOW'
          ? '考虑增加集群节点或调整Compaction策略'
          : '当前配置合适',
    };
  }

  calculateCompactionGrowthProjection(compactionData) {
    // 基于当前数据预测增长趋势（简化版本）
    const csStats = compactionData.cs_statistics || {};
    const totalHighCS =
      (csStats.warning_partitions || 0) +
      (csStats.critical_partitions || 0) +
      (csStats.emergency_partitions || 0);

    return {
      current_high_cs_partitions: totalHighCS,
      projected_monthly_growth: Math.ceil(totalHighCS * 0.1), // 假设月增长10%
      capacity_needed: Math.ceil(totalHighCS * 1.5), // 需要1.5倍处理能力
      scaling_timeline: totalHighCS > 100 ? '1-2个月内' : '3-6个月内',
    };
  }

  identifyCompactionBottlenecks(compactionData) {
    // 识别Compaction瓶颈
    const bottlenecks = [];
    const csStats = compactionData.cs_statistics || {};
    const threadConfig = compactionData.thread_config || {};
    const runningTasks = compactionData.running_tasks || {};

    // 检查CS瓶颈
    if ((csStats.critical_partitions || 0) > 10) {
      bottlenecks.push({
        type: 'high_cs_accumulation',
        severity: 'HIGH',
        description: '高CS分区过多，可能存在Compaction效率问题',
        impact: '严重影响查询性能，可能导致查询超时',
      });
    }

    // 检查线程瓶颈
    if ((threadConfig.cluster_stats?.avg_threads_per_core || 0) < 0.25) {
      bottlenecks.push({
        type: 'thread_under_utilization',
        severity: 'MEDIUM',
        description: 'Compaction线程配置过低，无法充分利用CPU资源',
        impact: 'Compaction处理速度慢，可能导致CS积累',
      });
    }

    // 检查任务执行瓶颈
    const runningTaskCount = runningTasks.tasks?.length || 0;
    const totalThreads = threadConfig.cluster_stats?.total_threads || 1;
    if (runningTaskCount === 0 && (csStats.warning_partitions || 0) > 5) {
      bottlenecks.push({
        type: 'task_scheduling_issue',
        severity: 'HIGH',
        description: '存在高CS分区但没有运行中的Compaction任务',
        impact: 'Compaction任务可能未正常调度或执行',
      });
    }

    return {
      total_bottlenecks: bottlenecks.length,
      bottlenecks: bottlenecks,
      overall_assessment:
        bottlenecks.length === 0
          ? 'NO_MAJOR_BOTTLENECKS'
          : bottlenecks.some((b) => b.severity === 'HIGH')
            ? 'CRITICAL_BOTTLENECKS'
            : 'MINOR_BOTTLENECKS',
    };
  }

  performAdvancedDiagnosis(compactionData, analysisScope) {
    // 执行高级诊断分析
    return {
      bottleneck_analysis: this.identifyCompactionBottlenecks(compactionData),
      cs_distribution: this.analyzeCSDistribution(
        compactionData.cs_statistics || {},
      ),
      thread_utilization: this.calculateThreadUtilization(compactionData),
      cluster_utilization: {
        load_level: this.calculateClusterLoadLevel(
          compactionData.running_tasks?.tasks?.length || 0,
          compactionData.cluster_stats?.total_nodes || 1,
          compactionData.cs_statistics || {},
        ),
      },
      growth_projection:
        this.calculateCompactionGrowthProjection(compactionData),
      optimization_opportunities:
        this.identifyOptimizationOpportunities(compactionData),
    };
  }

  identifyOptimizationOpportunities(compactionData) {
    // 识别优化机会
    const opportunities = [];
    const csStats = compactionData.cs_statistics || {};
    const threadConfig = compactionData.thread_config || {};

    // CS优化机会
    if ((csStats.warning_partitions || 0) > 5) {
      opportunities.push({
        type: 'cs_optimization',
        priority: 'HIGH',
        description: '优化高CS分区处理策略',
        potential_impact: '显著改善查询性能',
      });
    }

    // 线程配置优化机会
    const avgThreadsPerCore =
      threadConfig.cluster_stats?.avg_threads_per_core || 0;
    if (avgThreadsPerCore < 0.25 || avgThreadsPerCore > 0.75) {
      opportunities.push({
        type: 'thread_optimization',
        priority: 'MEDIUM',
        description: '调整Compaction线程配置以匹配硬件资源',
        potential_impact: '提高Compaction效率',
      });
    }

    return {
      total_opportunities: opportunities.length,
      opportunities: opportunities,
      optimization_priority: opportunities.some((o) => o.priority === 'HIGH')
        ? 'HIGH'
        : 'MEDIUM',
    };
  }

  generateScalingRecommendation(compactionData) {
    // 生成扩展建议
    const csStats = compactionData.cs_statistics || {};
    const clusterStats = compactionData.cluster_stats || {};
    const totalHighCS =
      (csStats.warning_partitions || 0) +
      (csStats.critical_partitions || 0) +
      (csStats.emergency_partitions || 0);

    if (totalHighCS > 100) {
      return {
        scaling_needed: true,
        urgency: 'HIGH',
        recommended_action: '建议增加集群节点或优化Compaction配置',
        timeline: '1-2周内',
        expected_benefit: '显著降低CS积累速度',
      };
    } else if (totalHighCS > 50) {
      return {
        scaling_needed: true,
        urgency: 'MEDIUM',
        recommended_action: '考虑优化Compaction线程配置',
        timeline: '1个月内',
        expected_benefit: '改善CS处理效率',
      };
    }

    return {
      scaling_needed: false,
      recommendation: '当前规模合适，建议定期监控',
    };
  }

  /**
   * === 协调器兼容性适配器 ===
   * 提供与其他专家一致的接口
   */

  /**
   * 适配器方法：为协调器提供统一的 diagnose 接口
   */
  async diagnose(connection, includeDetails = false) {
    // 调用完整分析方法，并转换为协调器期望的格式
    const comprehensiveResult = await this.performComprehensiveAnalysis(
      connection,
      {
        includeDetailedData: includeDetails,
        analysisScope: 'full',
      },
    );

    // 转换为协调器期望的结果格式
    return {
      expert_type: 'compaction',
      expert_version: this.version,
      analysis_timestamp: comprehensiveResult.analysis_timestamp,
      analysis_duration_ms: comprehensiveResult.analysis_duration_ms,

      // 健康评估
      compaction_health: {
        score: comprehensiveResult.compaction_health.score,
        level: comprehensiveResult.compaction_health.level,
        status: comprehensiveResult.compaction_health.status,
      },

      // 诊断结果
      diagnosis_results: {
        total_issues: comprehensiveResult.diagnosis_results.total_issues,
        criticals: comprehensiveResult.diagnosis_results.criticals.map((c) => ({
          type: c.type,
          message: c.message,
          urgency: c.urgency,
          impact: c.impact,
        })),
        warnings: comprehensiveResult.diagnosis_results.warnings.map((w) => ({
          type: w.type,
          message: w.message,
        })),
        summary: comprehensiveResult.diagnosis_results.summary,
      },

      // 专业建议
      professional_recommendations:
        comprehensiveResult.comprehensive_recommendations || [],

      // 原始数据（如果请求详细信息）
      raw_data: includeDetails ? comprehensiveResult.collected_data : null,
    };
  }

  /**
   * analyze() 方法 - 兼容协调器调用
   * 这是 diagnose() 的别名，用于统一专家接口
   */
  async analyze(connection, options = {}) {
    const includeDetails = options.includeDetails || false;
    return await this.diagnose(connection, includeDetails);
  }

  /**
   * 获取专家描述信息（用于协调器）
   */
  get description() {
    return 'StarRocks Compaction 系统专家 - 集成所有压缩相关功能：CS管理、线程配置、任务监控、根因分析';
  }

  get version() {
    return '2.0.0';
  }

  /**
   * === 多维度诊断支持方法 ===
   */

  /**
   * 收集系统资源数据
   */
  async collectSystemResources(connection, data) {
    console.error('💻 收集系统资源数据...');

    try {
      // 获取BE节点系统资源信息
      const [beResources] = await connection.query(`
        SHOW BACKENDS;
      `);

      data.system_resources = {
        be_nodes: beResources.map((node) => ({
          backend_id: node.BackendId,
          host: node.Host,
          alive: node.Alive === 'true',
          cpu_cores: parseInt(node.CpuCores) || 8,
          mem_used_pct: parseFloat(node.MemUsedPct) || 0,
          cpu_usage_pct: parseFloat(node.CpuUsage) || 0,
          disk_used_pct: parseFloat(node.MaxDiskUsedPct) || 0,
          net_in_rate: parseFloat(node.NetInRate) || 0,
          net_out_rate: parseFloat(node.NetOutRate) || 0,
        })),
        collection_time: new Date().toISOString(),
      };

      // 计算集群级别资源统计
      const aliveNodes = data.system_resources.be_nodes.filter((n) => n.alive);
      data.system_resources.cluster_stats = {
        total_nodes: beResources.length,
        alive_nodes: aliveNodes.length,
        total_cpu_cores: aliveNodes.reduce((sum, n) => sum + n.cpu_cores, 0),
        avg_cpu_usage:
          aliveNodes.reduce((sum, n) => sum + n.cpu_usage_pct, 0) /
          aliveNodes.length,
        max_disk_usage: Math.max(...aliveNodes.map((n) => n.disk_used_pct)),
        avg_disk_usage:
          aliveNodes.reduce((sum, n) => sum + n.disk_used_pct, 0) /
          aliveNodes.length,
        avg_memory_usage:
          aliveNodes.reduce((sum, n) => sum + n.mem_used_pct, 0) /
          aliveNodes.length,
      };
    } catch (error) {
      console.warn('获取系统资源信息失败:', error.message);
      data.system_resources = this.getEmptySystemResources();
    }
  }

  /**
   * 收集参数配置数据
   */
  async collectParameterConfiguration(connection, data) {
    console.error('⚙️ 收集参数配置数据...');

    try {
      // 获取FE配置参数
      const [feConfigs] = await connection.query(`
        SHOW VARIABLES LIKE '%compact%';
      `);

      const beConfigMap = {};
      feConfigs.forEach((config) => {
        beConfigMap[config.Variable_name] = config.Value;
      });

      data.parameter_config = {
        fe_configs: beConfigMap,
        collection_time: new Date().toISOString(),
        critical_params: {
          max_compaction_tasks:
            parseInt(beConfigMap.max_compaction_tasks) || 10,
          compact_threads: parseInt(beConfigMap.compact_threads) || 2,
          compaction_lower_size_mbytes:
            parseInt(beConfigMap.compaction_lower_size_mbytes) || 256,
          compaction_upper_size_mbytes:
            parseInt(beConfigMap.compaction_upper_size_mbytes) || 1024,
        },
      };
    } catch (error) {
      console.warn('获取参数配置失败:', error.message);
      data.parameter_config = this.getEmptyParameterConfig();
    }
  }

  /**
   * 收集数据导入模式数据
   */
  async collectDataIngestionPatterns(connection, data) {
    console.error('📥 收集数据导入模式数据...');

    try {
      // 分析表统计信息推断导入模式
      const [tableStats] = await connection.query(`
        SELECT
          table_schema as database_name,
          table_name,
          table_rows,
          data_length,
          index_length,
          create_time,
          update_time
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        AND table_rows > 0
        ORDER BY table_rows DESC
        LIMIT 20
      `);

      // 基于表统计信息推断导入模式
      data.ingestion_patterns = {
        active_tables: tableStats.map((table) => ({
          database_name: table.database_name,
          table_name: table.table_name,
          estimated_size_mb: Math.round(
            (table.data_length + table.index_length) / 1024 / 1024,
          ),
          row_count: table.table_rows,
          last_update: table.update_time,
          estimated_ingestion_pattern: this.inferIngestionPattern(
            table.table_rows,
            table.data_length,
          ),
        })),
        analysis_summary: {
          total_analyzed_tables: tableStats.length,
          total_estimated_data_gb: Math.round(
            tableStats.reduce(
              (sum, t) => sum + (t.data_length + t.index_length),
              0,
            ) /
              1024 /
              1024 /
              1024,
          ),
          large_tables: tableStats.filter(
            (t) => t.data_length + t.index_length > 1024 * 1024 * 1024,
          ).length,
          high_row_count_tables: tableStats.filter(
            (t) => t.table_rows > 1000000,
          ).length,
        },
      };
    } catch (error) {
      console.warn('获取导入模式数据失败:', error.message);
      data.ingestion_patterns = this.getEmptyIngestionPatterns();
    }
  }

  /**
   * 辅助方法：推断导入模式
   */
  inferIngestionPattern(rowCount, dataLength) {
    const avgRowSize = rowCount > 0 ? dataLength / rowCount : 0;

    if (rowCount > 10000000 && avgRowSize > 1000) {
      return { pattern: 'LARGE_BATCH', frequency: 'LOW', concern_level: 'LOW' };
    } else if (rowCount > 1000000 && avgRowSize < 100) {
      return {
        pattern: 'HIGH_FREQUENCY_SMALL',
        frequency: 'HIGH',
        concern_level: 'HIGH',
      };
    } else if (rowCount > 100000) {
      return {
        pattern: 'MODERATE_BATCH',
        frequency: 'MEDIUM',
        concern_level: 'MEDIUM',
      };
    } else {
      return {
        pattern: 'SMALL_TABLE',
        frequency: 'UNKNOWN',
        concern_level: 'LOW',
      };
    }
  }

  /**
   * 获取空的系统资源数据
   */
  getEmptySystemResources() {
    return {
      be_nodes: [],
      cluster_stats: {
        total_nodes: 0,
        alive_nodes: 0,
        total_cpu_cores: 0,
        avg_cpu_usage: 0,
        max_disk_usage: 0,
        avg_disk_usage: 0,
        avg_memory_usage: 0,
      },
      collection_time: new Date().toISOString(),
    };
  }

  /**
   * 获取空的参数配置数据
   */
  getEmptyParameterConfig() {
    return {
      fe_configs: {},
      critical_params: {
        max_compaction_tasks: 10,
        compact_threads: 2,
        compaction_lower_size_mbytes: 256,
        compaction_upper_size_mbytes: 1024,
      },
      collection_time: new Date().toISOString(),
    };
  }

  /**
   * 获取空的导入模式数据
   */
  getEmptyIngestionPatterns() {
    return {
      active_tables: [],
      analysis_summary: {
        total_analyzed_tables: 0,
        total_estimated_data_gb: 0,
        large_tables: 0,
        high_row_count_tables: 0,
      },
    };
  }

  /**
   * === 多维度诊断方法 ===
   */

  /**
   * 系统资源诊断
   */
  diagnoseSystemResources(compactionData, diagnosis) {
    console.error('💻 执行系统资源诊断...');

    const resources = compactionData.system_resources;
    if (!resources || !resources.cluster_stats) {
      return;
    }

    const stats = resources.cluster_stats;

    // 磁盘使用率诊断
    if (stats.max_disk_usage > 95) {
      diagnosis.criticals.push({
        type: 'critical_disk_usage',
        severity: 'CRITICAL',
        urgency: 'IMMEDIATE',
        message: `磁盘使用率达到${stats.max_disk_usage.toFixed(1)}%，严重影响Compaction执行`,
        impact: {
          compaction: 'Compaction任务可能因磁盘空间不足而失败或延迟',
          performance: '查询性能严重下降，可能出现服务不可用',
          business: '业务连续性面临威胁',
        },
        recommended_actions: [
          '立即清理临时文件和日志',
          '删除不必要的数据或归档历史数据',
          '紧急扩容磁盘空间',
          '暂停非关键数据导入',
        ],
        estimated_resolution_time: '30分钟-2小时',
        monitoring_commands: [
          'df -h  # 检查磁盘使用情况',
          'du -sh /path/to/starrocks/storage/*  # 检查数据目录大小',
        ],
      });
    } else if (stats.max_disk_usage > 85) {
      diagnosis.warnings.push({
        type: 'high_disk_usage',
        severity: 'WARNING',
        message: `磁盘使用率达到${stats.max_disk_usage.toFixed(1)}%，可能影响Compaction效率`,
        impact: 'Compaction执行速度变慢，CS积累可能加速',
        recommended_actions: [
          '计划在24小时内清理磁盘空间',
          '制定数据清理和归档策略',
          '考虑磁盘扩容计划',
        ],
      });
    }

    // CPU使用率诊断
    if (stats.avg_cpu_usage > 90) {
      diagnosis.warnings.push({
        type: 'high_cpu_usage',
        severity: 'WARNING',
        message: `集群平均CPU使用率${stats.avg_cpu_usage.toFixed(1)}%，资源紧张`,
        impact: 'Compaction任务与其他任务争用CPU资源，可能影响执行效率',
        recommended_actions: [
          '检查是否有异常的高CPU查询',
          '考虑在低峰期执行Compaction',
          '优化查询负载分布',
        ],
      });
    }

    // 内存使用率诊断
    if (stats.avg_memory_usage > 85) {
      diagnosis.warnings.push({
        type: 'high_memory_usage',
        severity: 'WARNING',
        message: `集群平均内存使用率${stats.avg_memory_usage.toFixed(1)}%`,
        impact: '内存紧张可能导致Compaction任务OOM或性能下降',
        recommended_actions: [
          '检查内存消耗异常的查询',
          '调整查询并发度',
          '考虑内存扩容',
        ],
      });
    }

    // 节点存活性检查
    if (stats.alive_nodes < stats.total_nodes) {
      const deadNodes = stats.total_nodes - stats.alive_nodes;
      diagnosis.criticals.push({
        type: 'node_unavailability',
        severity: 'CRITICAL',
        message: `发现${deadNodes}个BE节点不可用`,
        impact: '集群容量降低，Compaction负载集中在少数节点上',
        recommended_actions: [
          '立即检查不可用节点状态',
          '重启故障节点或替换硬件',
          '评估是否需要临时调整副本数',
        ],
      });
    }
  }

  /**
   * 参数配置诊断
   */
  diagnoseParameterConfiguration(compactionData, diagnosis) {
    console.error('⚙️ 执行参数配置诊断...');

    const config = compactionData.parameter_config;
    if (!config || !config.critical_params) {
      return;
    }

    const params = config.critical_params;
    const resources = compactionData.system_resources?.cluster_stats;

    // 检查max_compaction_tasks
    if (params.max_compaction_tasks < 5) {
      diagnosis.criticals.push({
        type: 'max_compaction_tasks_too_low',
        severity: 'CRITICAL',
        message: `max_compaction_tasks设置过低(${params.max_compaction_tasks})`,
        current_value: params.max_compaction_tasks,
        recommended_value: '10-20',
        impact: 'Compaction并发度严重不足，无法及时处理高CS分区',
        fix_command: 'SET GLOBAL max_compaction_tasks = 15;',
        risk_assessment: 'LOW - 该参数调整风险很小',
      });
    } else if (params.max_compaction_tasks > 50) {
      diagnosis.warnings.push({
        type: 'max_compaction_tasks_too_high',
        severity: 'WARNING',
        message: `max_compaction_tasks设置过高(${params.max_compaction_tasks})`,
        impact: '可能导致资源争用，影响查询性能',
        fix_command: `SET GLOBAL max_compaction_tasks = ${Math.max(10, Math.floor((resources?.total_cpu_cores || 8) * 0.5))};`,
      });
    }

    // 检查compact_threads
    if (resources && resources.total_cpu_cores > 0) {
      const threadsPerCore = params.compact_threads / resources.total_cpu_cores;
      if (threadsPerCore < 0.2) {
        diagnosis.warnings.push({
          type: 'compact_threads_underutilized',
          severity: 'WARNING',
          message: `compact_threads配置保守，仅为CPU核心数的${(threadsPerCore * 100).toFixed(1)}%`,
          current_value: params.compact_threads,
          recommended_value: `${Math.floor(resources.total_cpu_cores * 0.4)}-${Math.floor(resources.total_cpu_cores * 0.6)}`,
          impact: 'CPU资源未充分利用，Compaction处理能力不足',
          fix_command: `SET GLOBAL compact_threads = ${Math.floor(resources.total_cpu_cores * 0.5)};`,
        });
      } else if (threadsPerCore > 1) {
        diagnosis.warnings.push({
          type: 'compact_threads_over_provisioned',
          severity: 'WARNING',
          message: `compact_threads配置过高，超过CPU核心数`,
          impact: '可能导致线程上下文切换开销，降低效率',
          fix_command: `SET GLOBAL compact_threads = ${Math.floor(resources.total_cpu_cores * 0.5)};`,
        });
      }
    }

    // 检查compaction_lower_size_mbytes
    if (params.compaction_lower_size_mbytes > 512) {
      diagnosis.warnings.push({
        type: 'compaction_lower_size_too_high',
        severity: 'WARNING',
        message: `compaction_lower_size_mbytes过高(${params.compaction_lower_size_mbytes}MB)`,
        impact: '小文件无法及时合并，增加查询文件数',
        recommended_value: '128-256MB',
        fix_command: 'SET GLOBAL compaction_lower_size_mbytes = 256;',
      });
    }
  }

  /**
   * 导入模式诊断
   */
  diagnoseIngestionPatterns(compactionData, diagnosis) {
    console.error('📥 执行导入模式诊断...');

    const patterns = compactionData.ingestion_patterns;
    if (!patterns || !patterns.active_tables) {
      return;
    }

    // 检查高关注度表的导入模式
    const highConcernTables = patterns.active_tables.filter(
      (table) => table.estimated_ingestion_pattern.concern_level === 'HIGH',
    );

    if (highConcernTables.length > 0) {
      diagnosis.warnings.push({
        type: 'problematic_ingestion_patterns',
        severity: 'WARNING',
        message: `发现${highConcernTables.length}个表采用可能导致高CS的导入模式`,
        affected_tables: highConcernTables.map(
          (t) => `${t.database_name}.${t.table_name}`,
        ),
        pattern_analysis: highConcernTables.map((table) => ({
          table: `${table.database_name}.${table.table_name}`,
          pattern: table.estimated_ingestion_pattern.pattern,
          concern:
            table.estimated_ingestion_pattern.pattern === 'HIGH_FREQUENCY_SMALL'
              ? '高频小批次导入，容易产生大量小文件'
              : '导入模式可能不利于Compaction效率',
        })),
        recommended_actions: [
          '调整导入策略：合并小批次为大批次',
          '使用Stream Load事务模式减少文件碎片',
          '设置合理的导入时间窗口',
          '考虑使用批量导入替代实时导入',
        ],
      });
    }

    // 检查数据总量
    if (patterns.analysis_summary.total_estimated_data_gb > 1000) {
      diagnosis.insights.push({
        type: 'large_data_volume_insight',
        message: `集群数据总量约${patterns.analysis_summary.total_estimated_data_gb}GB`,
        implication: '大数据量环境需要更积极的Compaction策略',
        recommendations: [
          '考虑增加Compaction线程数',
          '优化大表的分区策略',
          '制定数据生命周期管理策略',
        ],
      });
    }
  }

  /**
   * 计算Compaction效率分数
   */
  calculateCompactionEfficiencyScore(compactionData) {
    let score = 100;
    const threadConfig = compactionData.thread_config?.cluster_stats;
    const runningTasks = compactionData.running_tasks?.tasks;
    const csStats = compactionData.cs_statistics;

    // 基于线程利用率扣分
    if (threadConfig) {
      const threadsPerCore = threadConfig.avg_threads_per_core || 0;
      if (threadsPerCore < 0.25) score -= 20;
      else if (threadsPerCore > 0.75) score -= 10;
    }

    // 基于运行任务数扣分
    const taskCount = runningTasks?.length || 0;
    if (taskCount === 0 && (csStats?.warning_partitions || 0) > 5) {
      score -= 30; // 有高CS但没有运行任务
    }

    // 基于CS统计扣分
    if (csStats) {
      score -= (csStats.critical_partitions || 0) * 5;
      score -= (csStats.emergency_partitions || 0) * 10;
    }

    return {
      score: Math.max(0, Math.min(100, score)),
      level: score >= 80 ? 'HIGH' : score >= 60 ? 'MEDIUM' : 'LOW',
    };
  }

  /**
   * 跨维度关联分析
   */
  performCrossDimensionalAnalysis(compactionData, diagnosis) {
    console.error('🔗 执行跨维度关联分析...');

    const resources = compactionData.system_resources?.cluster_stats;
    const config = compactionData.parameter_config?.critical_params;
    const patterns = compactionData.ingestion_patterns?.analysis_summary;
    const csStats = compactionData.cs_statistics;

    if (!resources || !config || !csStats) {
      return;
    }

    // 复合原因1: 磁盘空间不足 + 线程配置不当 + 高CS积累
    if (
      resources.max_disk_usage > 85 &&
      config.compact_threads < resources.total_cpu_cores * 0.3 &&
      csStats.critical_partitions + csStats.emergency_partitions > 5
    ) {
      diagnosis.insights.push({
        type: 'compound_cause_disk_thread_cs',
        severity: 'HIGH',
        message: '发现复合原因：磁盘空间紧张+线程配置不足+高CS积累',
        explanation:
          '磁盘空间不足限制Compaction执行，线程配置保守进一步降低处理能力，导致CS急剧积累',
        impact_multiplier: 2.5,
        integrated_solution: {
          priority_order: [
            '1. 立即清理磁盘空间至75%以下',
            '2. 调整compact_threads至推荐值',
            '3. 批量处理紧急CS分区',
            '4. 监控CS下降趋势',
          ],
          expected_resolution_time: '2-4小时',
          success_metrics: [
            '磁盘使用率 < 75%',
            'CS积累速度 < 50/小时',
            '线程利用率 > 60%',
          ],
        },
      });
    }

    // 复合原因2: 高频导入 + 参数配置不当
    if (
      patterns &&
      patterns.total_estimated_data_gb > 100 &&
      config.compaction_lower_size_mbytes > 256 &&
      config.max_compaction_tasks < 10
    ) {
      diagnosis.insights.push({
        type: 'compound_cause_ingestion_config',
        severity: 'MEDIUM',
        message: '发现复合原因：大数据量+参数配置不当',
        explanation:
          '大数据量环境配合不当的Compaction参数，导致小文件积累和处理能力不足',
        integrated_solution: {
          priority_order: [
            '1. 调整compaction_lower_size_mbytes至256MB',
            '2. 增加max_compaction_tasks至15',
            '3. 优化导入批次大小',
            '4. 制定定期Compaction维护计划',
          ],
        },
      });
    }
  }

  /**
   * 生成专家洞察
   */
  generateExpertInsights(compactionData, diagnosis) {
    const insights = [];
    const totalIssues = diagnosis.criticals.length + diagnosis.warnings.length;

    if (totalIssues === 0) {
      insights.push({
        type: 'healthy_system_insight',
        message: 'Compaction系统运行健康',
        recommendation: '继续保持当前配置，建议定期检查',
      });
    } else if (diagnosis.criticals.length > 0) {
      insights.push({
        type: 'critical_issues_insight',
        message: `发现${diagnosis.criticals.length}个严重问题需要立即处理`,
        priority: 'IMMEDIATE',
        recommendation: '建议按照专家建议的优先级顺序逐一解决问题',
      });
    }

    // 基于跨维度分析的洞察
    if (diagnosis.insights?.length > 0) {
      insights.push({
        type: 'cross_dimensional_insight',
        message: '发现跨维度复合问题，需要综合解决',
        complexity: 'HIGH',
        recommendation: '建议采用集成解决方案，同时优化多个维度',
      });
    }

    return insights;
  }

  /**
   * 获取高 Compaction Score 分区
   */
  async getHighCompactionPartitions(connection, limit = 10, minScore = 100) {
    try {
      const [partitions] = await connection.query(
        `
        SELECT
          DB_NAME as database_name,
          TABLE_NAME as table_name,
          PARTITION_NAME as partition_name,
          MAX_CS as max_compaction_score,
          AVG_CS as avg_compaction_score,
          P50_CS as p50_compaction_score,
          ROW_COUNT as row_count,
          DATA_SIZE as data_size
        FROM information_schema.partitions_meta
        WHERE MAX_CS >= ?
        ORDER BY MAX_CS DESC
        LIMIT ?
      `,
        [minScore, limit],
      );

      return {
        success: true,
        data: {
          partitions: partitions,
          total_count: partitions.length,
          filters: {
            min_score: minScore,
            limit: limit,
          },
          analysis: this.analyzeHighCompactionPartitions(partitions),
        },
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve high compaction partitions: ${error.message}`,
        data: {
          partitions: [],
          total_count: 0,
        },
      };
    }
  }

  /**
   * 分析高 Compaction Score 分区
   */
  analyzeHighCompactionPartitions(partitions) {
    if (!partitions || partitions.length === 0) {
      return {
        summary: 'No high compaction score partitions found',
        severity: 'NORMAL',
        recommendations: [],
      };
    }

    const maxScore = Math.max(...partitions.map((p) => p.max_compaction_score));
    const avgScore =
      partitions.reduce((sum, p) => sum + p.max_compaction_score, 0) /
      partitions.length;

    let severity = 'NORMAL';
    let recommendations = [];

    if (maxScore >= this.rules.compaction_score.emergency) {
      severity = 'EMERGENCY';
      recommendations.push('立即进行手动 compaction，避免严重影响查询性能');
    } else if (maxScore >= this.rules.compaction_score.critical) {
      severity = 'CRITICAL';
      recommendations.push('优先处理高分区的 compaction 任务');
    } else if (maxScore >= this.rules.compaction_score.warning) {
      severity = 'WARNING';
      recommendations.push('监控分区状态，考虑在维护窗口进行 compaction');
    }

    if (partitions.length >= 10) {
      recommendations.push('检查 compaction 线程配置，可能需要增加并行度');
    }

    return {
      summary: `Found ${partitions.length} high compaction score partitions (max: ${maxScore}, avg: ${avgScore.toFixed(2)})`,
      severity: severity,
      max_score: maxScore,
      avg_score: avgScore,
      recommendations: recommendations,
      affected_tables: [
        ...new Set(partitions.map((p) => `${p.database_name}.${p.table_name}`)),
      ].length,
    };
  }

  /**
   * 获取 Compaction 线程配置
   */
  async getCompactionThreads(connection) {
    try {
      const [threadConfig] = await connection.query(`
        SELECT
          BE_ID as be_id,
          VALUE as thread_count
        FROM information_schema.be_configs
        WHERE name = 'compact_threads'
        ORDER BY BE_ID
      `);

      // 获取BE节点信息以便分析
      const [backends] = await connection.query('SHOW BACKENDS');

      const analysis = threadConfig.map((config) => {
        const beInfo = backends.find(
          (be) => be.BackendId === config.be_id.toString(),
        );
        const threads = parseInt(config.thread_count);
        const cpuCores = beInfo ? parseInt(beInfo.CpuCores) || 1 : 1;

        return {
          be_id: config.be_id,
          ip: beInfo ? beInfo.IP : 'Unknown',
          thread_count: threads,
          cpu_cores: cpuCores,
          threads_per_core: (threads / cpuCores).toFixed(2),
          recommended_min: Math.max(
            this.rules.thread_config.absolute_min_threads,
            Math.ceil(cpuCores * this.rules.thread_config.min_threads_per_core),
          ),
          recommended_max: Math.min(
            this.rules.thread_config.absolute_max_threads,
            Math.ceil(cpuCores * this.rules.thread_config.max_threads_per_core),
          ),
          status: this.evaluateThreadConfig(threads, cpuCores),
        };
      });

      return {
        success: true,
        data: {
          thread_configurations: analysis,
          cluster_summary: {
            total_nodes: analysis.length,
            total_threads: analysis.reduce((sum, a) => sum + a.thread_count, 0),
            total_cpu_cores: analysis.reduce((sum, a) => sum + a.cpu_cores, 0),
            avg_threads_per_core: (
              analysis.reduce(
                (sum, a) => sum + parseFloat(a.threads_per_core),
                0,
              ) / analysis.length
            ).toFixed(2),
          },
          analysis: this.analyzeThreadConfiguration(analysis),
        },
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve compaction thread configuration: ${error.message}`,
        data: {
          thread_configurations: [],
          cluster_summary: null,
        },
      };
    }
  }

  /**
   * 评估线程配置状态
   */
  evaluateThreadConfig(threads, cpuCores) {
    const threadsPerCore = threads / cpuCores;
    const minRecommended = this.rules.thread_config.min_threads_per_core;
    const maxRecommended = this.rules.thread_config.max_threads_per_core;

    if (threadsPerCore < minRecommended) {
      return 'LOW';
    } else if (threadsPerCore > maxRecommended) {
      return 'HIGH';
    } else {
      return 'OPTIMAL';
    }
  }

  /**
   * 分析线程配置
   */
  analyzeThreadConfiguration(analysis) {
    const lowConfigNodes = analysis.filter((a) => a.status === 'LOW');
    const highConfigNodes = analysis.filter((a) => a.status === 'HIGH');
    const optimalNodes = analysis.filter((a) => a.status === 'OPTIMAL');

    let summary = '';
    let recommendations = [];

    if (lowConfigNodes.length > 0) {
      summary += `${lowConfigNodes.length} 个节点线程配置偏低; `;
      recommendations.push('增加低配置节点的 compaction 线程数');
    }

    if (highConfigNodes.length > 0) {
      summary += `${highConfigNodes.length} 个节点线程配置偏高; `;
      recommendations.push('考虑降低高配置节点的线程数以节省资源');
    }

    if (optimalNodes.length === analysis.length) {
      summary = '所有节点线程配置都在最优范围内';
      recommendations.push('保持当前线程配置');
    }

    return {
      summary: summary.trim(),
      node_status: {
        optimal: optimalNodes.length,
        low: lowConfigNodes.length,
        high: highConfigNodes.length,
      },
      recommendations: recommendations,
    };
  }

  /**
   * 设置 Compaction 线程数
   */
  async setCompactionThreads(connection, threadCount) {
    try {
      // 获取所有BE节点
      const [backends] = await connection.query('SHOW BACKENDS');
      const results = [];

      for (const backend of backends) {
        try {
          await connection.query(`
            ADMIN SET be_config ("compact_threads" = "${threadCount}") FOR "${backend.IP}:${backend.HeartbeatPort}"
          `);
          results.push({
            be_id: backend.BackendId,
            ip: backend.IP,
            status: 'SUCCESS',
            previous_threads: null, // Would need to query before setting
            new_threads: threadCount,
          });
        } catch (error) {
          results.push({
            be_id: backend.BackendId,
            ip: backend.IP,
            status: 'FAILED',
            error: error.message,
          });
        }
      }

      const successCount = results.filter((r) => r.status === 'SUCCESS').length;
      const failureCount = results.filter((r) => r.status === 'FAILED').length;

      return {
        success: failureCount === 0,
        data: {
          operation: 'set_compaction_threads',
          target_thread_count: threadCount,
          results: results,
          summary: {
            total_nodes: backends.length,
            successful_updates: successCount,
            failed_updates: failureCount,
          },
        },
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to set compaction threads: ${error.message}`,
        data: {
          operation: 'set_compaction_threads',
          target_thread_count: threadCount,
          results: [],
        },
      };
    }
  }

  /**
   * 获取正在运行的 Compaction 任务
   */
  async getRunningCompactionTasks(connection, includeDetails = true) {
    try {
      const [tasks] = await connection.query(`
        SELECT
          BE_ID as be_id,
          TXN_ID as txn_id,
          TABLET_ID as tablet_id,
          VERSION as version,
          START_TIME as start_time,
          PROGRESS as progress,
          STATUS as status,
          RUNS as runs
        FROM information_schema.be_cloud_native_compactions
        WHERE START_TIME IS NOT NULL AND FINISH_TIME IS NULL
        ORDER BY START_TIME DESC
      `);

      const taskAnalysis = this.analyzeRunningTasks(tasks);

      return {
        success: true,
        data: {
          running_tasks: includeDetails
            ? tasks
            : tasks.map((t) => ({
                be_id: t.be_id,
                tablet_id: t.tablet_id,
                progress: t.progress,
                status: t.status,
              })),
          task_count: tasks.length,
          analysis: taskAnalysis,
        },
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve running compaction tasks: ${error.message}`,
        data: {
          running_tasks: [],
          task_count: 0,
        },
      };
    }
  }

  /**
   * 分析正在运行的任务
   */
  analyzeRunningTasks(tasks) {
    if (tasks.length === 0) {
      return {
        summary: 'No running compaction tasks found',
        status: 'IDLE',
      };
    }

    const now = new Date();
    const longRunningTasks = tasks.filter((task) => {
      const startTime = new Date(task.start_time);
      const runningHours = (now - startTime) / (1000 * 60 * 60);
      return runningHours > this.rules.task_execution.slow_task_threshold_hours;
    });

    const stalledTasks = tasks.filter(
      (task) => task.progress < 50 && task.runs > 5,
    );

    let status = 'NORMAL';
    if (stalledTasks.length > 0) {
      status = 'STALLED';
    } else if (longRunningTasks.length > 0) {
      status = 'SLOW';
    } else if (tasks.length > 20) {
      status = 'BUSY';
    }

    return {
      summary: `${tasks.length} running tasks, ${longRunningTasks.length} long-running, ${stalledTasks.length} potentially stalled`,
      status: status,
      long_running_count: longRunningTasks.length,
      stalled_count: stalledTasks.length,
      avg_progress:
        tasks.length > 0
          ? (
              tasks.reduce((sum, t) => sum + t.progress, 0) / tasks.length
            ).toFixed(1)
          : 0,
    };
  }

  /**
   * 分析高 Compaction Score 原因
   */
  async analyzeHighCompactionScore(
    connection,
    targetDatabase = null,
    minScore = 100,
  ) {
    try {
      let query = `
        SELECT
          DB_NAME as database_name,
          TABLE_NAME as table_name,
          PARTITION_NAME as partition_name,
          MAX_CS as max_compaction_score,
          AVG_CS as avg_compaction_score,
          ROW_COUNT as row_count,
          DATA_SIZE as data_size
        FROM information_schema.partitions_meta
        WHERE MAX_CS >= ?
      `;

      const params = [minScore];

      if (targetDatabase) {
        query += ` AND DB_NAME = ?`;
        params.push(targetDatabase);
      }

      query += ` ORDER BY MAX_CS DESC LIMIT 50`;

      const [partitions] = await connection.query(query, params);

      const analysis = this.performCompactionScoreAnalysis(partitions);

      return {
        success: true,
        data: {
          high_score_partitions: partitions,
          analysis: analysis,
          recommendations:
            this.generateCompactionScoreRecommendations(analysis),
        },
      };
    } catch (error) {
      const errorAnalysis = {
        summary: 'Analysis failed',
        severity: 'ERROR',
        statistics: {
          max_score: 0,
          avg_score: 0,
          total_partitions: 0,
          affected_databases: 0,
          affected_tables: 0,
        },
        by_database: [],
        by_table: [],
      };

      return {
        success: false,
        error: `Failed to analyze high compaction scores: ${error.message}`,
        data: {
          high_score_partitions: [],
          analysis: errorAnalysis,
          recommendations:
            this.generateCompactionScoreRecommendations(errorAnalysis),
        },
      };
    }
  }

  /**
   * 执行 Compaction Score 分析
   */
  performCompactionScoreAnalysis(partitions) {
    if (partitions.length === 0) {
      return {
        summary: 'No high compaction score partitions found',
        severity: 'NORMAL',
        statistics: {
          max_score: 0,
          avg_score: 0,
          total_partitions: 0,
          affected_databases: 0,
          affected_tables: 0,
        },
        by_database: [],
        by_table: [],
      };
    }

    const scores = partitions.map((p) => p.max_compaction_score);
    const maxScore = Math.max(...scores);
    const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;

    // 按数据库分组分析
    const byDatabase = {};
    partitions.forEach((p) => {
      if (!byDatabase[p.database_name]) {
        byDatabase[p.database_name] = [];
      }
      byDatabase[p.database_name].push(p);
    });

    // 按表分组分析
    const byTable = {};
    partitions.forEach((p) => {
      const tableKey = `${p.database_name}.${p.table_name}`;
      if (!byTable[tableKey]) {
        byTable[tableKey] = [];
      }
      byTable[tableKey].push(p);
    });

    let severity = 'NORMAL';
    if (maxScore >= this.rules.compaction_score.emergency) {
      severity = 'EMERGENCY';
    } else if (maxScore >= this.rules.compaction_score.critical) {
      severity = 'CRITICAL';
    } else if (maxScore >= this.rules.compaction_score.warning) {
      severity = 'WARNING';
    }

    return {
      summary: `Found ${partitions.length} high CS partitions across ${Object.keys(byDatabase).length} databases`,
      severity: severity,
      statistics: {
        max_score: maxScore,
        avg_score: avgScore.toFixed(2),
        total_partitions: partitions.length,
        affected_databases: Object.keys(byDatabase).length,
        affected_tables: Object.keys(byTable).length,
      },
      by_database: Object.entries(byDatabase).map(([db, parts]) => ({
        database: db,
        partition_count: parts.length,
        max_score: Math.max(...parts.map((p) => p.max_compaction_score)),
        avg_score: (
          parts.reduce((sum, p) => sum + p.max_compaction_score, 0) /
          parts.length
        ).toFixed(2),
      })),
      by_table: Object.entries(byTable).map(([table, parts]) => ({
        table: table,
        partition_count: parts.length,
        max_score: Math.max(...parts.map((p) => p.max_compaction_score)),
        avg_score: (
          parts.reduce((sum, p) => sum + p.max_compaction_score, 0) /
          parts.length
        ).toFixed(2),
      })),
    };
  }

  /**
   * 生成 Compaction Score 建议
   */
  generateCompactionScoreRecommendations(analysis) {
    const recommendations = [];

    // 添加空值检查
    if (!analysis || !analysis.statistics) {
      return [
        {
          priority: 'INFO',
          action: '无分析数据',
          reason: '无法生成建议',
        },
      ];
    }

    if (analysis.severity === 'EMERGENCY') {
      recommendations.push({
        priority: 'URGENT',
        action: '立即手动触发最高 CS 分区的 compaction',
        reason: '防止查询性能严重下降',
      });
    }

    if (analysis.severity === 'CRITICAL' || analysis.severity === 'EMERGENCY') {
      recommendations.push({
        priority: 'HIGH',
        action: '增加 compaction 线程数',
        reason: '提高 compaction 处理能力',
      });
    }

    if (analysis.statistics && analysis.statistics.affected_tables > 10) {
      recommendations.push({
        priority: 'MEDIUM',
        action: '制定分批 compaction 计划',
        reason: '避免同时处理过多表影响系统性能',
      });
    }

    recommendations.push({
      priority: 'LOW',
      action: '建立 CS 监控告警',
      reason: '及早发现和处理高 CS 问题',
    });

    return recommendations;
  }

  /**
   * 手动触发分区 Compaction
   */
  async compactPartition(connection, database, table, partition) {
    try {
      await connection.query(`
        ALTER TABLE \`${database}\`.\`${table}\` COMPACT PARTITION (\`${partition}\`)
      `);

      return {
        success: true,
        data: {
          operation: 'compact_partition',
          database: database,
          table: table,
          partition: partition,
          status: 'INITIATED',
          message: `Compaction initiated for partition ${database}.${table}.${partition}`,
        },
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to compact partition: ${error.message}`,
        data: {
          operation: 'compact_partition',
          database: database,
          table: table,
          partition: partition,
          status: 'FAILED',
        },
      };
    }
  }

  /**
   * 获取此专家提供的 MCP 工具处理器
   * @returns {Object} 工具名称到处理函数的映射
   */
  getToolHandlers() {
    return {
      get_table_partitions_compaction_score: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        const data = {};
        await this.collectTableSpecificData(connection, data, {
          targetDatabase: args.database_name,
          targetTable: args.table_name,
        });

        const partitions = data.target_table_analysis?.partitions || [];
        const scoreThreshold = args.score_threshold || 0;

        const filteredPartitions = partitions.filter(
          (partition) => partition.max_cs >= scoreThreshold,
        );

        return {
          database: args.database_name,
          table: args.table_name,
          score_threshold: scoreThreshold,
          total_partitions: partitions.length,
          filtered_partitions: filteredPartitions.length,
          partitions: filteredPartitions.map((partition) => ({
            partition_name: partition.partition,
            max_compaction_score: partition.max_cs,
            avg_compaction_score: partition.avg_cs,
            p50_compaction_score: partition.p50_cs,
            row_count: partition.row_count,
            data_size: partition.data_size,
            storage_size: partition.storage_size,
            buckets: partition.buckets,
            replication_num: partition.replication_num,
          })),
        };
      },
      get_high_compaction_partitions: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        const limit = args.limit || 50;
        const threshold = args.threshold || 100;
        return await this.getHighCompactionPartitions(
          connection,
          limit,
          threshold,
        );
      },
      get_compaction_threads: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        return await this.getCompactionThreads(connection);
      },
      set_compaction_threads: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        return await this.setCompactionThreads(connection, args.thread_count);
      },
      get_running_compaction_tasks: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        const includeDetails = args.include_details !== false;
        return await this.getRunningCompactionTasks(connection, includeDetails);
      },
      analyze_high_compaction_score: async (args, context) => {
        const connection = context.connection;

        // 检查集群架构
        await this.checkSharedDataArchitecture(connection);

        return await this.analyzeHighCompactionScore(
          connection,
          args.database_name || null,
          args.include_details !== false,
        );
      },
    };
  }

  /**
   * 获取此专家提供的 MCP 工具定义
   */
  getTools() {
    return [
      {
        name: 'get_table_partitions_compaction_score',
        description: '🔍 查询指定表的所有分区 Compaction Score',
        inputSchema: {
          type: 'object',
          properties: {
            database_name: {
              type: 'string',
              description: '数据库名称',
            },
            table_name: {
              type: 'string',
              description: '表名称',
            },
          },
          required: ['database_name', 'table_name'],
        },
      },
      {
        name: 'get_high_compaction_partitions',
        description: '⚠️ 查找系统中 Compaction Score 较高的分区（默认 >= 100）',
        inputSchema: {
          type: 'object',
          properties: {
            threshold: {
              type: 'number',
              description: 'Compaction Score 阈值（默认100）',
              default: 100,
            },
            limit: {
              type: 'number',
              description: '返回结果数量限制（默认50）',
              default: 50,
            },
          },
          required: [],
        },
      },
      {
        name: 'get_compaction_threads',
        description: '🔧 查询所有 BE 节点的 Compaction 线程配置',
        inputSchema: {
          type: 'object',
          properties: {},
          required: [],
        },
      },
      {
        name: 'set_compaction_threads',
        description: '⚙️ 设置指定 BE 节点的 Compaction 线程数',
        inputSchema: {
          type: 'object',
          properties: {
            be_id: {
              type: 'string',
              description: 'BE 节点 ID',
            },
            thread_count: {
              type: 'number',
              description: '线程数量',
            },
          },
          required: ['be_id', 'thread_count'],
        },
      },
      {
        name: 'get_running_compaction_tasks',
        description: '📊 查询当前正在运行的 Compaction 任务',
        inputSchema: {
          type: 'object',
          properties: {},
          required: [],
        },
      },
      {
        name: 'analyze_high_compaction_score',
        description: '🎯 深度分析高 Compaction Score 问题并提供专业建议',
        inputSchema: {
          type: 'object',
          properties: {
            database_name: {
              type: 'string',
              description: '可选：目标数据库名称',
            },
            table_name: {
              type: 'string',
              description: '可选：目标表名称',
            },
            include_details: {
              type: 'boolean',
              description: '是否包含详细分析数据',
              default: true,
            },
          },
          required: [],
        },
      },
    ];
  }
}

export { StarRocksCompactionExpert };
