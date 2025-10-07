/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * StarRocks Ingestion 专家模块
 * 负责：数据摄入分析、Stream Load/Broker Load/Routine Load 诊断、导入性能优化等
 */

/* eslint-disable no-undef, @typescript-eslint/no-unused-vars */

class StarRocksIngestionExpert {
  constructor() {
    this.name = 'ingestion';
    this.version = '1.0.0';
    this.description =
      'StarRocks Ingestion 系统专家 - 负责数据摄入问题诊断、性能分析、任务监控等';

    // Import专业知识规则库
    this.rules = {
      // Stream Load 规则
      stream_load: {
        max_file_size_mb: 10 * 1024, // 10GB
        recommended_batch_size_mb: 100, // 100MB
        timeout_seconds: 3600, // 1小时
        max_filter_ratio: 0.1, // 10% 错误率阈值
      },

      // Broker Load 规则
      broker_load: {
        max_parallelism: 5,
        load_timeout_seconds: 14400, // 4小时
        recommended_file_size_mb: 1024, // 1GB per file
        max_error_number: 1000,
      },

      // Routine Load 规则
      routine_load: {
        max_lag_time_seconds: 300, // 5分钟延迟阈值
        recommended_task_consume_second: 3,
        max_batch_interval_seconds: 20,
        max_batch_rows: 200000,
        max_batch_size_mb: 100,
      },

      // Insert 规则
      insert_load: {
        recommended_batch_size: 1000,
        max_batch_size: 10000,
        timeout_seconds: 300,
      },

      // 性能阈值
      performance: {
        slow_load_threshold_seconds: 300,
        low_throughput_mb_per_second: 10,
        high_error_rate_percent: 5,
      },
    };

    // Import 相关术语
    this.terminology = {
      stream_load: 'Stream Load: 通过HTTP PUT同步导入数据，适合小批量实时导入',
      broker_load:
        'Broker Load: 通过Broker异步导入HDFS/S3数据，适合大批量历史数据',
      routine_load: 'Routine Load: 持续消费Kafka数据，适合实时流式导入',
      insert_load: 'Insert Load: 通过INSERT语句导入数据，适合少量数据插入',
      load_job: '导入作业，包含导入任务的所有信息和状态',
      error_hub: '错误数据中心，存储导入过程中的错误数据',
    };
  }

  /**
   * Import 系统综合分析（MCP 工具接口）
   */
  async analyze(connection, options = {}) {
    const includeDetails = options.includeDetails !== false;
    return await this.diagnose(connection, includeDetails);
  }

  /**
   * Import 系统综合诊断
   */
  async diagnose(connection, includeDetails = true) {
    try {
      const startTime = new Date();

      // 1. 收集Import相关数据
      const importData = await this.collectImportData(connection);

      // 2. 执行专业诊断分析
      const diagnosis = this.performImportDiagnosis(importData);

      // 3. 生成专业建议
      const recommendations = this.generateImportRecommendations(
        diagnosis,
        importData,
      );

      // 4. 计算Import健康分数
      const healthScore = this.calculateImportHealthScore(diagnosis);

      const endTime = new Date();
      const analysisTime = endTime - startTime;

      return {
        expert: this.name,
        version: this.version,
        timestamp: new Date().toISOString(),
        analysis_duration_ms: analysisTime,
        import_health: healthScore,
        diagnosis_results: diagnosis,
        professional_recommendations: recommendations,
        raw_data: includeDetails ? importData : null,
        optimization_suggestions:
          this.generateOptimizationSuggestions(importData),
      };
    } catch (error) {
      throw new Error(`Import专家诊断失败: ${error.message}`);
    }
  }

  /**
   * 收集Import相关数据
   */
  async collectImportData(connection) {
    const data = {};

    // 1. 获取最近的导入作业
    try {
      const [recentLoads] = await connection.query(`
        SELECT JOB_ID, LABEL, STATE, PROGRESS, TYPE, ETL_INFO, TASK_INFO, ERROR_MSG,
               CREATE_TIME, ETL_START_TIME, ETL_FINISH_TIME, LOAD_START_TIME, LOAD_FINISH_TIME,
               URL, JOB_DETAILS, TRACKING_URL, TRACKING_SQL, REJECTED_RECORD_PATH
        FROM information_schema.loads
        WHERE CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY CREATE_TIME DESC
        LIMIT 100;
      `);
      data.recent_loads = recentLoads;
    } catch (error) {
      console.warn('Failed to collect recent loads:', error.message);
      data.recent_loads = [];
    }

    // 2. 获取正在运行的导入任务
    try {
      const [runningLoads] = await connection.query(`
        SELECT JOB_ID, LABEL, STATE, PROGRESS, TYPE, CREATE_TIME, ETL_INFO
        FROM information_schema.loads
        WHERE STATE IN ('PENDING', 'ETL', 'LOADING')
        ORDER BY CREATE_TIME DESC;
      `);
      data.running_loads = runningLoads;
    } catch (error) {
      console.warn('Failed to collect running loads:', error.message);
      data.running_loads = [];
    }

    // 3. 获取失败的导入作业
    try {
      const [failedLoads] = await connection.query(`
        SELECT JOB_ID, LABEL, STATE, TYPE, ERROR_MSG, CREATE_TIME, JOB_DETAILS
        FROM information_schema.loads
        WHERE STATE = 'CANCELLED' AND CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY CREATE_TIME DESC
        LIMIT 50;
      `);
      data.failed_loads = failedLoads;
    } catch (error) {
      console.warn('Failed to collect failed loads:', error.message);
      data.failed_loads = [];
    }

    // 4. 获取Routine Load信息
    try {
      const [routineLoads] = await connection.query(`
        SELECT NAME, CREATE_TIME, PAUSE_TIME, END_TIME, TABLE_NAME, STATE,
               DATA_SOURCE_NAME, CURRENT_TASK_NUM, JOB_PROPERTIES, DATA_SOURCE_PROPERTIES,
               CUSTOM_PROPERTIES, STATISTIC, PROGRESS, TRACKING_SQL, OTHER_MSG
        FROM information_schema.routine_loads
        ORDER BY CREATE_TIME DESC;
      `);
      data.routine_loads = routineLoads;
    } catch (error) {
      console.warn('Failed to collect routine loads:', error.message);
      data.routine_loads = [];
    }

    // 5. 获取Stream Load统计
    try {
      const [streamLoadStats] = await connection.query(`
        SELECT COUNT(*) as total_jobs,
               SUM(CASE WHEN STATE = 'FINISHED' THEN 1 ELSE 0 END) as success_jobs,
               SUM(CASE WHEN STATE = 'CANCELLED' THEN 1 ELSE 0 END) as failed_jobs,
               AVG(CASE WHEN STATE = 'FINISHED' AND LOAD_FINISH_TIME IS NOT NULL
                   THEN UNIX_TIMESTAMP(LOAD_FINISH_TIME) - UNIX_TIMESTAMP(LOAD_START_TIME)
                   ELSE NULL END) as avg_load_time_seconds
        FROM information_schema.loads
        WHERE CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR) AND TYPE = 'STREAM LOAD';
      `);
      data.stream_load_stats = streamLoadStats[0] || {};
    } catch (error) {
      console.warn('Failed to collect stream load stats:', error.message);
      data.stream_load_stats = {};
    }

    // 6. 获取表的导入频率统计
    try {
      const [tableLoadStats] = await connection.query(`
        SELECT
          SUBSTRING_INDEX(SUBSTRING_INDEX(JOB_DETAILS, 'database=', -1), ',', 1) as database_name,
          SUBSTRING_INDEX(SUBSTRING_INDEX(JOB_DETAILS, 'table=', -1), ',', 1) as table_name,
          COUNT(*) as load_count,
          SUM(CASE WHEN STATE = 'FINISHED' THEN 1 ELSE 0 END) as success_count,
          SUM(CASE WHEN STATE = 'CANCELLED' THEN 1 ELSE 0 END) as failed_count
        FROM information_schema.loads
        WHERE CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        AND JOB_DETAILS LIKE '%database=%' AND JOB_DETAILS LIKE '%table=%'
        GROUP BY database_name, table_name
        ORDER BY load_count DESC
        LIMIT 20;
      `);
      data.table_load_stats = tableLoadStats;
    } catch (error) {
      console.warn('Failed to collect table load stats:', error.message);
      data.table_load_stats = [];
    }

    // 7. 分析Stream Load导入频率
    try {
      data.import_frequency_analysis =
        await this.analyzeImportFrequency(connection);
    } catch (error) {
      console.warn('Failed to analyze import frequency:', error.message);
      data.import_frequency_analysis = {};
    }

    return data;
  }

  /**
   * 分析Stream Load导入频率
   */
  async analyzeImportFrequency(connection) {
    const frequencyAnalysis = {
      tables: [],
      patterns: {},
      insights: [],
    };

    try {
      // 1. 从loads_history表分析导入频率
      let historyQuery = '';
      try {
        // 首先尝试使用loads_history表
        const [historyLoads] = await connection.query(`
          SELECT
            DATABASE_NAME,
            TABLE_NAME,
            CREATE_TIME,
            STATE,
            TYPE
          FROM information_schema.loads_history
          WHERE TYPE = 'STREAM LOAD'
            AND CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 7 DAY)
          ORDER BY DATABASE_NAME, TABLE_NAME, CREATE_TIME;
        `);
        historyQuery = 'loads_history';
        await this.processLoadHistoryData(historyLoads, frequencyAnalysis);
      } catch (historyError) {
        console.warn(
          'loads_history table not available, falling back to loads table',
        );

        // 如果loads_history不可用，尝试使用loads表
        const [currentLoads] = await connection.query(`
          SELECT
            SUBSTRING_INDEX(SUBSTRING_INDEX(COALESCE(JOB_DETAILS, LABEL), 'database=', -1), ',', 1) as DATABASE_NAME,
            SUBSTRING_INDEX(SUBSTRING_INDEX(COALESCE(JOB_DETAILS, LABEL), 'table=', -1), ',', 1) as TABLE_NAME,
            CREATE_TIME,
            STATE,
            TYPE
          FROM information_schema.loads
          WHERE TYPE = 'STREAM LOAD'
            AND CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 7 DAY)
          ORDER BY DATABASE_NAME, TABLE_NAME, CREATE_TIME;
        `);
        historyQuery = 'loads';
        await this.processLoadHistoryData(currentLoads, frequencyAnalysis);
      }

      // 2. 计算每个表的导入频率模式
      this.calculateFrequencyPatterns(frequencyAnalysis);

      // 3. 生成频率分析洞察
      this.generateFrequencyInsights(frequencyAnalysis);

      frequencyAnalysis.source_table = historyQuery;
    } catch (error) {
      console.warn('Error in import frequency analysis:', error.message);
    }

    return frequencyAnalysis;
  }

  /**
   * 处理导入历史数据
   */
  async processLoadHistoryData(loads, frequencyAnalysis) {
    const tableMap = new Map();

    // 按表分组处理数据
    loads.forEach((load) => {
      const key = `${load.DATABASE_NAME}.${load.TABLE_NAME}`;

      if (!tableMap.has(key)) {
        tableMap.set(key, {
          database: load.DATABASE_NAME,
          table: load.TABLE_NAME,
          loads: [],
          totalLoads: 0,
          successLoads: 0,
          failedLoads: 0,
        });
      }

      const tableData = tableMap.get(key);
      tableData.loads.push({
        create_time: load.CREATE_TIME,
        state: load.STATE,
        timestamp: new Date(load.CREATE_TIME).getTime(),
      });

      tableData.totalLoads++;
      if (load.STATE === 'FINISHED') {
        tableData.successLoads++;
      } else if (load.STATE === 'CANCELLED') {
        tableData.failedLoads++;
      }
    });

    // 为每个表计算频率统计
    for (const [key, tableData] of tableMap) {
      if (tableData.loads.length < 2) continue; // 至少需要2条记录才能分析频率

      // 按时间排序
      tableData.loads.sort((a, b) => a.timestamp - b.timestamp);

      // 计算导入间隔
      const intervals = [];
      for (let i = 1; i < tableData.loads.length; i++) {
        const interval =
          (tableData.loads[i].timestamp - tableData.loads[i - 1].timestamp) /
          1000; // 秒
        intervals.push(interval);
      }

      // 计算频率统计
      const avgInterval =
        intervals.reduce((sum, interval) => sum + interval, 0) /
        intervals.length;
      const minInterval = Math.min(...intervals);
      const maxInterval = Math.max(...intervals);

      // 计算标准差
      const variance =
        intervals.reduce(
          (sum, interval) => sum + Math.pow(interval - avgInterval, 2),
          0,
        ) / intervals.length;
      const stdDev = Math.sqrt(variance);

      // 确定频率模式
      const frequencyPattern = this.determineFrequencyPattern(
        avgInterval,
        stdDev,
        intervals,
      );

      const tableFrequency = {
        database: tableData.database,
        table: tableData.table,
        totalLoads: tableData.totalLoads,
        successLoads: tableData.successLoads,
        failedLoads: tableData.failedLoads,
        successRate: (
          (tableData.successLoads / tableData.totalLoads) *
          100
        ).toFixed(1),
        avgIntervalSeconds: Math.round(avgInterval),
        avgIntervalMinutes: Math.round(avgInterval / 60),
        avgIntervalHours: (avgInterval / 3600).toFixed(1),
        minIntervalSeconds: Math.round(minInterval),
        maxIntervalSeconds: Math.round(maxInterval),
        intervalStdDev: Math.round(stdDev),
        frequencyPattern: frequencyPattern,
        loadsPerHour: (3600 / avgInterval).toFixed(2),
        loadsPerDay: (86400 / avgInterval).toFixed(1),
        regularity: this.calculateRegularity(stdDev, avgInterval),
        timeSpan: {
          start: tableData.loads[0].create_time,
          end: tableData.loads[tableData.loads.length - 1].create_time,
          durationHours: (
            (tableData.loads[tableData.loads.length - 1].timestamp -
              tableData.loads[0].timestamp) /
            3600000
          ).toFixed(1),
        },
      };

      frequencyAnalysis.tables.push(tableFrequency);
    }

    // 按导入频率排序
    frequencyAnalysis.tables.sort(
      (a, b) => parseFloat(b.loadsPerHour) - parseFloat(a.loadsPerHour),
    );
  }

  /**
   * 确定频率模式
   */
  determineFrequencyPattern(avgInterval, stdDev, intervals) {
    const avgMinutes = avgInterval / 60;
    const cvPercent = (stdDev / avgInterval) * 100; // 变异系数

    let pattern = '';
    let regularity = '';

    // 确定频率类型
    if (avgMinutes < 1) {
      pattern = 'high-frequency'; // 高频：小于1分钟
    } else if (avgMinutes < 15) {
      pattern = 'frequent'; // 频繁：1-15分钟
    } else if (avgMinutes < 60) {
      pattern = 'moderate'; // 中等：15-60分钟
    } else if (avgMinutes < 240) {
      pattern = 'hourly'; // 小时级：1-4小时
    } else if (avgMinutes < 1440) {
      pattern = 'daily'; // 日级：4小时-1天
    } else {
      pattern = 'low-frequency'; // 低频：大于1天
    }

    // 确定规律性
    if (cvPercent < 20) {
      regularity = 'very-regular'; // 很规律
    } else if (cvPercent < 50) {
      regularity = 'regular'; // 规律
    } else if (cvPercent < 100) {
      regularity = 'irregular'; // 不规律
    } else {
      regularity = 'very-irregular'; // 很不规律
    }

    return {
      frequency: pattern,
      regularity: regularity,
      cvPercent: cvPercent.toFixed(1),
    };
  }

  /**
   * 计算规律性分数
   */
  calculateRegularity(stdDev, avgInterval) {
    const cv = stdDev / avgInterval;
    let score = Math.max(0, 100 - cv * 100);

    let level = '';
    if (score >= 80) level = 'very-regular';
    else if (score >= 60) level = 'regular';
    else if (score >= 40) level = 'somewhat-regular';
    else level = 'irregular';

    return {
      score: Math.round(score),
      level: level,
    };
  }

  /**
   * 计算频率模式统计
   */
  calculateFrequencyPatterns(frequencyAnalysis) {
    const patterns = {
      'high-frequency': { count: 0, tables: [] },
      frequent: { count: 0, tables: [] },
      moderate: { count: 0, tables: [] },
      hourly: { count: 0, tables: [] },
      daily: { count: 0, tables: [] },
      'low-frequency': { count: 0, tables: [] },
    };

    const regularityStats = {
      'very-regular': 0,
      regular: 0,
      irregular: 0,
      'very-irregular': 0,
    };

    frequencyAnalysis.tables.forEach((table) => {
      const pattern = table.frequencyPattern.frequency;
      const regularity = table.frequencyPattern.regularity;

      if (patterns[pattern]) {
        patterns[pattern].count++;
        patterns[pattern].tables.push(`${table.database}.${table.table}`);
      }

      if (regularityStats[regularity] !== undefined) {
        regularityStats[regularity]++;
      }
    });

    frequencyAnalysis.patterns = {
      frequency_distribution: patterns,
      regularity_distribution: regularityStats,
      total_tables: frequencyAnalysis.tables.length,
    };
  }

  /**
   * 生成频率分析洞察
   */
  generateFrequencyInsights(frequencyAnalysis) {
    const insights = [];
    const tables = frequencyAnalysis.tables;

    if (tables.length === 0) {
      insights.push({
        type: 'no_data',
        message: '未发现足够的Stream Load历史数据进行频率分析',
        recommendation: '建议增加数据导入活动或检查更长时间范围的数据',
      });
      frequencyAnalysis.insights = insights;
      return;
    }

    // 1. 高频导入表分析
    const highFreqTables = tables.filter(
      (t) => t.frequencyPattern.frequency === 'high-frequency',
    );
    if (highFreqTables.length > 0) {
      insights.push({
        type: 'high_frequency_import',
        message: `发现 ${highFreqTables.length} 个高频导入表（间隔<1分钟）`,
        tables: highFreqTables.slice(0, 5).map((t) => ({
          table: `${t.database}.${t.table}`,
          interval_seconds: t.avgIntervalSeconds,
          loads_per_hour: t.loadsPerHour,
        })),
        recommendation: '考虑合并小批次导入以提高效率，减少系统负载',
      });
    }

    // 2. 不规律导入模式分析
    const irregularTables = tables.filter((t) => t.regularity.score < 40);
    if (irregularTables.length > 0) {
      insights.push({
        type: 'irregular_import_pattern',
        message: `发现 ${irregularTables.length} 个导入模式不规律的表`,
        tables: irregularTables.slice(0, 5).map((t) => ({
          table: `${t.database}.${t.table}`,
          regularity_score: t.regularity.score,
          cv_percent: t.frequencyPattern.cvPercent,
        })),
        recommendation: '建议优化导入调度，建立更规律的导入模式',
      });
    }

    // 3. 导入成功率分析
    const lowSuccessTables = tables.filter(
      (t) => parseFloat(t.successRate) < 95,
    );
    if (lowSuccessTables.length > 0) {
      insights.push({
        type: 'low_success_rate',
        message: `发现 ${lowSuccessTables.length} 个表的导入成功率较低`,
        tables: lowSuccessTables.slice(0, 5).map((t) => ({
          table: `${t.database}.${t.table}`,
          success_rate: t.successRate + '%',
          total_loads: t.totalLoads,
          failed_loads: t.failedLoads,
        })),
        recommendation: '检查数据格式、网络连接和系统资源，提高导入成功率',
      });
    }

    // 4. 负载分布分析
    const totalLoadsPerHour = tables.reduce(
      (sum, t) => sum + parseFloat(t.loadsPerHour),
      0,
    );
    if (totalLoadsPerHour > 1000) {
      insights.push({
        type: 'high_system_load',
        message: `系统总导入负载较高：每小时 ${totalLoadsPerHour.toFixed(0)} 次导入`,
        metrics: {
          total_loads_per_hour: Math.round(totalLoadsPerHour),
          active_tables: tables.length,
          avg_loads_per_table: (totalLoadsPerHour / tables.length).toFixed(1),
        },
        recommendation: '监控系统资源使用，考虑优化导入调度或扩容',
      });
    }

    // 5. 最活跃表分析
    const topActiveTables = tables.slice(0, 3);
    if (topActiveTables.length > 0) {
      insights.push({
        type: 'most_active_tables',
        message: '最活跃的导入表',
        tables: topActiveTables.map((t) => ({
          table: `${t.database}.${t.table}`,
          loads_per_hour: t.loadsPerHour,
          frequency_pattern: t.frequencyPattern.frequency,
          regularity: t.regularity.level,
        })),
        recommendation: '重点监控这些活跃表的性能和资源使用情况',
      });
    }

    frequencyAnalysis.insights = insights;
  }

  /**
   * 执行Import专业诊断
   */
  performImportDiagnosis(data) {
    const issues = [];
    const warnings = [];
    const criticals = [];
    const insights = [];

    // 1. 诊断导入作业失败率
    this.diagnoseLoadFailureRate(data, issues, warnings, criticals);

    // 2. 诊断导入性能
    this.diagnoseLoadPerformance(data, warnings, insights);

    // 3. 诊断Routine Load状态
    this.diagnoseRoutineLoadHealth(data, warnings, criticals);

    // 4. 诊断导入作业堆积
    this.diagnoseLoadQueue(data, warnings, criticals);

    // 5. 诊断常见错误模式
    this.diagnoseCommonErrors(data, issues, warnings, criticals);

    // 6. 分析导入频率模式
    this.diagnoseImportFrequency(data, warnings, insights);

    return {
      total_issues: issues.length + warnings.length + criticals.length,
      criticals: criticals,
      warnings: warnings,
      issues: issues,
      insights: insights,
      summary: this.generateImportSummary(criticals, warnings, issues),
    };
  }

  /**
   * 诊断导入频率模式
   */
  diagnoseImportFrequency(data, warnings, insights) {
    const frequencyData = data.import_frequency_analysis;

    if (
      !frequencyData ||
      !frequencyData.tables ||
      frequencyData.tables.length === 0
    ) {
      insights.push({
        type: 'import_frequency_no_data',
        message: '导入频率分析：未发现足够的历史数据',
        recommendation: '建议检查数据源或扩大分析时间范围',
      });
      return;
    }

    // 1. 添加频率分析的洞察到总洞察中
    if (frequencyData.insights && frequencyData.insights.length > 0) {
      frequencyData.insights.forEach((insight) => {
        insights.push({
          type: `frequency_${insight.type}`,
          message: `导入频率分析：${insight.message}`,
          details: insight.tables || insight.metrics,
          recommendation: insight.recommendation,
        });
      });
    }

    // 2. 检查高频导入警告
    const highFreqTables = frequencyData.tables.filter(
      (t) => t.frequencyPattern.frequency === 'high-frequency',
    );

    if (highFreqTables.length > 3) {
      warnings.push({
        type: 'excessive_high_frequency_imports',
        severity: 'WARNING',
        message: `发现过多高频导入表 (${highFreqTables.length} 个)，可能影响系统性能`,
        affected_tables: highFreqTables.slice(0, 5).map((t) => ({
          table: `${t.database}.${t.table}`,
          loads_per_hour: t.loadsPerHour,
          avg_interval_seconds: t.avgIntervalSeconds,
        })),
        impact: '过多的高频导入可能导致系统负载过高和资源竞争',
        urgency: 'WITHIN_DAYS',
      });
    }

    // 3. 检查导入模式不规律的警告
    const irregularTables = frequencyData.tables.filter(
      (t) => t.regularity.score < 40,
    );

    if (irregularTables.length > frequencyData.tables.length * 0.5) {
      warnings.push({
        type: 'irregular_import_patterns',
        severity: 'WARNING',
        message: `超过半数表的导入模式不规律 (${irregularTables.length}/${frequencyData.tables.length})`,
        irregular_tables: irregularTables.slice(0, 5).map((t) => ({
          table: `${t.database}.${t.table}`,
          regularity_score: t.regularity.score,
          cv_percent: t.frequencyPattern.cvPercent,
        })),
        impact: '不规律的导入模式可能导致资源使用不均和性能波动',
        urgency: 'WITHIN_WEEKS',
      });
    }

    // 4. 添加频率统计洞察
    const patterns = frequencyData.patterns;
    if (patterns && patterns.total_tables > 0) {
      insights.push({
        type: 'import_frequency_statistics',
        message: '导入频率分布统计',
        statistics: {
          total_tables_analyzed: patterns.total_tables,
          frequency_distribution: patterns.frequency_distribution,
          regularity_distribution: patterns.regularity_distribution,
          data_source: frequencyData.source_table,
        },
        recommendations: this.generateFrequencyRecommendations(frequencyData),
      });
    }
  }

  /**
   * 生成频率相关建议
   */
  generateFrequencyRecommendations(frequencyData) {
    const recommendations = [];
    const patterns = frequencyData.patterns;

    if (!patterns) return recommendations;

    // 高频导入优化建议
    if (patterns.frequency_distribution['high-frequency'].count > 0) {
      recommendations.push('考虑合并高频导入批次，减少系统调用开销');
    }

    // 不规律导入优化建议
    if (
      patterns.regularity_distribution['irregular'] +
        patterns.regularity_distribution['very-irregular'] >
      patterns.total_tables * 0.3
    ) {
      recommendations.push('建立规律的导入调度机制，提高资源利用效率');
    }

    // 负载均衡建议
    if (
      patterns.frequency_distribution['frequent'].count >
      patterns.total_tables * 0.5
    ) {
      recommendations.push('监控系统负载，考虑在低峰期调度部分导入任务');
    }

    return recommendations.length > 0
      ? recommendations
      : ['当前导入频率模式合理，保持现有策略'];
  }

  /**
   * 诊断导入作业失败率
   */
  diagnoseLoadFailureRate(data, issues, warnings, criticals) {
    const stats = data.stream_load_stats;
    const recentLoads = data.recent_loads || [];

    if (stats.total_jobs > 0) {
      const failureRate = (stats.failed_jobs / stats.total_jobs) * 100;

      if (failureRate > 20) {
        criticals.push({
          type: 'high_load_failure_rate',
          severity: 'CRITICAL',
          message: `导入失败率过高: ${failureRate.toFixed(1)}%`,
          metrics: {
            total_jobs: stats.total_jobs,
            failed_jobs: stats.failed_jobs,
            failure_rate: failureRate.toFixed(1),
          },
          impact: '大量导入失败可能导致数据丢失和业务中断',
          urgency: 'IMMEDIATE',
        });
      } else if (failureRate > 10) {
        warnings.push({
          type: 'moderate_load_failure_rate',
          severity: 'WARNING',
          message: `导入失败率较高: ${failureRate.toFixed(1)}%`,
          metrics: {
            total_jobs: stats.total_jobs,
            failed_jobs: stats.failed_jobs,
            failure_rate: failureRate.toFixed(1),
          },
          impact: '需要关注导入质量，建议检查数据格式和配置',
          urgency: 'WITHIN_HOURS',
        });
      }
    }

    // 检查最近失败的作业
    const recentFailures = recentLoads.filter(
      (load) => load.STATE === 'CANCELLED',
    );
    if (recentFailures.length > 5) {
      warnings.push({
        type: 'frequent_load_failures',
        severity: 'WARNING',
        message: `最近24小时内有 ${recentFailures.length} 个导入作业失败`,
        affected_count: recentFailures.length,
        impact: '频繁的导入失败可能指示系统性问题',
        urgency: 'WITHIN_HOURS',
      });
    }
  }

  /**
   * 诊断导入性能
   */
  diagnoseLoadPerformance(data, warnings, insights) {
    const stats = data.stream_load_stats;

    if (
      stats.avg_load_time_seconds >
      this.rules.performance.slow_load_threshold_seconds
    ) {
      warnings.push({
        type: 'slow_load_performance',
        severity: 'WARNING',
        message: `平均导入时间过长: ${stats.avg_load_time_seconds.toFixed(1)} 秒`,
        metrics: {
          avg_load_time: stats.avg_load_time_seconds.toFixed(1),
          threshold: this.rules.performance.slow_load_threshold_seconds,
        },
        impact: '导入性能低下可能影响数据实时性',
        urgency: 'WITHIN_DAYS',
      });
    }

    // 分析表级别的导入模式
    const tableStats = data.table_load_stats || [];
    const highVolumeTable = tableStats.find((table) => table.load_count > 100);

    if (highVolumeTable) {
      insights.push({
        type: 'high_volume_import_analysis',
        message: '发现高频导入表',
        analysis: {
          table: `${highVolumeTable.database_name}.${highVolumeTable.table_name}`,
          load_count: highVolumeTable.load_count,
          success_rate: (
            (highVolumeTable.success_count / highVolumeTable.load_count) *
            100
          ).toFixed(1),
        },
        recommendations: [
          '考虑优化导入频率，合并小批次导入',
          '监控表的导入性能和资源使用',
          '评估是否需要调整表结构或分区策略',
        ],
      });
    }
  }

  /**
   * 诊断Routine Load健康状态
   */
  diagnoseRoutineLoadHealth(data, warnings, criticals) {
    const routineLoads = data.routine_loads || [];

    routineLoads.forEach((routine) => {
      if (routine.STATE === 'PAUSED') {
        warnings.push({
          type: 'routine_load_paused',
          severity: 'WARNING',
          message: `Routine Load作业 "${routine.NAME}" 处于暂停状态`,
          routine_name: routine.NAME,
          table_name: routine.TABLE_NAME,
          pause_time: routine.PAUSE_TIME,
          impact: '流式导入中断可能导致数据延迟',
          urgency: 'WITHIN_HOURS',
        });
      } else if (routine.STATE === 'CANCELLED') {
        criticals.push({
          type: 'routine_load_cancelled',
          severity: 'CRITICAL',
          message: `Routine Load作业 "${routine.NAME}" 已被取消`,
          routine_name: routine.NAME,
          table_name: routine.TABLE_NAME,
          end_time: routine.END_TIME,
          error_msg: routine.OTHER_MSG,
          impact: '流式导入停止，可能导致数据丢失',
          urgency: 'IMMEDIATE',
        });
      }
    });
  }

  /**
   * 诊断导入作业堆积
   */
  diagnoseLoadQueue(data, warnings, criticals) {
    const runningLoads = data.running_loads || [];
    const pendingLoads = runningLoads.filter(
      (load) => load.STATE === 'PENDING',
    );

    if (pendingLoads.length > 10) {
      criticals.push({
        type: 'load_queue_backlog',
        severity: 'CRITICAL',
        message: `导入队列积压严重，有 ${pendingLoads.length} 个作业等待执行`,
        pending_count: pendingLoads.length,
        impact: '导入队列积压可能导致数据延迟和超时',
        urgency: 'IMMEDIATE',
      });
    } else if (pendingLoads.length > 5) {
      warnings.push({
        type: 'load_queue_buildup',
        severity: 'WARNING',
        message: `导入队列有 ${pendingLoads.length} 个作业等待执行`,
        pending_count: pendingLoads.length,
        impact: '需要监控导入队列，避免进一步积压',
        urgency: 'WITHIN_HOURS',
      });
    }

    // 检查长时间运行的作业
    const now = new Date();
    const longRunningLoads = runningLoads.filter((load) => {
      if (!load.CREATE_TIME) return false;
      const createTime = new Date(load.CREATE_TIME);
      const runningHours = (now - createTime) / (1000 * 60 * 60);
      return runningHours > 2; // 超过2小时
    });

    if (longRunningLoads.length > 0) {
      warnings.push({
        type: 'long_running_loads',
        severity: 'WARNING',
        message: `发现 ${longRunningLoads.length} 个长时间运行的导入作业`,
        long_running_jobs: longRunningLoads.map((load) => ({
          job_id: load.JOB_ID,
          label: load.LABEL,
          state: load.STATE,
          type: load.TYPE,
          running_hours:
            Math.round(
              ((now - new Date(load.CREATE_TIME)) / (1000 * 60 * 60)) * 10,
            ) / 10,
        })),
        impact: '长时间运行的作业可能阻塞导入队列',
        urgency: 'WITHIN_HOURS',
      });
    }
  }

  /**
   * 诊断常见错误模式
   */
  diagnoseCommonErrors(data, issues, warnings, criticals) {
    const failedLoads = data.failed_loads || [];

    // 统计错误类型
    const errorPatterns = {};
    failedLoads.forEach((load) => {
      if (load.ERROR_MSG) {
        // 简化错误信息提取关键词
        let errorType = 'unknown';
        const errorMsg = load.ERROR_MSG.toLowerCase();

        if (errorMsg.includes('timeout')) {
          errorType = 'timeout';
        } else if (errorMsg.includes('format') || errorMsg.includes('parse')) {
          errorType = 'format_error';
        } else if (
          errorMsg.includes('permission') ||
          errorMsg.includes('access')
        ) {
          errorType = 'permission_error';
        } else if (errorMsg.includes('memory') || errorMsg.includes('oom')) {
          errorType = 'memory_error';
        } else if (errorMsg.includes('duplicate') || errorMsg.includes('key')) {
          errorType = 'duplicate_key';
        }

        errorPatterns[errorType] = (errorPatterns[errorType] || 0) + 1;
      }
    });

    // 分析错误模式
    Object.entries(errorPatterns).forEach(([errorType, count]) => {
      if (count >= 5) {
        warnings.push({
          type: 'recurring_error_pattern',
          severity: 'WARNING',
          message: `检测到重复错误模式: ${errorType} (${count} 次)`,
          error_type: errorType,
          occurrence_count: count,
          impact: '重复错误可能指示配置或数据问题',
          urgency: 'WITHIN_DAYS',
        });
      }
    });
  }

  /**
   * 生成Import专业建议
   */
  generateImportRecommendations(diagnosis, data) {
    const recommendations = [];

    [...diagnosis.criticals, ...diagnosis.warnings].forEach((issue) => {
      switch (issue.type) {
        case 'high_load_failure_rate':
        case 'moderate_load_failure_rate':
          recommendations.push({
            category: 'failure_rate_optimization',
            priority: 'HIGH',
            title: '导入失败率优化',
            description: '降低导入失败率，提高数据导入成功率',
            professional_actions: [
              {
                action: '分析失败原因',
                command:
                  'SELECT ERROR_MSG, COUNT(*) FROM information_schema.loads WHERE STATE = "CANCELLED" AND CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR) GROUP BY ERROR_MSG ORDER BY COUNT(*) DESC;',
                purpose: '识别最常见的失败原因',
              },
              {
                action: '检查数据格式',
                steps: [
                  '验证数据文件格式是否符合表结构',
                  '检查字段分隔符和编码格式',
                  '确认数据类型匹配',
                ],
              },
              {
                action: '优化导入参数',
                recommendations: [
                  '调整max_filter_ratio参数',
                  '增加timeout时间',
                  '优化批次大小',
                ],
              },
            ],
          });
          break;

        case 'routine_load_paused':
        case 'routine_load_cancelled':
          recommendations.push({
            category: 'routine_load_recovery',
            priority: 'HIGH',
            title: 'Routine Load恢复',
            description: `恢复 ${issue.routine_name} 流式导入作业`,
            professional_actions: [
              {
                action: '检查作业状态',
                command: `SHOW ROUTINE LOAD FOR ${issue.routine_name};`,
                purpose: '获取详细的作业状态信息',
              },
              {
                action: '恢复作业',
                command: `RESUME ROUTINE LOAD FOR ${issue.routine_name};`,
                risk_level: 'LOW',
                note: '确保Kafka连接正常',
              },
              {
                action: '监控恢复效果',
                monitoring_metrics: ['数据消费速度', '延迟时间', '错误率'],
              },
            ],
          });
          break;

        case 'load_queue_backlog':
          recommendations.push({
            category: 'queue_management',
            priority: 'HIGH',
            title: '导入队列优化',
            description: '解决导入队列积压问题',
            professional_actions: [
              {
                action: '检查系统资源',
                steps: ['监控CPU使用率', '检查内存使用情况', '评估磁盘IO负载'],
              },
              {
                action: '调整并发配置',
                recommendations: [
                  '增加BE节点导入并发度',
                  '优化FE资源分配',
                  '调整导入队列大小',
                ],
              },
            ],
          });
          break;
      }
    });

    return recommendations;
  }

  /**
   * 生成优化建议
   */
  generateOptimizationSuggestions(data) {
    const suggestions = {
      performance_optimization: [],
      reliability_optimization: [],
      monitoring_enhancement: [],
    };

    // 性能优化建议
    suggestions.performance_optimization.push({
      area: 'batch_size_optimization',
      suggestion: '优化导入批次大小以提高吞吐量',
      implementation: '根据数据量和系统资源调整Stream Load批次大小',
    });

    suggestions.performance_optimization.push({
      area: 'parallel_loading',
      suggestion: '利用并行导入提高数据导入速度',
      implementation: '合理配置导入并发度，避免资源竞争',
    });

    // 可靠性优化建议
    suggestions.reliability_optimization.push({
      area: 'error_handling',
      suggestion: '建立完善的错误处理和重试机制',
      implementation: '设置合理的错误容忍度和自动重试策略',
    });

    suggestions.reliability_optimization.push({
      area: 'data_validation',
      suggestion: '加强数据质量验证',
      implementation: '在导入前进行数据格式和完整性检查',
    });

    // 监控增强建议
    suggestions.monitoring_enhancement.push({
      area: 'import_monitoring',
      suggestion: '建立全面的导入监控体系',
      key_metrics: [
        '导入成功率和失败率',
        '导入性能和吞吐量',
        '队列积压情况',
        'Routine Load延迟时间',
      ],
    });

    return suggestions;
  }

  /**
   * 计算Import健康分数
   */
  calculateImportHealthScore(diagnosis) {
    let score = 100;

    // 基于不同问题类型的扣分策略
    diagnosis.criticals.forEach((issue) => {
      switch (issue.type) {
        case 'high_load_failure_rate':
          score -= 25;
          break;
        case 'routine_load_cancelled':
          score -= 20;
          break;
        case 'load_queue_backlog':
          score -= 20;
          break;
        default:
          score -= 15;
      }
    });

    diagnosis.warnings.forEach((issue) => {
      switch (issue.type) {
        case 'moderate_load_failure_rate':
          score -= 10;
          break;
        case 'routine_load_paused':
          score -= 8;
          break;
        case 'slow_load_performance':
          score -= 8;
          break;
        default:
          score -= 5;
      }
    });

    score = Math.max(0, score);

    let level = 'EXCELLENT';
    if (score < 40) level = 'POOR';
    else if (score < 60) level = 'FAIR';
    else if (score < 80) level = 'GOOD';

    return {
      score: score,
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
   * 生成Import诊断摘要
   */
  generateImportSummary(criticals, warnings, issues) {
    if (criticals.length > 0) {
      const failureIssues = criticals.filter((c) =>
        c.type.includes('failure'),
      ).length;
      const queueIssues = criticals.filter((c) =>
        c.type.includes('queue'),
      ).length;

      if (failureIssues > 0) {
        return `Import系统存在 ${failureIssues} 个严重失败问题，影响数据导入成功率`;
      }
      if (queueIssues > 0) {
        return `Import系统存在 ${queueIssues} 个队列积压问题，可能导致导入延迟`;
      }
      return `Import系统发现 ${criticals.length} 个严重问题，需要立即处理`;
    } else if (warnings.length > 0) {
      return `Import系统发现 ${warnings.length} 个警告问题，建议近期优化`;
    } else {
      return 'Import系统运行状态良好，数据导入正常';
    }
  }

  /**
   * 分析指定表的详细导入频率
   * @param {Object} connection - 数据库连接
   * @param {string} dbName - 数据库名
   * @param {string} tableName - 表名
   * @param {boolean} includeDetails - 是否包含详细信息
   * @returns {Object} 详细的频率分析结果
   */
  async analyzeTableImportFrequency(
    connection,
    dbName,
    tableName,
    includeDetails = true,
  ) {
    console.error(`🔍 开始分析表 ${dbName}.${tableName} 的导入频率...`);
    const startTime = Date.now();

    try {
      // 1. 基础统计查询
      const [basicStats] = await connection.query(
        `
        SELECT
          COUNT(*) as total_loads,
          MIN(CREATE_TIME) as first_load,
          MAX(CREATE_TIME) as last_load,
          TIMESTAMPDIFF(SECOND, MIN(CREATE_TIME), MAX(CREATE_TIME)) as time_span_seconds,
          AVG(SCAN_BYTES) as avg_bytes_per_load,
          SUM(SCAN_BYTES) as total_bytes_processed,
          SUM(CASE WHEN STATE = 'FINISHED' THEN 1 ELSE 0 END) as success_count,
          SUM(CASE WHEN STATE != 'FINISHED' THEN 1 ELSE 0 END) as failed_count,
          GROUP_CONCAT(DISTINCT TYPE ORDER BY TYPE SEPARATOR ', ') as import_types,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME)) as avg_write_duration_seconds,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME)) as avg_publish_duration_seconds,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)) as avg_total_duration_seconds,
          AVG(
            CASE
              WHEN LOAD_START_TIME IS NOT NULL
                AND LOAD_FINISH_TIME IS NOT NULL
                AND TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME) > 0
              THEN SCAN_BYTES / (1024.0 * 1024.0) / TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)
              ELSE NULL
            END
          ) as avg_throughput_mbps,
          MIN(
            CASE
              WHEN LOAD_START_TIME IS NOT NULL
                AND LOAD_FINISH_TIME IS NOT NULL
                AND TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME) > 0
              THEN SCAN_BYTES / (1024.0 * 1024.0) / TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)
              ELSE NULL
            END
          ) as min_throughput_mbps,
          MAX(
            CASE
              WHEN LOAD_START_TIME IS NOT NULL
                AND LOAD_FINISH_TIME IS NOT NULL
                AND TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME) > 0
              THEN SCAN_BYTES / (1024.0 * 1024.0) / TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)
              ELSE NULL
            END
          ) as max_throughput_mbps
        FROM _statistics_.loads_history
        WHERE DB_NAME = ? AND TABLE_NAME = ?
      `,
        [dbName, tableName],
      );

      if (
        !basicStats ||
        basicStats.length === 0 ||
        basicStats[0].total_loads === 0
      ) {
        return {
          table: `${dbName}.${tableName}`,
          analysis_type: 'table_frequency_analysis',
          status: 'no_data',
          message: '未找到该表的导入记录',
          analysis_duration_ms: Date.now() - startTime,
        };
      }

      const stats = basicStats[0];
      const timeSpanSeconds = Math.max(stats.time_span_seconds || 1, 1);

      // 2. 计算频率指标
      const frequencyMetrics = this.calculateFrequencyMetrics(
        stats,
        timeSpanSeconds,
      );

      // 3. 获取时间分布数据
      const timeDistribution = await this.getTimeDistribution(
        connection,
        dbName,
        tableName,
      );

      // 4. 获取导入阶段耗时统计
      const phaseStats = await this.getLoadPhaseStatistics(
        connection,
        dbName,
        tableName,
      );

      // 5. 获取数据量统计
      const sizeStats = await this.getSizeStatistics(
        connection,
        dbName,
        tableName,
      );

      // 6. 分析并发模式
      const concurrencyAnalysis = this.analyzeConcurrencyPattern(
        timeDistribution,
        stats.total_loads,
      );

      // 7. 性能评估
      const performanceAnalysis = this.evaluateImportPerformance(
        stats,
        frequencyMetrics,
        timeSpanSeconds,
      );

      // 8. 生成洞察和建议
      const insights = this.generateTableFrequencyInsights(
        stats,
        frequencyMetrics,
        performanceAnalysis,
        phaseStats,
      );

      const result = {
        table: `${dbName}.${tableName}`,
        analysis_type: 'table_frequency_analysis',
        status: 'completed',
        analysis_duration_ms: Date.now() - startTime,

        // 基础统计
        basic_statistics: {
          total_loads: stats.total_loads,
          success_count: stats.success_count,
          failed_count: stats.failed_count,
          success_rate: (
            (stats.success_count / stats.total_loads) *
            100
          ).toFixed(1),
          first_load: stats.first_load,
          last_load: stats.last_load,
          time_span_seconds: timeSpanSeconds,
          total_data_processed: stats.total_bytes_processed,
          avg_file_size: stats.avg_bytes_per_load,
          import_types: stats.import_types || 'N/A',
        },

        // 频率指标
        frequency_metrics: frequencyMetrics,

        // 时间分布
        time_distribution: includeDetails
          ? timeDistribution
          : timeDistribution.slice(0, 10),

        // 数据量统计
        size_statistics: sizeStats,

        // 导入阶段耗时统计
        phase_statistics: phaseStats,

        // 并发分析
        concurrency_analysis: concurrencyAnalysis,

        // 性能评估
        performance_analysis: performanceAnalysis,

        // 洞察和建议
        insights: insights,
      };

      console.error(
        `✅ 表 ${dbName}.${tableName} 频率分析完成，耗时 ${result.analysis_duration_ms}ms`,
      );
      return result;
    } catch (error) {
      console.error(`❌ 表频率分析失败: ${error.message}`);
      return {
        table: `${dbName}.${tableName}`,
        analysis_type: 'table_frequency_analysis',
        status: 'error',
        error: error.message,
        analysis_duration_ms: Date.now() - startTime,
      };
    }
  }

  /**
   * 计算频率指标
   */
  calculateFrequencyMetrics(stats, timeSpanSeconds) {
    const loadsPerSecond = stats.total_loads / timeSpanSeconds;
    const loadsPerMinute = loadsPerSecond * 60;
    const loadsPerHour = loadsPerSecond * 3600;
    const avgInterval = timeSpanSeconds / Math.max(stats.total_loads - 1, 1);

    // 频率等级分类
    let frequencyLevel = 'low';
    let frequencyCategory = 'low_frequency';

    if (loadsPerSecond > 1) {
      frequencyLevel = 'extreme';
      frequencyCategory = 'extreme_frequency';
    } else if (loadsPerMinute > 60) {
      frequencyLevel = 'very_high';
      frequencyCategory = 'very_high_frequency';
    } else if (loadsPerMinute > 4) {
      frequencyLevel = 'high';
      frequencyCategory = 'high_frequency';
    } else if (loadsPerMinute > 1) {
      frequencyLevel = 'frequent';
      frequencyCategory = 'frequent';
    } else if (loadsPerHour > 1) {
      frequencyLevel = 'moderate';
      frequencyCategory = 'moderate';
    }

    return {
      loads_per_second: parseFloat(loadsPerSecond.toFixed(2)),
      loads_per_minute: parseFloat(loadsPerMinute.toFixed(1)),
      loads_per_hour: parseFloat(loadsPerHour.toFixed(0)),
      avg_interval_seconds: parseFloat(avgInterval.toFixed(2)),
      frequency_level: frequencyLevel,
      frequency_category: frequencyCategory,
      frequency_description: this.getFrequencyDescription(frequencyCategory),
    };
  }

  /**
   * 获取时间分布数据
   */
  async getTimeDistribution(connection, dbName, tableName) {
    try {
      const [timeDistribution] = await connection.query(
        `
        SELECT
          LOAD_FINISH_TIME,
          COUNT(*) as job_count,
          ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM information_schema.loads
            WHERE DB_NAME = ? AND TABLE_NAME = ?
          ), 1) as percentage
        FROM information_schema.loads
        WHERE DB_NAME = ? AND TABLE_NAME = ? AND LOAD_FINISH_TIME IS NOT NULL
        GROUP BY LOAD_FINISH_TIME
        ORDER BY LOAD_FINISH_TIME
      `,
        [dbName, tableName, dbName, tableName],
      );

      return timeDistribution.map((item) => ({
        finish_time: item.LOAD_FINISH_TIME,
        job_count: item.job_count,
        percentage: parseFloat(item.percentage),
      }));
    } catch (error) {
      console.warn(`获取时间分布失败: ${error.message}`);
      return [];
    }
  }

  /**
   * 获取导入阶段耗时统计
   */
  async getLoadPhaseStatistics(connection, dbName, tableName) {
    try {
      console.error(`🔍 正在获取表 ${dbName}.${tableName} 的阶段耗时统计...`);

      // 首先检查有多少条记录有完整的时间字段
      const [checkResult] = await connection.query(
        `
        SELECT
          COUNT(*) as total_records,
          SUM(CASE WHEN LOAD_START_TIME IS NOT NULL AND LOAD_COMMIT_TIME IS NOT NULL
              AND LOAD_FINISH_TIME IS NOT NULL AND STATE = 'FINISHED' THEN 1 ELSE 0 END) as complete_records
        FROM _statistics_.loads_history
        WHERE DB_NAME = ? AND TABLE_NAME = ?
      `,
        [dbName, tableName],
      );

      console.error(
        `📊 时间字段检查: 总记录=${checkResult[0].total_records}, 完整记录=${checkResult[0].complete_records}`,
      );

      if (checkResult[0].complete_records === 0) {
        console.error(`⚠️  没有找到具有完整时间字段的记录，返回 null`);
        return null;
      }

      const [phaseStats] = await connection.query(
        `
        SELECT
          COUNT(*) as analyzed_loads,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME)) as avg_write_duration,
          MIN(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME)) as min_write_duration,
          MAX(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME)) as max_write_duration,
          STDDEV(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME)) as write_duration_stddev,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME)) as avg_publish_duration,
          MIN(TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME)) as min_publish_duration,
          MAX(TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME)) as max_publish_duration,
          STDDEV(TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME)) as publish_duration_stddev,
          AVG(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)) as avg_total_duration,
          MIN(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)) as min_total_duration,
          MAX(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)) as max_total_duration,
          STDDEV(TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_FINISH_TIME)) as total_duration_stddev
        FROM _statistics_.loads_history
        WHERE DB_NAME = ?
          AND TABLE_NAME = ?
          AND LOAD_START_TIME IS NOT NULL
          AND LOAD_COMMIT_TIME IS NOT NULL
          AND LOAD_FINISH_TIME IS NOT NULL
          AND STATE = 'FINISHED'
      `,
        [dbName, tableName],
      );

      if (
        !phaseStats ||
        phaseStats.length === 0 ||
        phaseStats[0].analyzed_loads === 0
      ) {
        console.error(`⚠️  阶段统计查询无结果，返回 null`);
        return null;
      }

      console.error(
        `✅ 成功获取阶段统计，分析了 ${phaseStats[0].analyzed_loads} 条记录`,
      );

      const stats = phaseStats[0];

      // 计算写入阶段占比
      const writePercentage =
        stats.avg_total_duration > 0
          ? (stats.avg_write_duration / stats.avg_total_duration) * 100
          : 0;

      // 计算 publish 阶段占比
      const publishPercentage =
        stats.avg_total_duration > 0
          ? (stats.avg_publish_duration / stats.avg_total_duration) * 100
          : 0;

      // 计算慢任务数量（单独查询，避免聚合函数嵌套）
      let slowWriteCount = 0;
      let slowPublishCount = 0;
      try {
        const writeThreshold = stats.avg_write_duration * 3;
        const publishThreshold = stats.avg_publish_duration * 3;

        const [slowTasks] = await connection.query(
          `
          SELECT
            SUM(CASE WHEN TIMESTAMPDIFF(SECOND, LOAD_START_TIME, LOAD_COMMIT_TIME) > ? THEN 1 ELSE 0 END) as slow_write,
            SUM(CASE WHEN TIMESTAMPDIFF(SECOND, LOAD_COMMIT_TIME, LOAD_FINISH_TIME) > ? THEN 1 ELSE 0 END) as slow_publish
          FROM _statistics_.loads_history
          WHERE DB_NAME = ?
            AND TABLE_NAME = ?
            AND LOAD_START_TIME IS NOT NULL
            AND LOAD_COMMIT_TIME IS NOT NULL
            AND LOAD_FINISH_TIME IS NOT NULL
            AND STATE = 'FINISHED'
        `,
          [writeThreshold, publishThreshold, dbName, tableName],
        );

        if (slowTasks && slowTasks.length > 0) {
          slowWriteCount = slowTasks[0].slow_write || 0;
          slowPublishCount = slowTasks[0].slow_publish || 0;
        }
      } catch (err) {
        console.warn(`计算慢任务数量失败: ${err.message}`);
      }

      return {
        analyzed_loads: stats.analyzed_loads,

        // 写入阶段统计
        write_phase: {
          avg_duration: parseFloat((stats.avg_write_duration || 0).toFixed(2)),
          min_duration: stats.min_write_duration || 0,
          max_duration: stats.max_write_duration || 0,
          stddev: parseFloat((stats.write_duration_stddev || 0).toFixed(2)),
          percentage_of_total: parseFloat(writePercentage.toFixed(1)),
          slow_count: slowWriteCount,
        },

        // Publish 阶段统计
        publish_phase: {
          avg_duration: parseFloat(
            (stats.avg_publish_duration || 0).toFixed(2),
          ),
          min_duration: stats.min_publish_duration || 0,
          max_duration: stats.max_publish_duration || 0,
          stddev: parseFloat((stats.publish_duration_stddev || 0).toFixed(2)),
          percentage_of_total: parseFloat(publishPercentage.toFixed(1)),
          slow_count: slowPublishCount,
        },

        // 总耗时统计
        total_phase: {
          avg_duration: parseFloat((stats.avg_total_duration || 0).toFixed(2)),
          min_duration: stats.min_total_duration || 0,
          max_duration: stats.max_total_duration || 0,
          stddev: parseFloat((stats.total_duration_stddev || 0).toFixed(2)),
        },

        // 性能洞察
        insights: this.generatePhaseInsights(
          writePercentage,
          publishPercentage,
          slowWriteCount,
          slowPublishCount,
        ),
      };
    } catch (error) {
      console.error(`❌ 获取阶段耗时统计失败: ${error.message}`);
      console.error(error.stack);
      return null;
    }
  }

  /**
   * 生成阶段耗时洞察
   */
  generatePhaseInsights(
    writePercentage,
    publishPercentage,
    slowWriteCount,
    slowPublishCount,
  ) {
    const insights = [];

    // 写入阶段分析
    if (writePercentage > 70) {
      insights.push({
        phase: 'write',
        type: 'bottleneck',
        message: `写入阶段耗时占比过高 (${writePercentage.toFixed(1)}%)`,
        suggestion: '考虑优化数据写入性能，检查磁盘I/O和内存配置',
      });
    }

    // Publish 阶段分析
    if (publishPercentage > 50) {
      insights.push({
        phase: 'publish',
        type: 'bottleneck',
        message: `Publish 阶段耗时占比较高 (${publishPercentage.toFixed(1)}%)`,
        suggestion: '可能存在版本发布或元数据更新瓶颈，检查事务提交性能',
      });
    }

    // 慢任务分析
    if (slowWriteCount > 0) {
      insights.push({
        phase: 'write',
        type: 'slow_tasks',
        message: `发现 ${slowWriteCount} 个慢写入任务`,
        suggestion: '分析慢任务的数据特征和系统状态',
      });
    }

    if (slowPublishCount > 0) {
      insights.push({
        phase: 'publish',
        type: 'slow_tasks',
        message: `发现 ${slowPublishCount} 个慢 publish 任务`,
        suggestion: '检查元数据服务和版本管理性能',
      });
    }

    // 性能均衡性分析
    if (
      writePercentage > 30 &&
      writePercentage < 70 &&
      publishPercentage > 20 &&
      publishPercentage < 50
    ) {
      insights.push({
        phase: 'overall',
        type: 'balanced',
        message: '导入各阶段耗时分布较为均衡',
        suggestion: '当前性能配置合理，继续保持',
      });
    }

    return insights;
  }

  /**
   * 获取数据量统计
   */
  async getSizeStatistics(connection, dbName, tableName) {
    try {
      const [sizeStats] = await connection.query(
        `
        SELECT
          MIN(SCAN_BYTES) as min_size,
          MAX(SCAN_BYTES) as max_size,
          AVG(SCAN_BYTES) as avg_size,
          STDDEV(SCAN_BYTES) as size_stddev
        FROM information_schema.loads
        WHERE DB_NAME = ? AND TABLE_NAME = ? AND SCAN_BYTES IS NOT NULL
      `,
        [dbName, tableName],
      );

      if (!sizeStats || sizeStats.length === 0) {
        return null;
      }

      const stats = sizeStats[0];
      const variationCoefficient =
        stats.size_stddev && stats.avg_size
          ? (stats.size_stddev / stats.avg_size) * 100
          : 0;

      return {
        min_size: stats.min_size,
        max_size: stats.max_size,
        avg_size: stats.avg_size,
        size_stddev: stats.size_stddev,
        variation_coefficient: parseFloat(variationCoefficient.toFixed(1)),
        consistency_level: this.getSizeConsistencyLevel(variationCoefficient),
      };
    } catch (error) {
      console.warn(`获取大小统计失败: ${error.message}`);
      return null;
    }
  }

  /**
   * 分析并发模式
   */
  analyzeConcurrencyPattern(timeDistribution, totalLoads) {
    if (!timeDistribution || timeDistribution.length === 0) {
      return null;
    }

    const peakTime = timeDistribution.reduce((max, current) =>
      current.job_count > max.job_count ? current : max,
    );

    const avgJobsPerSecond = totalLoads / timeDistribution.length;

    // 计算标准差
    const variance =
      timeDistribution.reduce((sum, time) => {
        return sum + Math.pow(time.job_count - avgJobsPerSecond, 2);
      }, 0) / timeDistribution.length;
    const stdDev = Math.sqrt(variance);

    return {
      peak_time: peakTime.finish_time,
      peak_concurrent_jobs: peakTime.job_count,
      time_span_seconds: timeDistribution.length,
      avg_jobs_per_second: parseFloat(avgJobsPerSecond.toFixed(1)),
      completion_std_dev: parseFloat(stdDev.toFixed(2)),
      concurrency_level: this.getConcurrencyLevel(
        peakTime.job_count,
        avgJobsPerSecond,
      ),
    };
  }

  /**
   * 识别导入模式
   */
  identifyImportPattern(stats, frequencyMetrics, sizeStats, timeDistribution) {
    const patterns = [];

    // 检查批量导入模式
    if (stats.total_loads > 100 && frequencyMetrics.loads_per_second > 10) {
      patterns.push({
        type: 'bulk_parallel_import',
        confidence: 0.9,
        description: '大批量并行导入模式',
        characteristics: [
          `${stats.total_loads}个文件快速并行导入`,
          `每秒处理${frequencyMetrics.loads_per_second}个文件`,
          '适用于大规模数据迁移或基准测试',
        ],
      });
    }

    // 检查流式导入模式
    if (frequencyMetrics.frequency_level === 'high' && stats.total_loads > 10) {
      patterns.push({
        type: 'streaming_import',
        confidence: 0.8,
        description: '流式导入模式',
        characteristics: [
          `高频导入 (${frequencyMetrics.loads_per_minute}次/分钟)`,
          '适用于实时数据处理',
          '需要关注系统资源消耗',
        ],
      });
    }

    // 检查数据分片模式
    if (sizeStats && sizeStats.variation_coefficient < 10) {
      patterns.push({
        type: 'uniform_sharding',
        confidence: 0.85,
        description: '均匀分片导入模式',
        characteristics: [
          `文件大小变异系数${sizeStats.variation_coefficient}%`,
          '数据已被均匀分片',
          '负载均衡良好',
        ],
      });
    }

    // 检查基准测试模式
    if (
      stats.total_loads === 195 &&
      stats.success_count === stats.total_loads
    ) {
      patterns.push({
        type: 'benchmark_testing',
        confidence: 0.95,
        description: 'SSB基准测试模式',
        characteristics: [
          '195个文件 (SSB 100GB标准分片)',
          '100%成功率',
          '性能测试或基准评估场景',
        ],
      });
    }

    return {
      identified_patterns: patterns,
      primary_pattern: patterns.length > 0 ? patterns[0] : null,
    };
  }

  /**
   * 评估导入性能
   */
  evaluateImportPerformance(stats, frequencyMetrics, timeSpanSeconds) {
    const totalDataGB = stats.total_bytes_processed / (1024 * 1024 * 1024);

    // 使用每个任务吞吐量的平均值（更准确）
    // 确保转换为数字类型，避免 null、undefined 或 NaN
    const avgThroughputMBps = Number(stats.avg_throughput_mbps) || 0;
    const minThroughputMBps = Number(stats.min_throughput_mbps) || 0;
    const maxThroughputMBps = Number(stats.max_throughput_mbps) || 0;

    // 安全地格式化数字
    const safeFormat = (num, decimals) => {
      const n = Number(num);
      return isNaN(n) ? 0 : parseFloat(n.toFixed(decimals));
    };

    return {
      total_data_gb: safeFormat(totalDataGB, 2),
      throughput_mbps: safeFormat(avgThroughputMBps, 1),
      min_throughput_mbps: safeFormat(minThroughputMBps, 1),
      max_throughput_mbps: safeFormat(maxThroughputMBps, 1),
      success_rate: safeFormat(
        (stats.success_count / stats.total_loads) * 100,
        1,
      ),
    };
  }

  /**
   * 生成表频率分析洞察
   */
  generateTableFrequencyInsights(
    stats,
    frequencyMetrics,
    performanceAnalysis,
    phaseStats,
  ) {
    const insights = [];

    // 添加阶段耗时洞察
    if (phaseStats && phaseStats.insights && phaseStats.insights.length > 0) {
      phaseStats.insights.forEach((insight) => {
        insights.push({
          type: `phase_${insight.type}`,
          priority: insight.type === 'bottleneck' ? 'high' : 'info',
          message: insight.message,
          implications: [insight.suggestion],
          recommendations: [insight.suggestion],
        });
      });
    }

    // 频率相关洞察
    if (frequencyMetrics.frequency_level === 'extreme') {
      insights.push({
        type: 'extreme_frequency',
        priority: 'high',
        message: `检测到极高频导入 (${frequencyMetrics.loads_per_second}次/秒)`,
        implications: [
          '可能是批量数据迁移或性能测试',
          '需要确保系统资源充足',
          '关注I/O和网络性能',
        ],
        recommendations: [
          '监控系统资源使用情况',
          '评估是否需要调整并发度',
          '考虑优化导入批次大小',
        ],
      });
    }

    // 成功率相关洞察
    if (performanceAnalysis.success_rate === 100) {
      insights.push({
        type: 'reliability_perfect',
        priority: 'info',
        message: '导入可靠性完美 (100%成功率)',
        implications: [
          '导入流程非常稳定',
          '数据质量和格式良好',
          '系统配置合理',
        ],
        recommendations: [
          '继续保持当前的数据处理流程',
          '可以作为最佳实践案例',
          '建立成功率监控告警',
        ],
      });
    } else if (performanceAnalysis.success_rate < 95) {
      insights.push({
        type: 'reliability_concern',
        priority: 'medium',
        message: `导入成功率较低 (${performanceAnalysis.success_rate}%)`,
        implications: [
          '存在数据质量或系统问题',
          '可能影响数据完整性',
          '需要分析失败原因',
        ],
        recommendations: [
          '检查失败导入的错误日志',
          '改进数据验证和清洗流程',
          '优化错误处理和重试机制',
        ],
      });
    }

    return insights;
  }

  /**
   * 获取频率描述
   */
  getFrequencyDescription(category) {
    const descriptions = {
      extreme_frequency: '极高频 (每秒多次)',
      very_high_frequency: '超高频 (每分钟60+次)',
      high_frequency: '高频 (每分钟4+次)',
      frequent: '频繁 (每分钟1-4次)',
      moderate: '中等 (每小时1+次)',
      low_frequency: '低频 (每小时<1次)',
    };
    return descriptions[category] || '未知频率';
  }

  /**
   * 获取大小一致性等级
   */
  getSizeConsistencyLevel(variationCoefficient) {
    if (variationCoefficient < 5) return 'excellent';
    if (variationCoefficient < 15) return 'good';
    if (variationCoefficient < 30) return 'fair';
    return 'poor';
  }

  /**
   * 获取并发等级
   */
  getConcurrencyLevel(peakJobs, avgJobs) {
    if (peakJobs > avgJobs * 3) return 'high_burst';
    if (peakJobs > avgJobs * 2) return 'moderate_burst';
    return 'steady';
  }

  /**
   * 获取模式推荐
   */
  getPatternRecommendations(patternType) {
    const recommendations = {
      bulk_parallel_import: [
        '监控系统资源使用峰值',
        '考虑在低峰时段执行大批量导入',
        '优化并行度避免资源争抢',
      ],
      streaming_import: [
        '建立实时监控和告警',
        '优化批次大小平衡延迟和吞吐量',
        '考虑使用Routine Load提高稳定性',
      ],
      uniform_sharding: [
        '继续保持均匀分片策略',
        '可以考虑适当增加并行度',
        '监控单个分片的处理时间',
      ],
      benchmark_testing: [
        '记录性能基准指标',
        '可以作为系统性能评估标准',
        '定期重复测试验证系统稳定性',
      ],
    };
    return recommendations[patternType] || ['需要进一步分析具体场景'];
  }

  /**
   * 格式化表频率分析报告
   */
  formatTableFrequencyReport(analysis) {
    if (analysis.status !== 'completed') {
      return `❌ 表 ${analysis.table} 频率分析失败: ${analysis.error || analysis.message}`;
    }

    const stats = analysis.basic_statistics;
    const freq = analysis.frequency_metrics;
    const perf = analysis.performance_analysis;

    let report = `📊 ${analysis.table} 导入频率详细分析\n`;
    report += '=========================================\n\n';

    // 基础统计
    report += '📈 基础统计信息:\n';
    report += `   总导入作业: ${stats.total_loads.toLocaleString()}\n`;
    report += `   成功率: ${stats.success_rate}%\n`;
    report += `   导入类型: ${stats.import_types}\n`;
    report += `   时间跨度: ${stats.time_span_seconds}秒\n`;
    report += `   数据处理量: ${this.formatBytes(stats.total_data_processed)}\n\n`;

    // 频率指标
    report += '⚡ 频率指标:\n';
    report += `   每秒导入: ${freq.loads_per_second} 次\n`;
    report += `   每分钟导入: ${freq.loads_per_minute} 次\n`;
    report += `   频率等级: ${freq.frequency_description}\n`;
    report += `   平均间隔: ${freq.avg_interval_seconds} 秒\n\n`;

    // 性能评估
    report += '📊 性能评估:\n';
    report += `   平均吞吐量: ${perf.throughput_mbps} MB/s\n`;
    report += `   吞吐量范围: ${perf.min_throughput_mbps} - ${perf.max_throughput_mbps} MB/s\n\n`;

    // 导入阶段耗时统计
    if (analysis.phase_statistics) {
      const phase = analysis.phase_statistics;
      report += '⏱️  导入阶段耗时分析:\n';
      report += `   分析样本: ${phase.analyzed_loads} 个成功导入\n\n`;

      report += `   📝 写入阶段:\n`;
      report += `      平均耗时: ${phase.write_phase.avg_duration} 秒\n`;
      report += `      耗时范围: ${phase.write_phase.min_duration} - ${phase.write_phase.max_duration} 秒\n`;
      report += `      占总耗时: ${phase.write_phase.percentage_of_total}%\n`;
      if (phase.write_phase.slow_count > 0) {
        report += `      ⚠️  慢任务: ${phase.write_phase.slow_count} 个\n`;
      }
      report += '\n';

      report += `   📤 Publish阶段:\n`;
      report += `      平均耗时: ${phase.publish_phase.avg_duration} 秒\n`;
      report += `      耗时范围: ${phase.publish_phase.min_duration} - ${phase.publish_phase.max_duration} 秒\n`;
      report += `      占总耗时: ${phase.publish_phase.percentage_of_total}%\n`;
      if (phase.publish_phase.slow_count > 0) {
        report += `      ⚠️  慢任务: ${phase.publish_phase.slow_count} 个\n`;
      }
      report += '\n';

      report += `   🔄 总耗时:\n`;
      report += `      平均耗时: ${phase.total_phase.avg_duration} 秒\n`;
      report += `      耗时范围: ${phase.total_phase.min_duration} - ${phase.total_phase.max_duration} 秒\n`;
      report += '\n';
    }

    // 关键洞察
    if (analysis.insights.length > 0) {
      report += '💡 关键洞察:\n';
      analysis.insights.slice(0, 3).forEach((insight, index) => {
        const priority =
          insight.priority === 'high'
            ? '🔥'
            : insight.priority === 'medium'
              ? '⚠️'
              : 'ℹ️';
        report += `   ${index + 1}. ${priority} ${insight.message}\n`;
        if (insight.recommendations.length > 0) {
          report += `      建议: ${insight.recommendations[0]}\n`;
        }
      });
    }

    return report;
  }

  /**
   * 格式化字节数
   */
  formatBytes(bytes) {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * 标准化 SHOW ROUTINE LOAD 返回的字段名
   * 将 SHOW 命令返回的字段名（如 Name, DbName）转换为统一格式（NAME, DB_NAME）
   */
  normalizeRoutineLoadFields(job) {
    return {
      NAME: job.Name || job.NAME,
      CREATE_TIME: job.CreateTime || job.CREATE_TIME,
      PAUSE_TIME: job.PauseTime || job.PAUSE_TIME,
      END_TIME: job.EndTime || job.END_TIME,
      DB_NAME: job.DbName || job.DB_NAME,
      TABLE_NAME: job.TableName || job.TABLE_NAME,
      STATE: job.State || job.STATE,
      DATA_SOURCE_NAME: job.DataSourceType || job.DATA_SOURCE_NAME,
      CURRENT_TASK_NUM: job.CurrentTaskNum || job.CURRENT_TASK_NUM || 0,
      JOB_PROPERTIES: job.JobProperties || job.JOB_PROPERTIES,
      DATA_SOURCE_PROPERTIES:
        job.DataSourceProperties || job.DATA_SOURCE_PROPERTIES,
      CUSTOM_PROPERTIES: job.CustomProperties || job.CUSTOM_PROPERTIES,
      STATISTIC: job.Statistic || job.STATISTIC,
      PROGRESS: job.Progress || job.PROGRESS,
      TRACKING_SQL: job.TrackingSQL || job.TRACKING_SQL,
      OTHER_MSG: job.OtherMsg || job.OTHER_MSG,
      REASON_OF_STATE_CHANGED:
        job.ReasonOfStateChanged || job.REASON_OF_STATE_CHANGED,
    };
  }

  /**
   * 检查 Routine Load Job 配置参数
   * @param {Object} connection - 数据库连接
   * @param {string} jobName - Routine Load 作业名称（可选）
   * @param {string} dbName - 数据库名称（可选）
   * @returns {Object} 参数检测结果
   */
  async checkRoutineLoadJobConfig(connection, jobName = null, dbName = null) {
    console.error(`🔍 开始检查 Routine Load 作业配置...`);
    const startTime = Date.now();

    try {
      // 1. 获取 Routine Load 作业列表（使用 SHOW ROUTINE LOAD 命令）
      let routineLoadJobs = [];

      if (dbName) {
        // 指定了数据库，直接查询该数据库
        const showCommand = jobName
          ? `SHOW ROUTINE LOAD FOR ${jobName} FROM \`${dbName}\``
          : `SHOW ROUTINE LOAD FROM \`${dbName}\``;

        try {
          const [jobs] = await connection.query(showCommand);
          if (jobs && jobs.length > 0) {
            // 标准化字段名
            routineLoadJobs = jobs.map((job) =>
              this.normalizeRoutineLoadFields(job),
            );
          }
        } catch (error) {
          console.error(
            `查询数据库 ${dbName} 的 Routine Load 失败: ${error.message}`,
          );
          throw error;
        }
      } else {
        // 未指定数据库，需要遍历所有数据库
        const [databases] = await connection.query('SHOW DATABASES');

        for (const db of databases) {
          const currentDb = db.Database;

          // 跳过系统数据库
          if (
            ['information_schema', '_statistics_', 'sys'].includes(currentDb)
          ) {
            continue;
          }

          try {
            const showCommand = jobName
              ? `SHOW ROUTINE LOAD FOR ${jobName} FROM \`${currentDb}\``
              : `SHOW ROUTINE LOAD FROM \`${currentDb}\``;

            const [jobs] = await connection.query(showCommand);
            if (jobs && jobs.length > 0) {
              // 标准化字段名
              const normalizedJobs = jobs.map((job) =>
                this.normalizeRoutineLoadFields(job),
              );
              routineLoadJobs.push(...normalizedJobs);
            }
          } catch (error) {
            // 某些数据库可能没有 Routine Load 权限或为空，忽略错误
            console.warn(
              `查询数据库 ${currentDb} 的 Routine Load 失败: ${error.message}`,
            );
          }
        }
      }

      if (!routineLoadJobs || routineLoadJobs.length === 0) {
        return {
          status: 'no_jobs',
          message: jobName
            ? `未找到名为 "${jobName}" 的 Routine Load 作业`
            : dbName
              ? `数据库 "${dbName}" 中未找到任何 Routine Load 作业`
              : '未找到任何 Routine Load 作业',
          analysis_duration_ms: Date.now() - startTime,
        };
      }

      // 2. 分析每个作业的配置
      const jobAnalysis = [];

      for (const job of routineLoadJobs) {
        const analysis = await this.analyzeRoutineLoadJobConfig(job);
        jobAnalysis.push(analysis);
      }

      // 3. 生成综合评估
      const overallAssessment =
        this.generateRoutineLoadOverallAssessment(jobAnalysis);

      console.error(
        `✅ Routine Load 配置检查完成，耗时 ${Date.now() - startTime}ms`,
      );

      return {
        status: 'completed',
        analysis_type: 'routine_load_config_check',
        analysis_duration_ms: Date.now() - startTime,
        total_jobs: routineLoadJobs.length,
        job_analysis: jobAnalysis,
        overall_assessment: overallAssessment,
      };
    } catch (error) {
      console.error(`❌ Routine Load 配置检查失败: ${error.message}`);
      return {
        status: 'error',
        error: error.message,
        analysis_duration_ms: Date.now() - startTime,
      };
    }
  }

  /**
   * 分析单个 Routine Load 作业配置
   */
  async analyzeRoutineLoadJobConfig(job) {
    const issues = [];
    const warnings = [];
    const recommendations = [];
    let configScore = 100;

    // 解析配置参数
    const jobProperties = this.parseJobProperties(job.JOB_PROPERTIES);
    const dataSourceProperties = this.parseJobProperties(
      job.DATA_SOURCE_PROPERTIES,
    );
    const customProperties = this.parseJobProperties(job.CUSTOM_PROPERTIES);
    const statistics = this.parseJobProperties(job.STATISTIC);

    // 1. 检查任务并发数
    const currentTaskNum = job.CURRENT_TASK_NUM || 0;
    const desiredConcurrentNum =
      parseInt(jobProperties.desired_concurrent_number) || 1;

    if (currentTaskNum === 0 && job.STATE === 'RUNNING') {
      issues.push({
        type: 'no_running_tasks',
        severity: 'CRITICAL',
        message: '作业状态为 RUNNING 但没有运行中的任务',
        impact: '数据无法被消费，可能导致数据延迟',
      });
      configScore -= 30;
    }

    if (desiredConcurrentNum > 5) {
      warnings.push({
        type: 'high_concurrent_number',
        severity: 'WARNING',
        message: `并发数设置过高: ${desiredConcurrentNum}`,
        current_value: desiredConcurrentNum,
        recommended_value: '1-5',
        impact: '过高的并发可能导致资源竞争和任务不稳定',
      });
      configScore -= 10;
    }

    // 2. 检查 max_batch_interval
    const maxBatchInterval =
      parseInt(jobProperties.max_batch_interval) ||
      this.rules.routine_load.max_batch_interval_seconds;

    if (maxBatchInterval > this.rules.routine_load.max_batch_interval_seconds) {
      warnings.push({
        type: 'high_batch_interval',
        severity: 'WARNING',
        message: `批次间隔过长: ${maxBatchInterval}秒`,
        current_value: maxBatchInterval,
        recommended_value: this.rules.routine_load.max_batch_interval_seconds,
        impact: '批次间隔过长可能导致数据延迟增加',
      });
      configScore -= 5;
    }

    // 3. 检查 max_batch_rows
    const maxBatchRows =
      parseInt(jobProperties.max_batch_rows) ||
      this.rules.routine_load.max_batch_rows;

    if (maxBatchRows > this.rules.routine_load.max_batch_rows) {
      warnings.push({
        type: 'high_batch_rows',
        severity: 'WARNING',
        message: `单批次行数过多: ${maxBatchRows.toLocaleString()}`,
        current_value: maxBatchRows,
        recommended_value: this.rules.routine_load.max_batch_rows,
        impact: '批次过大可能导致内存压力和任务超时',
      });
      configScore -= 5;
    }

    // 4. 检查 max_error_number
    const maxErrorNumber = parseInt(jobProperties.max_error_number) || 0;

    if (maxErrorNumber > 10000) {
      warnings.push({
        type: 'high_error_tolerance',
        severity: 'WARNING',
        message: `错误容忍度过高: ${maxErrorNumber.toLocaleString()}`,
        current_value: maxErrorNumber,
        recommended_value: '1000-5000',
        impact: '过高的错误容忍可能掩盖数据质量问题',
      });
      configScore -= 5;
    }

    // 5. 检查 Kafka 相关配置
    if (dataSourceProperties.kafka_topic) {
      // 检查 kafka_partitions
      const kafkaPartitions = dataSourceProperties.kafka_partitions;
      if (kafkaPartitions && desiredConcurrentNum > 1) {
        const partitionCount = kafkaPartitions.split(',').length;
        if (desiredConcurrentNum > partitionCount) {
          warnings.push({
            type: 'concurrent_exceeds_partitions',
            severity: 'WARNING',
            message: `并发数 (${desiredConcurrentNum}) 超过 Kafka 分区数 (${partitionCount})`,
            current_concurrent: desiredConcurrentNum,
            partition_count: partitionCount,
            impact: '部分任务将处于空闲状态，浪费资源',
          });
          configScore -= 10;
        }
      }

      // 检查 kafka_offsets
      if (!dataSourceProperties.kafka_offsets) {
        recommendations.push({
          type: 'kafka_offsets_not_set',
          priority: 'LOW',
          message: '未显式设置 kafka_offsets',
          suggestion: '建议显式设置起始 offset 以避免数据丢失或重复消费',
        });
      }
    }

    // 6. 检查 format 和数据格式配置
    const format = jobProperties.format?.toLowerCase() || 'csv';
    const stripOuterArray = customProperties.strip_outer_array === 'true';
    const jsonPaths = customProperties.jsonpaths;

    if (format === 'json' && !jsonPaths && !stripOuterArray) {
      recommendations.push({
        type: 'json_format_optimization',
        priority: 'MEDIUM',
        message: 'JSON 格式未配置 jsonpaths 或 strip_outer_array',
        suggestion:
          '建议配置 jsonpaths 或 strip_outer_array 以优化 JSON 解析性能',
      });
    }

    // 7. 检查作业状态和统计信息
    if (job.STATE === 'PAUSED') {
      issues.push({
        type: 'job_paused',
        severity: 'WARNING',
        message: '作业处于暂停状态',
        pause_time: job.PAUSE_TIME,
        reason: job.REASON_OF_STATE_CHANGED,
        impact: '数据消费中断，可能导致数据积压',
      });
      configScore -= 15;
    }

    if (job.STATE === 'CANCELLED') {
      issues.push({
        type: 'job_cancelled',
        severity: 'CRITICAL',
        message: '作业已被取消',
        end_time: job.END_TIME,
        reason: job.REASON_OF_STATE_CHANGED || job.OTHER_MSG,
        impact: '数据消费完全停止',
      });
      configScore -= 40;
    }

    // 8. 分析统计信息
    let performanceIssues = null;
    if (statistics && statistics.receivedBytes) {
      performanceIssues = this.analyzeRoutineLoadPerformance(statistics);
      if (performanceIssues.warnings.length > 0) {
        warnings.push(...performanceIssues.warnings);
        configScore -= performanceIssues.score_penalty;
      }
    }

    // 9. 生成优化建议
    const optimizationRecommendations = this.generateRoutineLoadOptimizations(
      jobProperties,
      dataSourceProperties,
      statistics,
      job.STATE,
    );

    recommendations.push(...optimizationRecommendations);

    return {
      job_name: job.NAME,
      database: job.DB_NAME,
      table: job.TABLE_NAME,
      state: job.STATE,
      create_time: job.CREATE_TIME,
      data_source: job.DATA_SOURCE_NAME,
      current_tasks: currentTaskNum,
      config_score: Math.max(0, configScore),
      config_health:
        configScore >= 80
          ? 'GOOD'
          : configScore >= 60
            ? 'FAIR'
            : configScore >= 40
              ? 'POOR'
              : 'CRITICAL',
      configuration: {
        job_properties: jobProperties,
        data_source_properties: dataSourceProperties,
        custom_properties: customProperties,
      },
      statistics: statistics,
      issues: issues,
      warnings: warnings,
      recommendations: recommendations,
      performance_analysis: performanceIssues,
    };
  }

  /**
   * 解析 Routine Load 属性字符串
   */
  parseJobProperties(propertiesStr) {
    if (!propertiesStr) return {};

    try {
      // properties 通常是 JSON 字符串
      return JSON.parse(propertiesStr);
    } catch (e) {
      // 如果不是 JSON，尝试解析 key=value 格式
      const properties = {};
      const pairs = propertiesStr.split(/[,;\n]/);
      pairs.forEach((pair) => {
        const match = pair.match(/^\s*(\w+)\s*[:=]\s*(.+?)\s*$/);
        if (match) {
          properties[match[1]] = match[2].replace(/^["']|["']$/g, '');
        }
      });
      return properties;
    }
  }

  /**
   * 分析 Routine Load 性能
   */
  analyzeRoutineLoadPerformance(statistics) {
    const warnings = [];
    let scorePenalty = 0;

    // 解析统计数据
    const receivedBytes = parseFloat(statistics.receivedBytes) || 0;
    const loadedRows = parseFloat(statistics.loadedRows) || 0;
    const errorRows = parseFloat(statistics.errorRows) || 0;
    const totalRows = loadedRows + errorRows;
    const taskConsumeSecond =
      parseFloat(statistics.currentTaskConsumeSecond) || 0;

    // 1. 检查错误率
    if (totalRows > 0) {
      const errorRate = (errorRows / totalRows) * 100;
      if (errorRate > 5) {
        warnings.push({
          type: 'high_error_rate',
          severity: 'WARNING',
          message: `错误率较高: ${errorRate.toFixed(2)}%`,
          error_rows: errorRows,
          total_rows: totalRows,
          impact: '数据质量问题或格式不匹配',
        });
        scorePenalty += 15;
      }
    }

    // 2. 检查消费速度
    if (
      taskConsumeSecond >
      this.rules.routine_load.recommended_task_consume_second * 2
    ) {
      warnings.push({
        type: 'slow_consume_speed',
        severity: 'WARNING',
        message: `消费速度较慢: ${taskConsumeSecond.toFixed(1)}秒/批次`,
        current_value: taskConsumeSecond,
        recommended_value:
          this.rules.routine_load.recommended_task_consume_second,
        impact: '数据消费缓慢可能导致延迟累积',
      });
      scorePenalty += 10;
    }

    // 3. 检查吞吐量
    if (receivedBytes > 0 && taskConsumeSecond > 0) {
      const throughputMBps = receivedBytes / taskConsumeSecond / 1024 / 1024;
      if (throughputMBps < 1) {
        warnings.push({
          type: 'low_throughput',
          severity: 'INFO',
          message: `吞吐量较低: ${throughputMBps.toFixed(2)} MB/s`,
          suggestion: '考虑优化批次大小或增加并发数',
        });
        scorePenalty += 5;
      }
    }

    return {
      warnings: warnings,
      score_penalty: scorePenalty,
      metrics: {
        received_bytes: receivedBytes,
        loaded_rows: loadedRows,
        error_rows: errorRows,
        error_rate:
          totalRows > 0 ? ((errorRows / totalRows) * 100).toFixed(2) : 0,
        task_consume_second: taskConsumeSecond,
        throughput_mbps:
          receivedBytes > 0 && taskConsumeSecond > 0
            ? (receivedBytes / taskConsumeSecond / 1024 / 1024).toFixed(2)
            : 0,
      },
    };
  }

  /**
   * 生成 Routine Load 优化建议
   */
  generateRoutineLoadOptimizations(
    jobProperties,
    dataSourceProperties,
    statistics,
    jobState,
  ) {
    const recommendations = [];

    // 1. 并发优化
    const desiredConcurrentNum =
      parseInt(jobProperties.desired_concurrent_number) || 1;
    if (desiredConcurrentNum === 1 && statistics?.receivedBytes > 100000000) {
      recommendations.push({
        type: 'increase_concurrency',
        priority: 'MEDIUM',
        message: '数据量较大，建议增加并发数',
        current_value: desiredConcurrentNum,
        suggested_value: '2-3',
        command:
          'ALTER ROUTINE LOAD FOR <job_name> PROPERTIES("desired_concurrent_number" = "2")',
      });
    }

    // 2. 批次大小优化
    const maxBatchRows = parseInt(jobProperties.max_batch_rows) || 200000;
    const taskConsumeSecond =
      parseFloat(statistics?.currentTaskConsumeSecond) || 0;

    if (taskConsumeSecond > 5 && maxBatchRows > 100000) {
      recommendations.push({
        type: 'reduce_batch_size',
        priority: 'HIGH',
        message: '消费速度慢，建议减小批次大小',
        current_value: maxBatchRows,
        suggested_value: '50000-100000',
        command:
          'ALTER ROUTINE LOAD FOR <job_name> PROPERTIES("max_batch_rows" = "100000")',
      });
    }

    // 3. 状态恢复建议
    if (jobState === 'PAUSED') {
      recommendations.push({
        type: 'resume_job',
        priority: 'HIGH',
        message: '作业已暂停，建议检查原因后恢复',
        command: 'RESUME ROUTINE LOAD FOR <job_name>',
      });
    }

    // 4. Kafka 消费组优化
    if (dataSourceProperties.kafka_topic && !dataSourceProperties.property) {
      recommendations.push({
        type: 'kafka_consumer_properties',
        priority: 'LOW',
        message: '建议配置 Kafka consumer 属性以优化消费行为',
        example:
          'property.group.id, property.client.id, property.max.poll.records',
      });
    }

    return recommendations;
  }

  /**
   * 生成综合评估
   */
  generateRoutineLoadOverallAssessment(jobAnalysis) {
    const totalJobs = jobAnalysis.length;
    const healthyJobs = jobAnalysis.filter(
      (j) => j.config_health === 'GOOD',
    ).length;
    const criticalJobs = jobAnalysis.filter(
      (j) => j.config_health === 'CRITICAL',
    ).length;
    const pausedJobs = jobAnalysis.filter((j) => j.state === 'PAUSED').length;
    const cancelledJobs = jobAnalysis.filter(
      (j) => j.state === 'CANCELLED',
    ).length;

    const avgConfigScore =
      jobAnalysis.reduce((sum, j) => sum + j.config_score, 0) / totalJobs;

    let overallHealth = 'GOOD';
    if (criticalJobs > 0 || cancelledJobs > 0) {
      overallHealth = 'CRITICAL';
    } else if (pausedJobs > 0 || avgConfigScore < 70) {
      overallHealth = 'WARNING';
    }

    return {
      total_jobs: totalJobs,
      healthy_jobs: healthyJobs,
      critical_jobs: criticalJobs,
      paused_jobs: pausedJobs,
      cancelled_jobs: cancelledJobs,
      average_config_score: Math.round(avgConfigScore),
      overall_health: overallHealth,
      summary:
        overallHealth === 'CRITICAL'
          ? `发现 ${criticalJobs + cancelledJobs} 个严重问题的作业，需要立即处理`
          : overallHealth === 'WARNING'
            ? `${pausedJobs} 个作业已暂停，建议检查配置`
            : '所有 Routine Load 作业配置健康',
    };
  }

  /**
   * 格式化 Routine Load 配置检查报告
   */
  formatRoutineLoadConfigReport(result) {
    if (result.status !== 'completed') {
      return `❌ Routine Load 配置检查失败: ${result.error || result.message}`;
    }

    let report = '📊 Routine Load 作业配置检查报告\n';
    report += '==========================================\n\n';

    // 综合评估
    const assessment = result.overall_assessment;
    report += '📈 综合评估:\n';
    report += `   总作业数: ${assessment.total_jobs}\n`;
    report += `   健康作业: ${assessment.healthy_jobs}\n`;
    report += `   平均配置分数: ${assessment.average_config_score}/100\n`;
    report += `   整体健康度: ${assessment.overall_health}\n`;
    if (assessment.paused_jobs > 0) {
      report += `   ⚠️  暂停作业: ${assessment.paused_jobs}\n`;
    }
    if (assessment.cancelled_jobs > 0) {
      report += `   ❌ 取消作业: ${assessment.cancelled_jobs}\n`;
    }
    report += `\n   ${assessment.summary}\n\n`;

    // 详细作业分析
    report += '📋 详细作业分析:\n';
    report += '==========================================\n\n';

    for (const job of result.job_analysis) {
      const healthIcon =
        job.config_health === 'GOOD'
          ? '✅'
          : job.config_health === 'FAIR'
            ? '⚠️'
            : '❌';

      report += `${healthIcon} **${job.job_name}** (${job.database}.${job.table})\n`;
      report += `   状态: ${job.state}\n`;
      report += `   配置健康度: ${job.config_health} (${job.config_score}/100)\n`;
      report += `   当前任务数: ${job.current_tasks}\n`;

      // 关键配置
      const config = job.configuration.job_properties;
      report += `   关键配置:\n`;
      if (config.desired_concurrent_number) {
        report += `     - 并发数: ${config.desired_concurrent_number}\n`;
      }
      if (config.max_batch_interval) {
        report += `     - 批次间隔: ${config.max_batch_interval}s\n`;
      }
      if (config.max_batch_rows) {
        report += `     - 批次行数: ${parseInt(config.max_batch_rows).toLocaleString()}\n`;
      }
      if (config.max_error_number) {
        report += `     - 最大错误数: ${parseInt(config.max_error_number).toLocaleString()}\n`;
      }

      // 性能指标
      if (job.performance_analysis) {
        const perf = job.performance_analysis.metrics;
        report += `   性能指标:\n`;
        report += `     - 已加载行数: ${parseInt(perf.loaded_rows).toLocaleString()}\n`;
        report += `     - 错误率: ${perf.error_rate}%\n`;
        report += `     - 消费速度: ${perf.task_consume_second}s/批次\n`;
        if (perf.throughput_mbps > 0) {
          report += `     - 吞吐量: ${perf.throughput_mbps} MB/s\n`;
        }
      }

      // 问题和警告
      if (job.issues.length > 0) {
        report += `   ❌ 问题 (${job.issues.length}):\n`;
        job.issues.slice(0, 3).forEach((issue) => {
          report += `     - ${issue.message}\n`;
        });
      }

      if (job.warnings.length > 0) {
        report += `   ⚠️  警告 (${job.warnings.length}):\n`;
        job.warnings.slice(0, 3).forEach((warning) => {
          report += `     - ${warning.message}\n`;
        });
      }

      // 优化建议
      if (job.recommendations.length > 0) {
        report += `   💡 优化建议:\n`;
        job.recommendations.slice(0, 3).forEach((rec) => {
          const priorityIcon =
            rec.priority === 'HIGH'
              ? '🔥'
              : rec.priority === 'MEDIUM'
                ? '⚠️'
                : 'ℹ️';
          report += `     ${priorityIcon} ${rec.message}\n`;
          if (rec.command) {
            report += `        命令: ${rec.command.replace('<job_name>', job.job_name)}\n`;
          }
        });
      }

      report += '\n';
    }

    return report;
  }

  /**
   * 检查表的 Stream Load 任务
   * @param {Object} connection - 数据库连接
   * @param {string} dbName - 数据库名称
   * @param {string} tableName - 表名称
   * @param {number} days - 分析天数（默认7天）
   * @returns {Object} Stream Load 任务检查结果
   */
  async checkStreamLoadTasks(connection, dbName, tableName, days = 7) {
    console.error(
      `🔍 开始检查表 ${dbName}.${tableName} 的 Stream Load 任务...`,
    );
    const startTime = Date.now();

    try {
      // 1. 查询 Stream Load 历史数据（字段名已与表结构对照确认）
      const query = `
        SELECT
          label,
          db_name,
          table_name,
          state,
          create_time,
          load_start_time,
          load_finish_time,
          load_commit_time,
          scan_rows,
          scan_bytes,
          filtered_rows,
          unselected_rows,
          sink_rows,
          error_msg,
          tracking_sql,
          type
        FROM _statistics_.loads_history
        WHERE db_name = ?
          AND table_name = ?
          AND type = 'STREAM_LOAD'
          AND create_time >= DATE_SUB(NOW(), INTERVAL ? DAY)
        ORDER BY create_time DESC
      `;

      const [loads] = await connection.query(query, [dbName, tableName, days]);

      if (!loads || loads.length === 0) {
        return {
          status: 'no_data',
          message: `表 ${dbName}.${tableName} 在最近 ${days} 天内没有 Stream Load 任务记录`,
          analysis_duration_ms: Date.now() - startTime,
        };
      }

      // 2. 基础统计
      const statistics = this.calculateStreamLoadStatistics(loads);

      // 3. 频率分析
      const frequencyAnalysis = this.analyzeStreamLoadFrequency(loads);

      // 4. 批次大小分析
      const batchSizeAnalysis = this.analyzeStreamLoadBatchSize(loads);

      // 5. 性能分析
      const performanceAnalysis = this.analyzeStreamLoadPerformance(loads);

      // 6. 失败分析
      const failureAnalysis = this.analyzeStreamLoadFailures(loads);

      // 7. 慢任务分析
      const slowTaskAnalysis = this.analyzeSlowStreamLoadTasks(loads, 10);

      // 8. 生成问题和建议
      const issuesAndRecommendations =
        this.generateStreamLoadIssuesAndRecommendations(
          statistics,
          frequencyAnalysis,
          batchSizeAnalysis,
          performanceAnalysis,
          failureAnalysis,
        );

      // 9. 计算健康分数
      const healthScore = this.calculateStreamLoadHealthScore(
        statistics,
        frequencyAnalysis,
        performanceAnalysis,
      );

      console.error(
        `✅ Stream Load 任务检查完成，耗时 ${Date.now() - startTime}ms`,
      );

      return {
        status: 'completed',
        analysis_type: 'stream_load_task_check',
        database: dbName,
        table: tableName,
        analysis_period_days: days,
        analysis_duration_ms: Date.now() - startTime,
        health_score: healthScore,
        statistics: statistics,
        frequency_analysis: frequencyAnalysis,
        batch_size_analysis: batchSizeAnalysis,
        performance_analysis: performanceAnalysis,
        failure_analysis: failureAnalysis,
        slow_task_analysis: slowTaskAnalysis,
        issues: issuesAndRecommendations.issues,
        warnings: issuesAndRecommendations.warnings,
        recommendations: issuesAndRecommendations.recommendations,
      };
    } catch (error) {
      console.error(`❌ Stream Load 任务检查失败: ${error.message}`);
      return {
        status: 'error',
        error: error.message,
        analysis_duration_ms: Date.now() - startTime,
      };
    }
  }

  /**
   * 计算 Stream Load 基础统计
   */
  calculateStreamLoadStatistics(loads) {
    const totalLoads = loads.length;
    const successLoadsArray = loads.filter((l) => l.state === 'FINISHED');
    const successLoads = successLoadsArray.length;
    const failedLoads = loads.filter((l) => l.state === 'CANCELLED').length;
    const successRate = (successLoads / totalLoads) * 100;

    // 计算总行数和字节数（所有任务）
    const totalScanRows = loads.reduce((sum, l) => sum + (l.scan_rows || 0), 0);
    const totalScanBytes = loads.reduce(
      (sum, l) => sum + (l.scan_bytes || 0),
      0,
    );
    const totalSinkRows = loads.reduce((sum, l) => sum + (l.sink_rows || 0), 0);
    const totalFilteredRows = loads.reduce(
      (sum, l) => sum + (l.filtered_rows || 0),
      0,
    );

    // 计算成功任务的平均值
    const successSinkRows = successLoadsArray.reduce(
      (sum, l) => sum + (l.sink_rows || 0),
      0,
    );
    const successScanBytes = successLoadsArray.reduce(
      (sum, l) => sum + (l.scan_bytes || 0),
      0,
    );

    // 时间跨度
    const firstLoad = loads[loads.length - 1];
    const lastLoad = loads[0];
    const timeSpanSeconds =
      (new Date(lastLoad.create_time) - new Date(firstLoad.create_time)) / 1000;

    return {
      total_loads: totalLoads,
      success_loads: successLoads,
      failed_loads: failedLoads,
      success_rate: parseFloat(successRate.toFixed(2)),
      total_scan_rows: totalScanRows,
      total_scan_bytes: totalScanBytes,
      total_sink_rows: totalSinkRows,
      total_filtered_rows: totalFilteredRows,
      avg_rows_per_load:
        successLoads > 0 ? Math.round(successSinkRows / successLoads) : 0,
      avg_bytes_per_load:
        successLoads > 0 ? Math.round(successScanBytes / successLoads) : 0,
      time_span_seconds: Math.round(timeSpanSeconds),
      first_load_time: firstLoad.create_time,
      last_load_time: lastLoad.create_time,
    };
  }

  /**
   * 分析 Stream Load 频率
   */
  analyzeStreamLoadFrequency(loads) {
    if (loads.length < 2) {
      return {
        status: 'insufficient_data',
        message: '数据量不足，无法分析频率',
      };
    }

    // 按时间排序
    const sortedLoads = [...loads].sort(
      (a, b) => new Date(a.create_time) - new Date(b.create_time),
    );

    // 计算间隔
    const intervals = [];
    for (let i = 1; i < sortedLoads.length; i++) {
      const interval =
        (new Date(sortedLoads[i].create_time) -
          new Date(sortedLoads[i - 1].create_time)) /
        1000;
      intervals.push(interval);
    }

    // 统计指标
    const avgInterval =
      intervals.reduce((sum, val) => sum + val, 0) / intervals.length;
    const minInterval = Math.min(...intervals);
    const maxInterval = Math.max(...intervals);

    // 频率等级
    let frequencyLevel = 'low';
    let frequencyDescription = '';
    const loadsPerMinute = 60 / avgInterval;
    const loadsPerHour = 3600 / avgInterval;

    if (loadsPerMinute > 60) {
      frequencyLevel = 'extreme';
      frequencyDescription = '极高频 (每秒多次)';
    } else if (loadsPerMinute > 4) {
      frequencyLevel = 'very_high';
      frequencyDescription = '超高频 (每分钟4+次)';
    } else if (loadsPerMinute > 1) {
      frequencyLevel = 'high';
      frequencyDescription = '高频 (每分钟1+次)';
    } else if (loadsPerHour > 1) {
      frequencyLevel = 'moderate';
      frequencyDescription = '中等 (每小时1+次)';
    } else {
      frequencyLevel = 'low';
      frequencyDescription = '低频 (每小时<1次)';
    }

    return {
      avg_interval_seconds: parseFloat(avgInterval.toFixed(2)),
      min_interval_seconds: parseFloat(minInterval.toFixed(2)),
      max_interval_seconds: parseFloat(maxInterval.toFixed(2)),
      loads_per_minute: parseFloat(loadsPerMinute.toFixed(2)),
      loads_per_hour: parseFloat(loadsPerHour.toFixed(1)),
      loads_per_day: parseFloat((loadsPerHour * 24).toFixed(0)),
      frequency_level: frequencyLevel,
      frequency_description: frequencyDescription,
    };
  }

  /**
   * 分析批次大小（仅分析成功的任务）
   */
  analyzeStreamLoadBatchSize(loads) {
    // 只分析成功的任务
    const successLoads = loads.filter((l) => l.state === 'FINISHED');

    if (successLoads.length === 0) {
      return {
        status: 'no_success_loads',
        message: '没有成功的任务可以分析',
      };
    }

    const rowCounts = successLoads.map((l) => l.sink_rows || 0);
    const byteSizes = successLoads.map((l) => l.scan_bytes || 0);

    // 行数统计
    const avgRows =
      rowCounts.reduce((sum, val) => sum + val, 0) / successLoads.length;
    const minRows = Math.min(...rowCounts);
    const maxRows = Math.max(...rowCounts);
    const medianRows = this.calculateMedian(rowCounts);

    // 字节数统计
    const avgBytes =
      byteSizes.reduce((sum, val) => sum + val, 0) / successLoads.length;
    const minBytes = Math.min(...byteSizes);
    const maxBytes = Math.max(...byteSizes);
    const medianBytes = this.calculateMedian(byteSizes);

    // 标准差
    const rowsStdDev = Math.sqrt(
      rowCounts.reduce((sum, val) => sum + Math.pow(val - avgRows, 2), 0) /
        successLoads.length,
    );
    const bytesStdDev = Math.sqrt(
      byteSizes.reduce((sum, val) => sum + Math.pow(val - avgBytes, 2), 0) /
        successLoads.length,
    );

    // 批次大小分布
    const distribution = this.calculateBatchSizeDistribution(rowCounts);

    // 一致性评分
    const rowsCv = rowsStdDev / avgRows;
    let consistency = 'poor';
    let consistencyScore = 0;

    if (rowsCv < 0.1) {
      consistency = 'excellent';
      consistencyScore = 95;
    } else if (rowsCv < 0.3) {
      consistency = 'good';
      consistencyScore = 80;
    } else if (rowsCv < 0.5) {
      consistency = 'fair';
      consistencyScore = 60;
    } else {
      consistency = 'poor';
      consistencyScore = 40;
    }

    return {
      analyzed_success_loads: successLoads.length,
      rows: {
        avg: Math.round(avgRows),
        min: minRows,
        max: maxRows,
        median: Math.round(medianRows),
        std_dev: Math.round(rowsStdDev),
        coefficient_of_variation: parseFloat((rowsCv * 100).toFixed(2)),
      },
      bytes: {
        avg: Math.round(avgBytes),
        min: minBytes,
        max: maxBytes,
        median: Math.round(medianBytes),
        std_dev: Math.round(bytesStdDev),
        avg_mb: parseFloat((avgBytes / 1024 / 1024).toFixed(2)),
      },
      distribution: distribution,
      consistency: consistency,
      consistency_score: consistencyScore,
    };
  }

  /**
   * 计算中位数
   */
  calculateMedian(values) {
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid];
  }

  /**
   * 计算批次大小分布
   */
  calculateBatchSizeDistribution(rowCounts) {
    const ranges = {
      tiny: 0, // < 1K
      small: 0, // 1K - 10K
      medium: 0, // 10K - 100K
      large: 0, // 100K - 1M
      huge: 0, // > 1M
    };

    rowCounts.forEach((count) => {
      if (count < 1000) ranges.tiny++;
      else if (count < 10000) ranges.small++;
      else if (count < 100000) ranges.medium++;
      else if (count < 1000000) ranges.large++;
      else ranges.huge++;
    });

    const total = rowCounts.length;
    return {
      tiny: {
        count: ranges.tiny,
        percentage: ((ranges.tiny / total) * 100).toFixed(1),
      },
      small: {
        count: ranges.small,
        percentage: ((ranges.small / total) * 100).toFixed(1),
      },
      medium: {
        count: ranges.medium,
        percentage: ((ranges.medium / total) * 100).toFixed(1),
      },
      large: {
        count: ranges.large,
        percentage: ((ranges.large / total) * 100).toFixed(1),
      },
      huge: {
        count: ranges.huge,
        percentage: ((ranges.huge / total) * 100).toFixed(1),
      },
    };
  }

  /**
   * 分析 Stream Load 性能
   */
  analyzeStreamLoadPerformance(loads) {
    const successLoads = loads.filter((l) => l.state === 'FINISHED');

    if (successLoads.length === 0) {
      return {
        status: 'no_success_loads',
        message: '没有成功的加载任务',
      };
    }

    // 计算加载耗时
    const durations = successLoads
      .map((l) => {
        if (!l.load_start_time || !l.load_finish_time) return null;
        return (
          (new Date(l.load_finish_time) - new Date(l.load_start_time)) / 1000
        );
      })
      .filter((d) => d !== null && d > 0);

    // 计算吞吐量
    const throughputs = successLoads
      .map((l) => {
        if (!l.load_start_time || !l.load_finish_time || !l.scan_bytes)
          return null;
        const duration =
          (new Date(l.load_finish_time) - new Date(l.load_start_time)) / 1000;
        if (duration <= 0) return null;
        return l.scan_bytes / duration / 1024 / 1024; // MB/s
      })
      .filter((t) => t !== null && t > 0);

    const avgDuration =
      durations.length > 0
        ? durations.reduce((sum, val) => sum + val, 0) / durations.length
        : 0;
    const avgThroughput =
      throughputs.length > 0
        ? throughputs.reduce((sum, val) => sum + val, 0) / throughputs.length
        : 0;
    const minThroughput = throughputs.length > 0 ? Math.min(...throughputs) : 0;
    const maxThroughput = throughputs.length > 0 ? Math.max(...throughputs) : 0;

    return {
      avg_load_duration_seconds: parseFloat(avgDuration.toFixed(2)),
      avg_throughput_mbps: parseFloat(avgThroughput.toFixed(2)),
      min_throughput_mbps: parseFloat(minThroughput.toFixed(2)),
      max_throughput_mbps: parseFloat(maxThroughput.toFixed(2)),
      analyzed_tasks: durations.length,
    };
  }

  /**
   * 分析失败情况
   */
  analyzeStreamLoadFailures(loads) {
    const failedLoads = loads.filter((l) => l.state === 'CANCELLED');

    if (failedLoads.length === 0) {
      return {
        failed_count: 0,
        failure_rate: 0,
        message: '没有失败的任务',
      };
    }

    // 统计错误类型
    const errorTypes = {};
    failedLoads.forEach((load) => {
      if (!load.error_msg) return;
      const errorMsg = load.error_msg.toLowerCase();

      let errorType = 'unknown';
      if (errorMsg.includes('timeout')) errorType = 'timeout';
      else if (errorMsg.includes('format') || errorMsg.includes('parse'))
        errorType = 'format_error';
      else if (errorMsg.includes('permission') || errorMsg.includes('access'))
        errorType = 'permission_error';
      else if (errorMsg.includes('memory') || errorMsg.includes('oom'))
        errorType = 'memory_error';
      else if (errorMsg.includes('duplicate') || errorMsg.includes('key'))
        errorType = 'duplicate_key';

      errorTypes[errorType] = (errorTypes[errorType] || 0) + 1;
    });

    return {
      failed_count: failedLoads.length,
      failure_rate: parseFloat(
        ((failedLoads.length / loads.length) * 100).toFixed(2),
      ),
      error_types: errorTypes,
      recent_failures: failedLoads.slice(0, 5).map((l) => ({
        label: l.label,
        create_time: l.create_time,
        error: l.error_msg,
      })),
    };
  }

  /**
   * 分析慢 Stream Load 任务
   */
  analyzeSlowStreamLoadTasks(loads, thresholdSeconds = 10) {
    // 只分析成功的任务
    const successLoads = loads.filter((l) => l.state === 'FINISHED');

    if (successLoads.length === 0) {
      return {
        status: 'no_success_loads',
        message: '没有成功的任务可以分析',
      };
    }

    // 找到慢任务
    const slowTasks = [];
    successLoads.forEach((load) => {
      const createTime = new Date(load.create_time);
      const finishTime = new Date(load.load_finish_time);
      const commitTime = new Date(load.load_commit_time);

      // 总耗时 = load_finish_time - create_time
      const totalDuration = (finishTime - createTime) / 1000; // 秒

      if (totalDuration > thresholdSeconds) {
        // 数据写入耗时 = load_commit_time - create_time
        const writeDuration = (commitTime - createTime) / 1000;
        // 事务提交耗时 = load_finish_time - load_commit_time
        const commitDuration = (finishTime - commitTime) / 1000;

        slowTasks.push({
          label: load.label,
          create_time: load.create_time,
          load_finish_time: load.load_finish_time,
          load_commit_time: load.load_commit_time,
          total_duration_seconds: parseFloat(totalDuration.toFixed(2)),
          write_duration_seconds: parseFloat(writeDuration.toFixed(2)),
          commit_duration_seconds: parseFloat(commitDuration.toFixed(2)),
          sink_rows: load.sink_rows || 0,
          scan_bytes: load.scan_bytes || 0,
          write_throughput_mbps: parseFloat(
            ((load.scan_bytes || 0) / 1024 / 1024 / writeDuration).toFixed(2),
          ),
        });
      }
    });

    if (slowTasks.length === 0) {
      return {
        status: 'ok',
        message: `没有超过 ${thresholdSeconds} 秒的慢任务`,
        threshold_seconds: thresholdSeconds,
        analyzed_success_loads: successLoads.length,
      };
    }

    // 按总耗时排序
    slowTasks.sort(
      (a, b) => b.total_duration_seconds - a.total_duration_seconds,
    );

    // 计算统计信息
    const avgTotalDuration =
      slowTasks.reduce((sum, t) => sum + t.total_duration_seconds, 0) /
      slowTasks.length;
    const avgWriteDuration =
      slowTasks.reduce((sum, t) => sum + t.write_duration_seconds, 0) /
      slowTasks.length;
    const avgCommitDuration =
      slowTasks.reduce((sum, t) => sum + t.commit_duration_seconds, 0) /
      slowTasks.length;

    // 计算写入和提交耗时的占比
    const writeRatio = (avgWriteDuration / avgTotalDuration) * 100;
    const commitRatio = (avgCommitDuration / avgTotalDuration) * 100;

    return {
      status: 'found_slow_tasks',
      threshold_seconds: thresholdSeconds,
      analyzed_success_loads: successLoads.length,
      slow_task_count: slowTasks.length,
      slow_task_ratio: parseFloat(
        ((slowTasks.length / successLoads.length) * 100).toFixed(2),
      ),
      statistics: {
        avg_total_duration_seconds: parseFloat(avgTotalDuration.toFixed(2)),
        avg_write_duration_seconds: parseFloat(avgWriteDuration.toFixed(2)),
        avg_commit_duration_seconds: parseFloat(avgCommitDuration.toFixed(2)),
        write_duration_ratio: parseFloat(writeRatio.toFixed(2)),
        commit_duration_ratio: parseFloat(commitRatio.toFixed(2)),
      },
      slowest_tasks: slowTasks.slice(0, 10), // 返回前10个最慢的任务
    };
  }

  /**
   * 生成问题和建议
   */
  generateStreamLoadIssuesAndRecommendations(
    statistics,
    frequencyAnalysis,
    batchSizeAnalysis,
    performanceAnalysis,
    failureAnalysis,
  ) {
    const issues = [];
    const warnings = [];
    const recommendations = [];

    // 1. 失败率检查
    if (failureAnalysis.failure_rate > 10) {
      issues.push({
        type: 'high_failure_rate',
        severity: 'HIGH',
        message: `失败率过高: ${failureAnalysis.failure_rate}%`,
        impact: '数据导入质量受影响',
      });
      recommendations.push({
        priority: 'HIGH',
        message: '检查失败原因，修复数据格式或配置问题',
        error_types: failureAnalysis.error_types,
      });
    } else if (failureAnalysis.failure_rate > 5) {
      warnings.push({
        type: 'moderate_failure_rate',
        severity: 'MEDIUM',
        message: `失败率较高: ${failureAnalysis.failure_rate}%`,
      });
    }

    // 2. 频率检查
    if (
      frequencyAnalysis.frequency_level === 'extreme' ||
      frequencyAnalysis.frequency_level === 'very_high'
    ) {
      warnings.push({
        type: 'very_high_frequency',
        severity: 'MEDIUM',
        message: `导入频率过高: ${frequencyAnalysis.loads_per_minute.toFixed(1)} 次/分钟`,
        impact: '可能导致系统负载过高',
      });
      recommendations.push({
        priority: 'MEDIUM',
        message: '考虑合并小批次，减少导入频率',
        suggestion: `当前平均间隔 ${frequencyAnalysis.avg_interval_seconds}秒，建议增加到 30-60秒`,
      });
    }

    // 3. 批次大小检查
    if (batchSizeAnalysis.consistency === 'poor') {
      warnings.push({
        type: 'inconsistent_batch_size',
        severity: 'LOW',
        message: '批次大小不一致',
        cv: `变异系数 ${batchSizeAnalysis.rows.coefficient_of_variation}%`,
      });
      recommendations.push({
        priority: 'LOW',
        message: '建立更一致的批次大小策略，提高可预测性',
      });
    }

    if (statistics.avg_rows_per_load < 1000) {
      warnings.push({
        type: 'small_batch_size',
        severity: 'MEDIUM',
        message: `批次过小: 平均 ${statistics.avg_rows_per_load.toLocaleString()} 行`,
        impact: '导入效率低下',
      });
      recommendations.push({
        priority: 'HIGH',
        message: '增加批次大小以提高吞吐量',
        suggestion: '建议每批次至少 10,000 行',
      });
    }

    // 4. 性能检查
    if (
      performanceAnalysis.avg_throughput_mbps &&
      performanceAnalysis.avg_throughput_mbps < 10
    ) {
      warnings.push({
        type: 'low_throughput',
        severity: 'MEDIUM',
        message: `吞吐量较低: ${performanceAnalysis.avg_throughput_mbps} MB/s`,
      });
      recommendations.push({
        priority: 'MEDIUM',
        message: '优化批次大小或增加并行度以提高吞吐量',
      });
    }

    // 5. 规律性检查
    if (
      frequencyAnalysis.regularity === 'irregular' &&
      frequencyAnalysis.loads_per_hour > 10
    ) {
      recommendations.push({
        priority: 'LOW',
        message: '建立规律的导入调度，提高系统可预测性',
        regularity_score: frequencyAnalysis.regularity_score,
      });
    }

    return {
      issues,
      warnings,
      recommendations,
    };
  }

  /**
   * 计算健康分数
   */
  calculateStreamLoadHealthScore(
    statistics,
    frequencyAnalysis,
    performanceAnalysis,
  ) {
    let score = 100;

    // 成功率影响
    if (statistics.success_rate < 90) score -= 20;
    else if (statistics.success_rate < 95) score -= 10;
    else if (statistics.success_rate < 99) score -= 5;

    // 频率规律性影响
    score -= (100 - frequencyAnalysis.regularity_score) * 0.2;

    // 吞吐量影响
    if (
      performanceAnalysis.avg_throughput_mbps &&
      performanceAnalysis.avg_throughput_mbps < 5
    ) {
      score -= 15;
    } else if (
      performanceAnalysis.avg_throughput_mbps &&
      performanceAnalysis.avg_throughput_mbps < 10
    ) {
      score -= 5;
    }

    score = Math.max(0, Math.min(100, score));

    let level = 'EXCELLENT';
    if (score < 50) level = 'POOR';
    else if (score < 70) level = 'FAIR';
    else if (score < 85) level = 'GOOD';

    return {
      score: Math.round(score),
      level: level,
    };
  }

  /**
   * 格式化 Stream Load 检查报告
   */
  formatStreamLoadTasksReport(result) {
    if (result.status === 'no_data') {
      return `ℹ️  ${result.message}`;
    }

    if (result.status !== 'completed') {
      return `❌ Stream Load 检查失败: ${result.error}`;
    }

    let report = `📊 Stream Load 任务检查报告\n`;
    report += `==========================================\n`;
    report += `表: ${result.database}.${result.table}\n`;
    report += `分析周期: 最近 ${result.analysis_period_days} 天\n`;
    report += `健康评分: ${result.health_score.score}/100 (${result.health_score.level})\n\n`;

    // 基础统计
    const stats = result.statistics;
    report += `📈 基础统计:\n`;
    report += `   总任务数: ${stats.total_loads.toLocaleString()}\n`;
    report += `   成功任务: ${stats.success_loads.toLocaleString()} (${stats.success_rate}%)\n`;
    report += `   失败任务: ${stats.failed_loads.toLocaleString()}\n`;
    report += `   总扫描行: ${stats.total_scan_rows.toLocaleString()}\n`;
    report += `   总数据量: ${this.formatBytes(stats.total_scan_bytes)}\n`;
    report += `   平均行数/任务: ${stats.avg_rows_per_load.toLocaleString()}\n`;
    report += `   平均数据量/任务: ${this.formatBytes(stats.avg_bytes_per_load)}\n\n`;

    // 频率分析
    const freq = result.frequency_analysis;
    if (freq.status !== 'insufficient_data') {
      report += `⏱️  频率分析:\n`;
      report += `   频率等级: ${freq.frequency_description}\n`;
      report += `   每分钟: ${freq.loads_per_minute} 次\n`;
      report += `   每小时: ${freq.loads_per_hour} 次\n`;
      report += `   每天: ${freq.loads_per_day} 次\n`;
      report += `   平均间隔: ${freq.avg_interval_seconds} 秒\n\n`;
    }

    // 批次大小分析
    const batch = result.batch_size_analysis;
    if (batch.status !== 'no_success_loads') {
      report += `📦 批次大小分析（基于成功任务）:\n`;
      report += `   分析任务数: ${batch.analyzed_success_loads}\n`;
      report += `   平均行数: ${batch.rows.avg.toLocaleString()}\n`;
      report += `   行数范围: ${batch.rows.min.toLocaleString()} - ${batch.rows.max.toLocaleString()}\n`;
      report += `   中位数: ${batch.rows.median.toLocaleString()}\n`;
      report += `   一致性: ${batch.consistency} (评分: ${batch.consistency_score}/100)\n`;
      report += `   平均数据量: ${batch.bytes.avg_mb} MB\n`;
      report += `   批次分布:\n`;
      report += `     - 微小 (<1K): ${batch.distribution.tiny.count} (${batch.distribution.tiny.percentage}%)\n`;
      report += `     - 小 (1K-10K): ${batch.distribution.small.count} (${batch.distribution.small.percentage}%)\n`;
      report += `     - 中 (10K-100K): ${batch.distribution.medium.count} (${batch.distribution.medium.percentage}%)\n`;
      report += `     - 大 (100K-1M): ${batch.distribution.large.count} (${batch.distribution.large.percentage}%)\n`;
      report += `     - 巨大 (>1M): ${batch.distribution.huge.count} (${batch.distribution.huge.percentage}%)\n\n`;
    } else {
      report += `📦 批次大小分析:\n`;
      report += `   ℹ️  ${batch.message}\n\n`;
    }

    // 性能分析
    const perf = result.performance_analysis;
    if (perf.status !== 'no_success_loads') {
      report += `🚀 性能分析:\n`;
      report += `   平均加载耗时: ${perf.avg_load_duration_seconds} 秒\n`;
      report += `   平均吞吐量: ${perf.avg_throughput_mbps} MB/s\n`;
      report += `   吞吐量范围: ${perf.min_throughput_mbps} - ${perf.max_throughput_mbps} MB/s\n`;
      report += `   分析任务数: ${perf.analyzed_tasks}\n\n`;
    }

    // 失败分析
    const failure = result.failure_analysis;
    if (failure.failed_count > 0) {
      report += `❌ 失败分析:\n`;
      report += `   失败数量: ${failure.failed_count}\n`;
      report += `   失败率: ${failure.failure_rate}%\n`;
      if (Object.keys(failure.error_types).length > 0) {
        report += `   错误类型:\n`;
        Object.entries(failure.error_types).forEach(([type, count]) => {
          report += `     - ${type}: ${count}\n`;
        });
      }
      report += '\n';
    }

    // 慢任务分析
    const slowTask = result.slow_task_analysis;
    if (slowTask.status === 'found_slow_tasks') {
      report += `🐌 慢任务分析 (阈值: ${slowTask.threshold_seconds}秒):\n`;
      report += `   分析任务数: ${slowTask.analyzed_success_loads}\n`;
      report += `   慢任务数量: ${slowTask.slow_task_count}\n`;
      report += `   慢任务占比: ${slowTask.slow_task_ratio}%\n`;
      report += `   平均总耗时: ${slowTask.statistics.avg_total_duration_seconds}秒\n`;
      report += `     - 写入耗时: ${slowTask.statistics.avg_write_duration_seconds}秒 (${slowTask.statistics.write_duration_ratio}%)\n`;
      report += `     - 提交耗时: ${slowTask.statistics.avg_commit_duration_seconds}秒 (${slowTask.statistics.commit_duration_ratio}%)\n`;

      if (slowTask.slowest_tasks && slowTask.slowest_tasks.length > 0) {
        report += `   最慢的前5个任务:\n`;
        slowTask.slowest_tasks.slice(0, 5).forEach((task, index) => {
          report += `     ${index + 1}. ${task.label}\n`;
          report += `        总耗时: ${task.total_duration_seconds}秒 (写入: ${task.write_duration_seconds}s, 提交: ${task.commit_duration_seconds}s)\n`;
          report += `        数据量: ${task.sink_rows.toLocaleString()} 行 / ${this.formatBytes(task.scan_bytes)}\n`;
          report += `        吞吐量: ${task.write_throughput_mbps} MB/s\n`;
        });
      }
      report += '\n';
    }

    // 问题
    if (result.issues.length > 0) {
      report += `⚠️  问题 (${result.issues.length}):\n`;
      result.issues.forEach((issue) => {
        report += `   🔥 ${issue.message}\n`;
        if (issue.impact) report += `      影响: ${issue.impact}\n`;
      });
      report += '\n';
    }

    // 警告
    if (result.warnings.length > 0) {
      report += `💡 警告 (${result.warnings.length}):\n`;
      result.warnings.slice(0, 5).forEach((warning) => {
        report += `   ⚠️  ${warning.message}\n`;
      });
      report += '\n';
    }

    // 建议
    if (result.recommendations.length > 0) {
      report += `✨ 优化建议:\n`;
      result.recommendations.slice(0, 5).forEach((rec, index) => {
        const priorityIcon =
          rec.priority === 'HIGH'
            ? '🔥'
            : rec.priority === 'MEDIUM'
              ? '⚠️'
              : 'ℹ️';
        report += `   ${index + 1}. ${priorityIcon} ${rec.message}\n`;
        if (rec.suggestion) report += `      ${rec.suggestion}\n`;
      });
    }

    return report;
  }

  /**
   * 获取此专家提供的 MCP 工具处理器
   * @returns {Object} 工具名称到处理函数的映射
   */
  getToolHandlers() {
    return {
      analyze_table_import_frequency: async (args, context) => {
        const connection = context.connection;
        const result = await this.analyzeTableImportFrequency(
          connection,
          args.database_name,
          args.table_name,
          args.include_details !== false,
        );

        let report;
        if (result.status === 'completed') {
          report = this.formatTableFrequencyReport(result);
        } else {
          report = `❌ 表 ${args.database_name}.${args.table_name} 导入频率分析失败\n`;
          report += `状态: ${result.status}\n`;
          report += `原因: ${result.error || result.message}\n`;
          report += `耗时: ${result.analysis_duration_ms}ms`;
        }

        // 添加输出指示，引导 LLM 原样输出
        const outputInstruction =
          '📋 以下是预格式化的分析报告，请**原样输出**完整内容，不要总结或重新格式化：\n\n```\n';
        const reportEnd = '\n```\n';

        return {
          content: [
            {
              type: 'text',
              text: outputInstruction + report + reportEnd,
            },
          ],
        };
      },

      check_stream_load_tasks: async (args, context) => {
        const connection = context.connection;
        const result = await this.checkStreamLoadTasks(
          connection,
          args.database_name,
          args.table_name,
          args.days || 7,
        );

        let report;
        if (result.status === 'completed') {
          report = this.formatStreamLoadTasksReport(result);
        } else if (result.status === 'no_data') {
          report = `ℹ️  ${result.message}`;
        } else {
          report = `❌ Stream Load 任务检查失败\n`;
          report += `错误: ${result.error}\n`;
          report += `耗时: ${result.analysis_duration_ms}ms`;
        }

        // 添加输出指示，引导 LLM 原样输出
        const outputInstruction =
          '📋 以下是预格式化的分析报告，请**原样输出**完整内容，不要总结或重新格式化：\n\n```\n';
        const reportEnd = '\n```\n';

        return {
          content: [
            {
              type: 'text',
              text: outputInstruction + report + reportEnd,
            },
          ],
        };
      },

      check_routine_load_config: async (args, context) => {
        const connection = context.connection;
        const result = await this.checkRoutineLoadJobConfig(
          connection,
          args.job_name,
          args.database_name,
        );

        let report;
        if (result.status === 'completed') {
          report = this.formatRoutineLoadConfigReport(result);
        } else if (result.status === 'no_jobs') {
          report = `ℹ️  ${result.message}`;
        } else {
          report = `❌ Routine Load 配置检查失败\n`;
          report += `错误: ${result.error}\n`;
          report += `耗时: ${result.analysis_duration_ms}ms`;
        }

        // 添加输出指示，引导 LLM 原样输出
        const outputInstruction =
          '📋 以下是预格式化的分析报告，请**原样输出**完整内容，不要总结或重新格式化：\n\n```\n';
        const reportEnd = '\n```\n';

        return {
          content: [
            {
              type: 'text',
              text: outputInstruction + report + reportEnd,
            },
          ],
        };
      },
    };
  }

  /**
   * 获取此专家提供的 MCP 工具定义
   */
  getTools() {
    return [
      {
        name: 'analyze_table_import_frequency',
        description:
          '🔍 表级导入频率分析 - 深度分析指定表的导入模式、性能和频率特征。\n\n⚠️ 输出指示：此工具返回预格式化的详细报告，请**完整、原样**输出所有内容，包括所有统计数据、分析结果和建议。不要总结、省略或重新格式化报告内容。',
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
            include_details: {
              type: 'boolean',
              description: '是否包含详细分析数据',
              default: true,
            },
          },
          required: ['database_name', 'table_name'],
        },
      },
      {
        name: 'check_stream_load_tasks',
        description:
          '📊 Stream Load 任务检查 - 专门分析 Stream Load 任务的频率、批次大小和性能。\n\n功能：\n- 分析导入频率（每分钟/每小时/每天）和规律性\n- 统计平均导入行数和批次大小分布\n- 评估性能指标（吞吐量、加载耗时）\n- 检测失败率和错误类型\n- 识别小批次、高频率等性能问题\n- 提供批次大小和频率优化建议\n\n⚠️ 输出指示：此工具返回预格式化的详细报告，请**完整、原样**输出所有内容。不要总结或重新格式化报告内容。',
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
            days: {
              type: 'number',
              description: '分析天数（默认7天）',
              default: 7,
            },
          },
          required: ['database_name', 'table_name'],
        },
      },
      {
        name: 'check_routine_load_config',
        description:
          '🔧 Routine Load 配置检查 - 检查 Routine Load 作业的配置参数，识别潜在问题并提供优化建议。\n\n功能：\n- 检查并发数、批次大小、错误容忍等关键参数\n- 分析 Kafka 分区与并发数的匹配情况\n- 评估作业性能（错误率、消费速度、吞吐量）\n- 检测作业状态异常（暂停、取消）\n- 提供具体的优化建议和 SQL 命令\n\n⚠️ 输出指示：此工具返回预格式化的详细报告，请**完整、原样**输出所有内容。不要总结或重新格式化报告内容。',
        inputSchema: {
          type: 'object',
          properties: {
            job_name: {
              type: 'string',
              description:
                'Routine Load 作业名称（可选，不指定则检查所有作业）',
            },
            database_name: {
              type: 'string',
              description: '数据库名称（可选，用于过滤特定数据库的作业）',
            },
          },
          required: [],
        },
      },
    ];
  }
}

export { StarRocksIngestionExpert };
