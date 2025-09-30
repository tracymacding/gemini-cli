/**
 * StarRocks Import 专家模块
 * 负责：数据导入分析、Stream Load/Broker Load/Routine Load 诊断、导入性能优化等
 */

class StarRocksImportExpert {
  constructor() {
    this.name = 'import';
    this.version = '1.0.0';
    this.description = 'StarRocks Import 系统专家 - 负责数据导入问题诊断、性能分析、任务监控等';

    // Import专业知识规则库
    this.rules = {
      // Stream Load 规则
      stream_load: {
        max_file_size_mb: 10 * 1024, // 10GB
        recommended_batch_size_mb: 100, // 100MB
        timeout_seconds: 3600, // 1小时
        max_filter_ratio: 0.1 // 10% 错误率阈值
      },

      // Broker Load 规则
      broker_load: {
        max_parallelism: 5,
        load_timeout_seconds: 14400, // 4小时
        recommended_file_size_mb: 1024, // 1GB per file
        max_error_number: 1000
      },

      // Routine Load 规则
      routine_load: {
        max_lag_time_seconds: 300, // 5分钟延迟阈值
        recommended_task_consume_second: 3,
        max_batch_interval_seconds: 20,
        max_batch_rows: 200000,
        max_batch_size_mb: 100
      },

      // Insert 规则
      insert_load: {
        recommended_batch_size: 1000,
        max_batch_size: 10000,
        timeout_seconds: 300
      },

      // 性能阈值
      performance: {
        slow_load_threshold_seconds: 300,
        low_throughput_mb_per_second: 10,
        high_error_rate_percent: 5
      }
    };

    // Import 相关术语
    this.terminology = {
      stream_load: 'Stream Load: 通过HTTP PUT同步导入数据，适合小批量实时导入',
      broker_load: 'Broker Load: 通过Broker异步导入HDFS/S3数据，适合大批量历史数据',
      routine_load: 'Routine Load: 持续消费Kafka数据，适合实时流式导入',
      insert_load: 'Insert Load: 通过INSERT语句导入数据，适合少量数据插入',
      load_job: '导入作业，包含导入任务的所有信息和状态',
      error_hub: '错误数据中心，存储导入过程中的错误数据'
    };
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
      const recommendations = this.generateImportRecommendations(diagnosis, importData);

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
        optimization_suggestions: this.generateOptimizationSuggestions(importData)
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
      data.import_frequency_analysis = await this.analyzeImportFrequency(connection);
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
      insights: []
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
        console.warn('loads_history table not available, falling back to loads table');

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
    loads.forEach(load => {
      const key = `${load.DATABASE_NAME}.${load.TABLE_NAME}`;

      if (!tableMap.has(key)) {
        tableMap.set(key, {
          database: load.DATABASE_NAME,
          table: load.TABLE_NAME,
          loads: [],
          totalLoads: 0,
          successLoads: 0,
          failedLoads: 0
        });
      }

      const tableData = tableMap.get(key);
      tableData.loads.push({
        create_time: load.CREATE_TIME,
        state: load.STATE,
        timestamp: new Date(load.CREATE_TIME).getTime()
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
        const interval = (tableData.loads[i].timestamp - tableData.loads[i-1].timestamp) / 1000; // 秒
        intervals.push(interval);
      }

      // 计算频率统计
      const avgInterval = intervals.reduce((sum, interval) => sum + interval, 0) / intervals.length;
      const minInterval = Math.min(...intervals);
      const maxInterval = Math.max(...intervals);

      // 计算标准差
      const variance = intervals.reduce((sum, interval) => sum + Math.pow(interval - avgInterval, 2), 0) / intervals.length;
      const stdDev = Math.sqrt(variance);

      // 确定频率模式
      const frequencyPattern = this.determineFrequencyPattern(avgInterval, stdDev, intervals);

      const tableFrequency = {
        database: tableData.database,
        table: tableData.table,
        totalLoads: tableData.totalLoads,
        successLoads: tableData.successLoads,
        failedLoads: tableData.failedLoads,
        successRate: (tableData.successLoads / tableData.totalLoads * 100).toFixed(1),
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
          durationHours: ((tableData.loads[tableData.loads.length - 1].timestamp - tableData.loads[0].timestamp) / 3600000).toFixed(1)
        }
      };

      frequencyAnalysis.tables.push(tableFrequency);
    }

    // 按导入频率排序
    frequencyAnalysis.tables.sort((a, b) => parseFloat(b.loadsPerHour) - parseFloat(a.loadsPerHour));
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
      cvPercent: cvPercent.toFixed(1)
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
      level: level
    };
  }

  /**
   * 计算频率模式统计
   */
  calculateFrequencyPatterns(frequencyAnalysis) {
    const patterns = {
      'high-frequency': { count: 0, tables: [] },
      'frequent': { count: 0, tables: [] },
      'moderate': { count: 0, tables: [] },
      'hourly': { count: 0, tables: [] },
      'daily': { count: 0, tables: [] },
      'low-frequency': { count: 0, tables: [] }
    };

    const regularityStats = {
      'very-regular': 0,
      'regular': 0,
      'irregular': 0,
      'very-irregular': 0
    };

    frequencyAnalysis.tables.forEach(table => {
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
      total_tables: frequencyAnalysis.tables.length
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
        recommendation: '建议增加数据导入活动或检查更长时间范围的数据'
      });
      frequencyAnalysis.insights = insights;
      return;
    }

    // 1. 高频导入表分析
    const highFreqTables = tables.filter(t => t.frequencyPattern.frequency === 'high-frequency');
    if (highFreqTables.length > 0) {
      insights.push({
        type: 'high_frequency_import',
        message: `发现 ${highFreqTables.length} 个高频导入表（间隔<1分钟）`,
        tables: highFreqTables.slice(0, 5).map(t => ({
          table: `${t.database}.${t.table}`,
          interval_seconds: t.avgIntervalSeconds,
          loads_per_hour: t.loadsPerHour
        })),
        recommendation: '考虑合并小批次导入以提高效率，减少系统负载'
      });
    }

    // 2. 不规律导入模式分析
    const irregularTables = tables.filter(t => t.regularity.score < 40);
    if (irregularTables.length > 0) {
      insights.push({
        type: 'irregular_import_pattern',
        message: `发现 ${irregularTables.length} 个导入模式不规律的表`,
        tables: irregularTables.slice(0, 5).map(t => ({
          table: `${t.database}.${t.table}`,
          regularity_score: t.regularity.score,
          cv_percent: t.frequencyPattern.cvPercent
        })),
        recommendation: '建议优化导入调度，建立更规律的导入模式'
      });
    }

    // 3. 导入成功率分析
    const lowSuccessTables = tables.filter(t => parseFloat(t.successRate) < 95);
    if (lowSuccessTables.length > 0) {
      insights.push({
        type: 'low_success_rate',
        message: `发现 ${lowSuccessTables.length} 个表的导入成功率较低`,
        tables: lowSuccessTables.slice(0, 5).map(t => ({
          table: `${t.database}.${t.table}`,
          success_rate: t.successRate + '%',
          total_loads: t.totalLoads,
          failed_loads: t.failedLoads
        })),
        recommendation: '检查数据格式、网络连接和系统资源，提高导入成功率'
      });
    }

    // 4. 负载分布分析
    const totalLoadsPerHour = tables.reduce((sum, t) => sum + parseFloat(t.loadsPerHour), 0);
    if (totalLoadsPerHour > 1000) {
      insights.push({
        type: 'high_system_load',
        message: `系统总导入负载较高：每小时 ${totalLoadsPerHour.toFixed(0)} 次导入`,
        metrics: {
          total_loads_per_hour: Math.round(totalLoadsPerHour),
          active_tables: tables.length,
          avg_loads_per_table: (totalLoadsPerHour / tables.length).toFixed(1)
        },
        recommendation: '监控系统资源使用，考虑优化导入调度或扩容'
      });
    }

    // 5. 最活跃表分析
    const topActiveTables = tables.slice(0, 3);
    if (topActiveTables.length > 0) {
      insights.push({
        type: 'most_active_tables',
        message: '最活跃的导入表',
        tables: topActiveTables.map(t => ({
          table: `${t.database}.${t.table}`,
          loads_per_hour: t.loadsPerHour,
          frequency_pattern: t.frequencyPattern.frequency,
          regularity: t.regularity.level
        })),
        recommendation: '重点监控这些活跃表的性能和资源使用情况'
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
      summary: this.generateImportSummary(criticals, warnings, issues)
    };
  }

  /**
   * 诊断导入频率模式
   */
  diagnoseImportFrequency(data, warnings, insights) {
    const frequencyData = data.import_frequency_analysis;

    if (!frequencyData || !frequencyData.tables || frequencyData.tables.length === 0) {
      insights.push({
        type: 'import_frequency_no_data',
        message: '导入频率分析：未发现足够的历史数据',
        recommendation: '建议检查数据源或扩大分析时间范围'
      });
      return;
    }

    // 1. 添加频率分析的洞察到总洞察中
    if (frequencyData.insights && frequencyData.insights.length > 0) {
      frequencyData.insights.forEach(insight => {
        insights.push({
          type: `frequency_${insight.type}`,
          message: `导入频率分析：${insight.message}`,
          details: insight.tables || insight.metrics,
          recommendation: insight.recommendation
        });
      });
    }

    // 2. 检查高频导入警告
    const highFreqTables = frequencyData.tables.filter(t =>
      t.frequencyPattern.frequency === 'high-frequency'
    );

    if (highFreqTables.length > 3) {
      warnings.push({
        type: 'excessive_high_frequency_imports',
        severity: 'WARNING',
        message: `发现过多高频导入表 (${highFreqTables.length} 个)，可能影响系统性能`,
        affected_tables: highFreqTables.slice(0, 5).map(t => ({
          table: `${t.database}.${t.table}`,
          loads_per_hour: t.loadsPerHour,
          avg_interval_seconds: t.avgIntervalSeconds
        })),
        impact: '过多的高频导入可能导致系统负载过高和资源竞争',
        urgency: 'WITHIN_DAYS'
      });
    }

    // 3. 检查导入模式不规律的警告
    const irregularTables = frequencyData.tables.filter(t =>
      t.regularity.score < 40
    );

    if (irregularTables.length > frequencyData.tables.length * 0.5) {
      warnings.push({
        type: 'irregular_import_patterns',
        severity: 'WARNING',
        message: `超过半数表的导入模式不规律 (${irregularTables.length}/${frequencyData.tables.length})`,
        irregular_tables: irregularTables.slice(0, 5).map(t => ({
          table: `${t.database}.${t.table}`,
          regularity_score: t.regularity.score,
          cv_percent: t.frequencyPattern.cvPercent
        })),
        impact: '不规律的导入模式可能导致资源使用不均和性能波动',
        urgency: 'WITHIN_WEEKS'
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
          data_source: frequencyData.source_table
        },
        recommendations: this.generateFrequencyRecommendations(frequencyData)
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
    if (patterns.regularity_distribution['irregular'] + patterns.regularity_distribution['very-irregular'] > patterns.total_tables * 0.3) {
      recommendations.push('建立规律的导入调度机制，提高资源利用效率');
    }

    // 负载均衡建议
    if (patterns.frequency_distribution['frequent'].count > patterns.total_tables * 0.5) {
      recommendations.push('监控系统负载，考虑在低峰期调度部分导入任务');
    }

    return recommendations.length > 0 ? recommendations : ['当前导入频率模式合理，保持现有策略'];
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
            failure_rate: failureRate.toFixed(1)
          },
          impact: '大量导入失败可能导致数据丢失和业务中断',
          urgency: 'IMMEDIATE'
        });
      } else if (failureRate > 10) {
        warnings.push({
          type: 'moderate_load_failure_rate',
          severity: 'WARNING',
          message: `导入失败率较高: ${failureRate.toFixed(1)}%`,
          metrics: {
            total_jobs: stats.total_jobs,
            failed_jobs: stats.failed_jobs,
            failure_rate: failureRate.toFixed(1)
          },
          impact: '需要关注导入质量，建议检查数据格式和配置',
          urgency: 'WITHIN_HOURS'
        });
      }
    }

    // 检查最近失败的作业
    const recentFailures = recentLoads.filter(load => load.STATE === 'CANCELLED');
    if (recentFailures.length > 5) {
      warnings.push({
        type: 'frequent_load_failures',
        severity: 'WARNING',
        message: `最近24小时内有 ${recentFailures.length} 个导入作业失败`,
        affected_count: recentFailures.length,
        impact: '频繁的导入失败可能指示系统性问题',
        urgency: 'WITHIN_HOURS'
      });
    }
  }

  /**
   * 诊断导入性能
   */
  diagnoseLoadPerformance(data, warnings, insights) {
    const stats = data.stream_load_stats;

    if (stats.avg_load_time_seconds > this.rules.performance.slow_load_threshold_seconds) {
      warnings.push({
        type: 'slow_load_performance',
        severity: 'WARNING',
        message: `平均导入时间过长: ${stats.avg_load_time_seconds.toFixed(1)} 秒`,
        metrics: {
          avg_load_time: stats.avg_load_time_seconds.toFixed(1),
          threshold: this.rules.performance.slow_load_threshold_seconds
        },
        impact: '导入性能低下可能影响数据实时性',
        urgency: 'WITHIN_DAYS'
      });
    }

    // 分析表级别的导入模式
    const tableStats = data.table_load_stats || [];
    const highVolumeTable = tableStats.find(table => table.load_count > 100);

    if (highVolumeTable) {
      insights.push({
        type: 'high_volume_import_analysis',
        message: '发现高频导入表',
        analysis: {
          table: `${highVolumeTable.database_name}.${highVolumeTable.table_name}`,
          load_count: highVolumeTable.load_count,
          success_rate: ((highVolumeTable.success_count / highVolumeTable.load_count) * 100).toFixed(1)
        },
        recommendations: [
          '考虑优化导入频率，合并小批次导入',
          '监控表的导入性能和资源使用',
          '评估是否需要调整表结构或分区策略'
        ]
      });
    }
  }

  /**
   * 诊断Routine Load健康状态
   */
  diagnoseRoutineLoadHealth(data, warnings, criticals) {
    const routineLoads = data.routine_loads || [];

    routineLoads.forEach(routine => {
      if (routine.STATE === 'PAUSED') {
        warnings.push({
          type: 'routine_load_paused',
          severity: 'WARNING',
          message: `Routine Load作业 "${routine.NAME}" 处于暂停状态`,
          routine_name: routine.NAME,
          table_name: routine.TABLE_NAME,
          pause_time: routine.PAUSE_TIME,
          impact: '流式导入中断可能导致数据延迟',
          urgency: 'WITHIN_HOURS'
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
          urgency: 'IMMEDIATE'
        });
      }
    });
  }

  /**
   * 诊断导入作业堆积
   */
  diagnoseLoadQueue(data, warnings, criticals) {
    const runningLoads = data.running_loads || [];
    const pendingLoads = runningLoads.filter(load => load.STATE === 'PENDING');

    if (pendingLoads.length > 10) {
      criticals.push({
        type: 'load_queue_backlog',
        severity: 'CRITICAL',
        message: `导入队列积压严重，有 ${pendingLoads.length} 个作业等待执行`,
        pending_count: pendingLoads.length,
        impact: '导入队列积压可能导致数据延迟和超时',
        urgency: 'IMMEDIATE'
      });
    } else if (pendingLoads.length > 5) {
      warnings.push({
        type: 'load_queue_buildup',
        severity: 'WARNING',
        message: `导入队列有 ${pendingLoads.length} 个作业等待执行`,
        pending_count: pendingLoads.length,
        impact: '需要监控导入队列，避免进一步积压',
        urgency: 'WITHIN_HOURS'
      });
    }

    // 检查长时间运行的作业
    const now = new Date();
    const longRunningLoads = runningLoads.filter(load => {
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
        long_running_jobs: longRunningLoads.map(load => ({
          job_id: load.JOB_ID,
          label: load.LABEL,
          state: load.STATE,
          type: load.TYPE,
          running_hours: Math.round((now - new Date(load.CREATE_TIME)) / (1000 * 60 * 60) * 10) / 10
        })),
        impact: '长时间运行的作业可能阻塞导入队列',
        urgency: 'WITHIN_HOURS'
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
    failedLoads.forEach(load => {
      if (load.ERROR_MSG) {
        // 简化错误信息提取关键词
        let errorType = 'unknown';
        const errorMsg = load.ERROR_MSG.toLowerCase();

        if (errorMsg.includes('timeout')) {
          errorType = 'timeout';
        } else if (errorMsg.includes('format') || errorMsg.includes('parse')) {
          errorType = 'format_error';
        } else if (errorMsg.includes('permission') || errorMsg.includes('access')) {
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
          urgency: 'WITHIN_DAYS'
        });
      }
    });
  }

  /**
   * 生成Import专业建议
   */
  generateImportRecommendations(diagnosis, data) {
    const recommendations = [];

    [...diagnosis.criticals, ...diagnosis.warnings].forEach(issue => {
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
                command: 'SELECT ERROR_MSG, COUNT(*) FROM information_schema.loads WHERE STATE = "CANCELLED" AND CREATE_TIME >= DATE_SUB(NOW(), INTERVAL 24 HOUR) GROUP BY ERROR_MSG ORDER BY COUNT(*) DESC;',
                purpose: '识别最常见的失败原因'
              },
              {
                action: '检查数据格式',
                steps: [
                  '验证数据文件格式是否符合表结构',
                  '检查字段分隔符和编码格式',
                  '确认数据类型匹配'
                ]
              },
              {
                action: '优化导入参数',
                recommendations: [
                  '调整max_filter_ratio参数',
                  '增加timeout时间',
                  '优化批次大小'
                ]
              }
            ]
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
                purpose: '获取详细的作业状态信息'
              },
              {
                action: '恢复作业',
                command: `RESUME ROUTINE LOAD FOR ${issue.routine_name};`,
                risk_level: 'LOW',
                note: '确保Kafka连接正常'
              },
              {
                action: '监控恢复效果',
                monitoring_metrics: [
                  '数据消费速度',
                  '延迟时间',
                  '错误率'
                ]
              }
            ]
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
                steps: [
                  '监控CPU使用率',
                  '检查内存使用情况',
                  '评估磁盘IO负载'
                ]
              },
              {
                action: '调整并发配置',
                recommendations: [
                  '增加BE节点导入并发度',
                  '优化FE资源分配',
                  '调整导入队列大小'
                ]
              }
            ]
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
      monitoring_enhancement: []
    };

    // 性能优化建议
    suggestions.performance_optimization.push({
      area: 'batch_size_optimization',
      suggestion: '优化导入批次大小以提高吞吐量',
      implementation: '根据数据量和系统资源调整Stream Load批次大小'
    });

    suggestions.performance_optimization.push({
      area: 'parallel_loading',
      suggestion: '利用并行导入提高数据导入速度',
      implementation: '合理配置导入并发度，避免资源竞争'
    });

    // 可靠性优化建议
    suggestions.reliability_optimization.push({
      area: 'error_handling',
      suggestion: '建立完善的错误处理和重试机制',
      implementation: '设置合理的错误容忍度和自动重试策略'
    });

    suggestions.reliability_optimization.push({
      area: 'data_validation',
      suggestion: '加强数据质量验证',
      implementation: '在导入前进行数据格式和完整性检查'
    });

    // 监控增强建议
    suggestions.monitoring_enhancement.push({
      area: 'import_monitoring',
      suggestion: '建立全面的导入监控体系',
      key_metrics: [
        '导入成功率和失败率',
        '导入性能和吞吐量',
        '队列积压情况',
        'Routine Load延迟时间'
      ]
    });

    return suggestions;
  }

  /**
   * 计算Import健康分数
   */
  calculateImportHealthScore(diagnosis) {
    let score = 100;

    // 基于不同问题类型的扣分策略
    diagnosis.criticals.forEach(issue => {
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

    diagnosis.warnings.forEach(issue => {
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
      status: diagnosis.criticals.length > 0 ? 'CRITICAL' :
              diagnosis.warnings.length > 0 ? 'WARNING' : 'HEALTHY'
    };
  }

  /**
   * 生成Import诊断摘要
   */
  generateImportSummary(criticals, warnings, issues) {
    if (criticals.length > 0) {
      const failureIssues = criticals.filter(c => c.type.includes('failure')).length;
      const queueIssues = criticals.filter(c => c.type.includes('queue')).length;

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
  async analyzeTableImportFrequency(connection, dbName, tableName, includeDetails = true) {
    console.error(`🔍 开始分析表 ${dbName}.${tableName} 的导入频率...`);
    const startTime = Date.now();

    try {
      // 1. 基础统计查询
      const [basicStats] = await connection.query(`
        SELECT
          COUNT(*) as total_loads,
          MIN(CREATE_TIME) as first_load,
          MAX(CREATE_TIME) as last_load,
          TIMESTAMPDIFF(SECOND, MIN(CREATE_TIME), MAX(CREATE_TIME)) as time_span_seconds,
          AVG(SCAN_BYTES) as avg_bytes_per_load,
          SUM(SCAN_BYTES) as total_bytes_processed,
          SUM(CASE WHEN STATE = 'FINISHED' THEN 1 ELSE 0 END) as success_count,
          SUM(CASE WHEN STATE != 'FINISHED' THEN 1 ELSE 0 END) as failed_count
        FROM _statistics_.loads_history
        WHERE DB_NAME = ? AND TABLE_NAME = ?
      `, [dbName, tableName]);

      if (!basicStats || basicStats.length === 0 || basicStats[0].total_loads === 0) {
        return {
          table: `${dbName}.${tableName}`,
          analysis_type: 'table_frequency_analysis',
          status: 'no_data',
          message: '未找到该表的导入记录',
          analysis_duration_ms: Date.now() - startTime
        };
      }

      const stats = basicStats[0];
      const timeSpanSeconds = Math.max(stats.time_span_seconds || 1, 1);

      // 2. 计算频率指标
      const frequencyMetrics = this.calculateFrequencyMetrics(stats, timeSpanSeconds);

      // 3. 获取时间分布数据
      const timeDistribution = await this.getTimeDistribution(connection, dbName, tableName);

      // 4. 获取数据量统计
      const sizeStats = await this.getSizeStatistics(connection, dbName, tableName);

      // 5. 分析并发模式
      const concurrencyAnalysis = this.analyzeConcurrencyPattern(timeDistribution, stats.total_loads);

      // 6. 识别导入模式
      const patternAnalysis = this.identifyImportPattern(stats, frequencyMetrics, sizeStats, timeDistribution);

      // 7. 性能评估
      const performanceAnalysis = this.evaluateImportPerformance(stats, frequencyMetrics, timeSpanSeconds);

      // 8. 生成洞察和建议
      const insights = this.generateTableFrequencyInsights(stats, frequencyMetrics, patternAnalysis, performanceAnalysis);

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
          success_rate: ((stats.success_count / stats.total_loads) * 100).toFixed(1),
          first_load: stats.first_load,
          last_load: stats.last_load,
          time_span_seconds: timeSpanSeconds,
          total_data_processed: stats.total_bytes_processed,
          avg_file_size: stats.avg_bytes_per_load
        },

        // 频率指标
        frequency_metrics: frequencyMetrics,

        // 时间分布
        time_distribution: includeDetails ? timeDistribution : timeDistribution.slice(0, 10),

        // 数据量统计
        size_statistics: sizeStats,

        // 并发分析
        concurrency_analysis: concurrencyAnalysis,

        // 模式识别
        pattern_analysis: patternAnalysis,

        // 性能评估
        performance_analysis: performanceAnalysis,

        // 洞察和建议
        insights: insights
      };

      console.error(`✅ 表 ${dbName}.${tableName} 频率分析完成，耗时 ${result.analysis_duration_ms}ms`);
      return result;

    } catch (error) {
      console.error(`❌ 表频率分析失败: ${error.message}`);
      return {
        table: `${dbName}.${tableName}`,
        analysis_type: 'table_frequency_analysis',
        status: 'error',
        error: error.message,
        analysis_duration_ms: Date.now() - startTime
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
      frequency_description: this.getFrequencyDescription(frequencyCategory)
    };
  }

  /**
   * 获取时间分布数据
   */
  async getTimeDistribution(connection, dbName, tableName) {
    try {
      const [timeDistribution] = await connection.query(`
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
      `, [dbName, tableName, dbName, tableName]);

      return timeDistribution.map(item => ({
        finish_time: item.LOAD_FINISH_TIME,
        job_count: item.job_count,
        percentage: parseFloat(item.percentage)
      }));
    } catch (error) {
      console.warn(`获取时间分布失败: ${error.message}`);
      return [];
    }
  }

  /**
   * 获取数据量统计
   */
  async getSizeStatistics(connection, dbName, tableName) {
    try {
      const [sizeStats] = await connection.query(`
        SELECT
          MIN(SCAN_BYTES) as min_size,
          MAX(SCAN_BYTES) as max_size,
          AVG(SCAN_BYTES) as avg_size,
          STDDEV(SCAN_BYTES) as size_stddev
        FROM information_schema.loads
        WHERE DB_NAME = ? AND TABLE_NAME = ? AND SCAN_BYTES IS NOT NULL
      `, [dbName, tableName]);

      if (!sizeStats || sizeStats.length === 0) {
        return null;
      }

      const stats = sizeStats[0];
      const variationCoefficient = stats.size_stddev && stats.avg_size ?
        (stats.size_stddev / stats.avg_size * 100) : 0;

      return {
        min_size: stats.min_size,
        max_size: stats.max_size,
        avg_size: stats.avg_size,
        size_stddev: stats.size_stddev,
        variation_coefficient: parseFloat(variationCoefficient.toFixed(1)),
        consistency_level: this.getSizeConsistencyLevel(variationCoefficient)
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
      current.job_count > max.job_count ? current : max
    );

    const avgJobsPerSecond = totalLoads / timeDistribution.length;

    // 计算标准差
    const variance = timeDistribution.reduce((sum, time) => {
      return sum + Math.pow(time.job_count - avgJobsPerSecond, 2);
    }, 0) / timeDistribution.length;
    const stdDev = Math.sqrt(variance);

    return {
      peak_time: peakTime.finish_time,
      peak_concurrent_jobs: peakTime.job_count,
      time_span_seconds: timeDistribution.length,
      avg_jobs_per_second: parseFloat(avgJobsPerSecond.toFixed(1)),
      completion_std_dev: parseFloat(stdDev.toFixed(2)),
      concurrency_level: this.getConcurrencyLevel(peakTime.job_count, avgJobsPerSecond)
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
          '适用于大规模数据迁移或基准测试'
        ]
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
          '需要关注系统资源消耗'
        ]
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
          '负载均衡良好'
        ]
      });
    }

    // 检查基准测试模式
    if (stats.total_loads === 195 && stats.success_count === stats.total_loads) {
      patterns.push({
        type: 'benchmark_testing',
        confidence: 0.95,
        description: 'SSB基准测试模式',
        characteristics: [
          '195个文件 (SSB 100GB标准分片)',
          '100%成功率',
          '性能测试或基准评估场景'
        ]
      });
    }

    return {
      identified_patterns: patterns,
      primary_pattern: patterns.length > 0 ? patterns[0] : null
    };
  }

  /**
   * 评估导入性能
   */
  evaluateImportPerformance(stats, frequencyMetrics, timeSpanSeconds) {
    const totalDataGB = stats.total_bytes_processed / (1024 * 1024 * 1024);
    const throughputMBps = (stats.total_bytes_processed / (1024 * 1024)) / timeSpanSeconds;
    const fileProcessingRate = stats.total_loads / timeSpanSeconds;

    let performanceLevel = 'poor';
    if (throughputMBps > 100) performanceLevel = 'excellent';
    else if (throughputMBps > 50) performanceLevel = 'good';
    else if (throughputMBps > 20) performanceLevel = 'fair';

    return {
      total_data_gb: parseFloat(totalDataGB.toFixed(2)),
      throughput_mbps: parseFloat(throughputMBps.toFixed(1)),
      file_processing_rate: parseFloat(fileProcessingRate.toFixed(1)),
      performance_level: performanceLevel,
      success_rate: parseFloat(((stats.success_count / stats.total_loads) * 100).toFixed(1))
    };
  }

  /**
   * 生成表频率分析洞察
   */
  generateTableFrequencyInsights(stats, frequencyMetrics, patternAnalysis, performanceAnalysis) {
    const insights = [];

    // 频率相关洞察
    if (frequencyMetrics.frequency_level === 'extreme') {
      insights.push({
        type: 'extreme_frequency',
        priority: 'high',
        message: `检测到极高频导入 (${frequencyMetrics.loads_per_second}次/秒)`,
        implications: [
          '可能是批量数据迁移或性能测试',
          '需要确保系统资源充足',
          '关注I/O和网络性能'
        ],
        recommendations: [
          '监控系统资源使用情况',
          '评估是否需要调整并发度',
          '考虑优化导入批次大小'
        ]
      });
    }

    // 性能相关洞察
    if (performanceAnalysis.performance_level === 'excellent') {
      insights.push({
        type: 'performance_excellent',
        priority: 'info',
        message: `导入性能优秀 (${performanceAnalysis.throughput_mbps} MB/s)`,
        implications: [
          '系统具备强大的数据处理能力',
          '当前配置和优化效果良好',
          '可以作为性能基准参考'
        ],
        recommendations: [
          '保持当前的导入策略',
          '可以考虑处理更大规模的数据',
          '建立性能监控基线'
        ]
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
          '系统配置合理'
        ],
        recommendations: [
          '继续保持当前的数据处理流程',
          '可以作为最佳实践案例',
          '建立成功率监控告警'
        ]
      });
    } else if (performanceAnalysis.success_rate < 95) {
      insights.push({
        type: 'reliability_concern',
        priority: 'medium',
        message: `导入成功率较低 (${performanceAnalysis.success_rate}%)`,
        implications: [
          '存在数据质量或系统问题',
          '可能影响数据完整性',
          '需要分析失败原因'
        ],
        recommendations: [
          '检查失败导入的错误日志',
          '改进数据验证和清洗流程',
          '优化错误处理和重试机制'
        ]
      });
    }

    // 模式相关洞察
    if (patternAnalysis.primary_pattern) {
      const pattern = patternAnalysis.primary_pattern;
      insights.push({
        type: 'pattern_identified',
        priority: 'info',
        message: `识别出导入模式: ${pattern.description}`,
        implications: pattern.characteristics,
        recommendations: this.getPatternRecommendations(pattern.type)
      });
    }

    return insights;
  }

  /**
   * 获取频率描述
   */
  getFrequencyDescription(category) {
    const descriptions = {
      'extreme_frequency': '极高频 (每秒多次)',
      'very_high_frequency': '超高频 (每分钟60+次)',
      'high_frequency': '高频 (每分钟4+次)',
      'frequent': '频繁 (每分钟1-4次)',
      'moderate': '中等 (每小时1+次)',
      'low_frequency': '低频 (每小时<1次)'
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
      'bulk_parallel_import': [
        '监控系统资源使用峰值',
        '考虑在低峰时段执行大批量导入',
        '优化并行度避免资源争抢'
      ],
      'streaming_import': [
        '建立实时监控和告警',
        '优化批次大小平衡延迟和吞吐量',
        '考虑使用Routine Load提高稳定性'
      ],
      'uniform_sharding': [
        '继续保持均匀分片策略',
        '可以考虑适当增加并行度',
        '监控单个分片的处理时间'
      ],
      'benchmark_testing': [
        '记录性能基准指标',
        '可以作为系统性能评估标准',
        '定期重复测试验证系统稳定性'
      ]
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
    report += `   数据吞吐量: ${perf.throughput_mbps} MB/s\n`;
    report += `   文件处理速度: ${perf.file_processing_rate} 文件/秒\n`;
    report += `   性能等级: ${perf.performance_level}\n\n`;

    // 模式识别
    if (analysis.pattern_analysis.primary_pattern) {
      const pattern = analysis.pattern_analysis.primary_pattern;
      report += '🎯 模式识别:\n';
      report += `   识别模式: ${pattern.description}\n`;
      report += `   置信度: ${(pattern.confidence * 100).toFixed(0)}%\n`;
      pattern.characteristics.forEach(char => {
        report += `   特征: ${char}\n`;
      });
      report += '\n';
    }

    // 关键洞察
    if (analysis.insights.length > 0) {
      report += '💡 关键洞察:\n';
      analysis.insights.slice(0, 3).forEach((insight, index) => {
        const priority = insight.priority === 'high' ? '🔥' :
                        insight.priority === 'medium' ? '⚠️' : 'ℹ️';
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
}

export { StarRocksImportExpert };