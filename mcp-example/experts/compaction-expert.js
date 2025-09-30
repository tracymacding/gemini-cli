/**
 * StarRocks Compaction 专家模块
 * 负责：Compaction Score管理、版本控制、数据压缩优化等
 */

class StarRocksCompactionExpert {
  constructor() {
    this.name = 'compaction';
    this.version = '1.0.0';
    this.description = 'StarRocks Compaction 系统专家 - 负责数据压缩、版本管理、Compaction调度优化';

    // Compaction专业知识规则库
    this.rules = {
      // Compaction Score 规则
      compaction_score: {
        normal_threshold: 10,
        warning_threshold: 100,
        critical_threshold: 500,
        emergency_threshold: 1000
      },

      // 线程配置规则
      thread_config: {
        min_threads_per_core: 0.25,  // 最少线程数/CPU核心
        max_threads_per_core: 0.5,   // 最多线程数/CPU核心
        absolute_min_threads: 4,
        absolute_max_threads: 64
      },

      // 任务执行规则
      task_execution: {
        max_running_tasks_per_node: 10,
        max_queue_length: 50,
        task_timeout_hours: 4,
        slow_task_threshold_hours: 2
      },

      // 版本管理规则
      version_management: {
        max_version_count: 1000,
        version_cleanup_threshold: 500,
        old_version_days: 7
      }
    };

    // Compaction 相关术语
    this.terminology = {
      compaction_score: 'Compaction Score (CS) 表示数据文件的碎片化程度，分数越高表示碎片越严重，需要压缩',
      base_compaction: '基础压缩，将小文件合并成大文件，减少文件数量',
      cumulative_compaction: '累积压缩，合并增量文件到基础文件',
      version: '数据版本，每次数据变更都会产生新版本',
      rowset: '数据文件集合，包含一批数据行'
    };
  }

  /**
   * Compaction 系统综合诊断
   */
  async diagnose(connection, includeDetails = true) {
    try {
      const startTime = new Date();

      // 1. 收集Compaction相关数据
      const compactionData = await this.collectCompactionData(connection);

      // 2. 执行专业诊断分析
      const diagnosis = this.performCompactionDiagnosis(compactionData);

      // 3. 生成专业建议
      const recommendations = this.generateCompactionRecommendations(diagnosis, compactionData);

      // 4. 计算Compaction健康分数
      const healthScore = this.calculateCompactionHealthScore(diagnosis);

      const endTime = new Date();
      const analysisTime = endTime - startTime;

      return {
        expert: this.name,
        version: this.version,
        timestamp: new Date().toISOString(),
        analysis_duration_ms: analysisTime,
        compaction_health: healthScore,
        diagnosis_results: diagnosis,
        professional_recommendations: recommendations,
        raw_data: includeDetails ? compactionData : null,
        optimization_suggestions: this.generateOptimizationSuggestions(compactionData)
      };
    } catch (error) {
      throw new Error(`Compaction专家诊断失败: ${error.message}`);
    }
  }

  /**
   * 收集Compaction相关数据
   */
  async collectCompactionData(connection) {
    const data = {};

    // 1. 高Compaction Score分区
    try {
      const [highCSPartitions] = await connection.query(`
        SELECT DB_NAME, TABLE_NAME, PARTITION_NAME, MAX_CS, AVG_CS, P50_CS, ROW_COUNT
        FROM information_schema.partitions_meta
        WHERE MAX_CS > ${this.rules.compaction_score.warning_threshold}
        ORDER BY MAX_CS DESC
        LIMIT 100;
      `);
      data.high_cs_partitions = highCSPartitions;
    } catch (error) {
      console.warn('Failed to collect high CS partitions:', error.message);
      data.high_cs_partitions = [];
    }

    // 2. Compaction线程配置
    try {
      const [threadConfig] = await connection.query(`
        SELECT * FROM information_schema.be_configs WHERE name = 'compact_threads';
      `);
      data.thread_config = threadConfig;
    } catch (error) {
      console.warn('Failed to collect thread config:', error.message);
      data.thread_config = [];
    }

    // 3. 正在运行的Compaction任务
    try {
      const [runningTasks] = await connection.query(`
        SELECT BE_ID, TXN_ID, TABLET_ID, VERSION, START_TIME, PROGRESS, STATUS, RUNS
        FROM information_schema.be_cloud_native_compactions
        WHERE START_TIME IS NOT NULL AND FINISH_TIME IS NULL
        ORDER BY START_TIME;
      `);
      data.running_tasks = runningTasks;
    } catch (error) {
      console.warn('Failed to collect running tasks:', error.message);
      data.running_tasks = [];
    }

    // 4. 最近完成的Compaction任务
    try {
      const [recentTasks] = await connection.query(`
        SELECT BE_ID, TXN_ID, TABLET_ID, START_TIME, FINISH_TIME, PROGRESS, STATUS
        FROM information_schema.be_cloud_native_compactions
        WHERE FINISH_TIME IS NOT NULL
        ORDER BY FINISH_TIME DESC
        LIMIT 50;
      `);
      data.recent_tasks = recentTasks;
    } catch (error) {
      console.warn('Failed to collect recent tasks:', error.message);
      data.recent_tasks = [];
    }

    // 5. BE节点信息（用于线程配置分析）
    const [backends] = await connection.query('SHOW BACKENDS;');
    data.backends = backends;

    // 6. FE配置相关
    try {
      const [feConfig] = await connection.query('SHOW FRONTEND CONFIG;');
      data.fe_config = feConfig.filter(config =>
        config.Key.includes('compaction') || config.Key.includes('lake_compaction')
      );
    } catch (error) {
      data.fe_config = [];
    }

    return data;
  }

  /**
   * 执行Compaction专业诊断
   */
  performCompactionDiagnosis(data) {
    const issues = [];
    const warnings = [];
    const criticals = [];
    const insights = [];

    // 1. Compaction Score诊断
    this.diagnoseCompactionScore(data.high_cs_partitions, issues, warnings, criticals);

    // 2. 线程配置诊断
    this.diagnoseThreadConfiguration(data.thread_config, data.backends, warnings, insights);

    // 3. 任务执行效率诊断
    this.diagnoseTaskExecution(data.running_tasks, data.recent_tasks, warnings, criticals);

    // 4. 系统级Compaction压力诊断
    this.diagnoseCompactionPressure(data, warnings, criticals, insights);

    return {
      total_issues: issues.length + warnings.length + criticals.length,
      criticals: criticals,
      warnings: warnings,
      issues: issues,
      insights: insights,
      summary: this.generateCompactionSummary(criticals, warnings, issues)
    };
  }

  /**
   * Compaction Score 诊断
   */
  diagnoseCompactionScore(highCSPartitions, issues, warnings, criticals) {
    const emergencyPartitions = highCSPartitions.filter(p => p.MAX_CS >= this.rules.compaction_score.emergency_threshold);
    const criticalPartitions = highCSPartitions.filter(p => p.MAX_CS >= this.rules.compaction_score.critical_threshold && p.MAX_CS < this.rules.compaction_score.emergency_threshold);
    const warningPartitions = highCSPartitions.filter(p => p.MAX_CS >= this.rules.compaction_score.warning_threshold && p.MAX_CS < this.rules.compaction_score.critical_threshold);

    if (emergencyPartitions.length > 0) {
      criticals.push({
        type: 'emergency_compaction_score',
        severity: 'CRITICAL',
        message: `发现 ${emergencyPartitions.length} 个分区 CS >= 1000，严重影响性能`,
        affected_partitions: emergencyPartitions.slice(0, 10).map(p => ({
          partition: `${p.DB_NAME}.${p.TABLE_NAME}.${p.PARTITION_NAME}`,
          max_cs: p.MAX_CS,
          avg_cs: p.AVG_CS,
          row_count: p.ROW_COUNT
        })),
        impact: '严重影响查询性能，可能导致查询超时',
        urgency: 'IMMEDIATE',
        business_impact: '用户查询体验严重下降，可能影响业务正常运行'
      });
    }

    if (criticalPartitions.length > 0) {
      criticals.push({
        type: 'critical_compaction_score',
        severity: 'CRITICAL',
        message: `发现 ${criticalPartitions.length} 个分区 CS >= 500，需要立即处理`,
        affected_partitions: criticalPartitions.slice(0, 10).map(p => ({
          partition: `${p.DB_NAME}.${p.TABLE_NAME}.${p.PARTITION_NAME}`,
          max_cs: p.MAX_CS,
          avg_cs: p.AVG_CS
        })),
        impact: '显著影响查询性能和存储效率',
        urgency: 'WITHIN_HOURS'
      });
    }

    if (warningPartitions.length > 0) {
      warnings.push({
        type: 'warning_compaction_score',
        severity: 'WARNING',
        message: `发现 ${warningPartitions.length} 个分区 CS >= 100，建议优化`,
        affected_count: warningPartitions.length,
        impact: '可能影响查询性能，建议制定Compaction计划',
        urgency: 'WITHIN_DAYS'
      });
    }
  }

  /**
   * 线程配置诊断
   */
  diagnoseThreadConfiguration(threadConfig, backends, warnings, insights) {
    if (!threadConfig.length || !backends.length) return;

    const threadAnalysis = threadConfig.map(config => {
      const beInfo = backends.find(be => be.BackendId === config.BE_ID.toString());
      const threads = parseInt(config.VALUE);
      const cpuCores = beInfo ? parseInt(beInfo.CpuCores) : 1;

      return {
        node_id: config.BE_ID,
        ip: beInfo?.IP || 'Unknown',
        threads: threads,
        cpu_cores: cpuCores,
        threads_per_core: threads / cpuCores,
        recommended_min: Math.max(this.rules.thread_config.absolute_min_threads,
                                 Math.ceil(cpuCores * this.rules.thread_config.min_threads_per_core)),
        recommended_max: Math.min(this.rules.thread_config.absolute_max_threads,
                                 Math.ceil(cpuCores * this.rules.thread_config.max_threads_per_core))
      };
    });

    // 检查线程配置是否合理
    threadAnalysis.forEach(analysis => {
      if (analysis.threads < analysis.recommended_min) {
        warnings.push({
          type: 'low_compaction_threads',
          node: analysis.ip,
          severity: 'WARNING',
          message: `节点 ${analysis.ip} Compaction线程数过低 (${analysis.threads}/${analysis.cpu_cores}核)`,
          metrics: {
            current_threads: analysis.threads,
            cpu_cores: analysis.cpu_cores,
            recommended_min: analysis.recommended_min,
            threads_per_core: analysis.threads_per_core.toFixed(2)
          },
          impact: 'Compaction处理能力不足，可能导致CS积累',
          urgency: 'WITHIN_DAYS'
        });
      } else if (analysis.threads > analysis.recommended_max) {
        warnings.push({
          type: 'high_compaction_threads',
          node: analysis.ip,
          severity: 'WARNING',
          message: `节点 ${analysis.ip} Compaction线程数过高 (${analysis.threads}/${analysis.cpu_cores}核)`,
          metrics: {
            current_threads: analysis.threads,
            cpu_cores: analysis.cpu_cores,
            recommended_max: analysis.recommended_max,
            threads_per_core: analysis.threads_per_core.toFixed(2)
          },
          impact: '可能过度消耗CPU资源，影响其他操作',
          urgency: 'WITHIN_WEEKS'
        });
      }
    });

    // 生成线程配置优化洞察
    const totalThreads = threadAnalysis.reduce((sum, a) => sum + a.threads, 0);
    const totalCores = threadAnalysis.reduce((sum, a) => sum + a.cpu_cores, 0);
    const avgThreadsPerCore = totalThreads / totalCores;

    insights.push({
      type: 'thread_configuration_analysis',
      message: `集群Compaction线程配置分析`,
      metrics: {
        total_nodes: threadAnalysis.length,
        total_threads: totalThreads,
        total_cpu_cores: totalCores,
        avg_threads_per_core: avgThreadsPerCore.toFixed(2)
      },
      recommendations: [
        `建议线程/核心比例保持在 ${this.rules.thread_config.min_threads_per_core}-${this.rules.thread_config.max_threads_per_core} 之间`,
        '根据实际Compaction压力动态调整线程数',
        '监控线程使用率和CPU负载的平衡'
      ]
    });
  }

  /**
   * 任务执行效率诊断
   */
  diagnoseTaskExecution(runningTasks, recentTasks, warnings, criticals) {
    // 检查长时间运行的任务
    const now = new Date();
    const longRunningTasks = runningTasks.filter(task => {
      if (!task.START_TIME) return false;
      const startTime = new Date(task.START_TIME);
      const runningHours = (now - startTime) / (1000 * 60 * 60);
      return runningHours > this.rules.task_execution.slow_task_threshold_hours;
    });

    if (longRunningTasks.length > 0) {
      warnings.push({
        type: 'long_running_compaction_tasks',
        severity: 'WARNING',
        message: `发现 ${longRunningTasks.length} 个长时间运行的Compaction任务`,
        tasks: longRunningTasks.map(task => ({
          be_id: task.BE_ID,
          tablet_id: task.TABLET_ID,
          start_time: task.START_TIME,
          progress: task.PROGRESS,
          duration_hours: Math.round((now - new Date(task.START_TIME)) / (1000 * 60 * 60) * 10) / 10
        })),
        impact: '长时间运行的任务可能表示系统负载过高或任务复杂度高',
        urgency: 'WITHIN_DAYS'
      });
    }

    // 检查任务进度停滞
    const stalledTasks = runningTasks.filter(task => {
      return task.PROGRESS < 50 && task.RUNS > 5; // 进度小于50%但重试次数多
    });

    if (stalledTasks.length > 0) {
      criticals.push({
        type: 'stalled_compaction_tasks',
        severity: 'CRITICAL',
        message: `发现 ${stalledTasks.length} 个可能停滞的Compaction任务`,
        tasks: stalledTasks.map(task => ({
          be_id: task.BE_ID,
          tablet_id: task.TABLET_ID,
          progress: task.PROGRESS,
          runs: task.RUNS,
          status: task.STATUS
        })),
        impact: '停滞的任务阻塞Compaction队列，影响整体性能',
        urgency: 'IMMEDIATE'
      });
    }

    // 分析最近任务的成功率
    if (recentTasks.length > 0) {
      const failedTasks = recentTasks.filter(task => task.STATUS === 'FAILED' || task.PROGRESS < 100);
      const successRate = ((recentTasks.length - failedTasks.length) / recentTasks.length) * 100;

      if (successRate < 90) {
        warnings.push({
          type: 'low_compaction_success_rate',
          severity: 'WARNING',
          message: `Compaction任务成功率较低 (${successRate.toFixed(1)}%)`,
          metrics: {
            total_tasks: recentTasks.length,
            failed_tasks: failedTasks.length,
            success_rate: successRate.toFixed(1)
          },
          impact: '任务失败率高可能导致CS持续积累',
          urgency: 'WITHIN_DAYS'
        });
      }
    }
  }

  /**
   * 系统级Compaction压力诊断
   */
  diagnoseCompactionPressure(data, warnings, criticals, insights) {
    const runningTasksCount = data.running_tasks.length;
    const highCSCount = data.high_cs_partitions.length;
    const nodeCount = data.backends.length;

    // 计算每个节点的平均任务数
    const avgTasksPerNode = runningTasksCount / nodeCount;

    if (avgTasksPerNode > this.rules.task_execution.max_running_tasks_per_node) {
      criticals.push({
        type: 'high_compaction_pressure',
        severity: 'CRITICAL',
        message: `系统Compaction压力过高，平均每节点 ${avgTasksPerNode.toFixed(1)} 个任务`,
        metrics: {
          running_tasks: runningTasksCount,
          node_count: nodeCount,
          avg_tasks_per_node: avgTasksPerNode.toFixed(1),
          high_cs_partitions: highCSCount
        },
        impact: '系统负载过高，可能导致性能下降和任务积压',
        urgency: 'IMMEDIATE'
      });
    }

    // 生成压力分析洞察
    insights.push({
      type: 'compaction_pressure_analysis',
      message: 'Compaction系统压力评估',
      assessment: {
        pressure_level: this.assessCompactionPressure(runningTasksCount, highCSCount, nodeCount),
        current_load: {
          running_tasks: runningTasksCount,
          high_cs_partitions: highCSCount,
          avg_tasks_per_node: avgTasksPerNode.toFixed(1)
        },
        capacity_utilization: this.calculateCapacityUtilization(data.thread_config, runningTasksCount)
      },
      recommendations: this.generateCapacityRecommendations(avgTasksPerNode, highCSCount)
    });
  }

  /**
   * 评估Compaction压力级别
   */
  assessCompactionPressure(runningTasks, highCSCount, nodeCount) {
    const avgTasks = runningTasks / nodeCount;
    const csPerNode = highCSCount / nodeCount;

    if (avgTasks > 8 || csPerNode > 50) return 'HIGH';
    if (avgTasks > 5 || csPerNode > 20) return 'MEDIUM';
    return 'LOW';
  }

  /**
   * 计算容量利用率
   */
  calculateCapacityUtilization(threadConfig, runningTasks) {
    const totalThreads = threadConfig.reduce((sum, config) => sum + parseInt(config.VALUE), 0);
    const utilization = totalThreads > 0 ? (runningTasks / totalThreads) * 100 : 0;

    return {
      total_threads: totalThreads,
      running_tasks: runningTasks,
      utilization_percent: utilization.toFixed(1)
    };
  }

  /**
   * 生成容量建议
   */
  generateCapacityRecommendations(avgTasksPerNode, highCSCount) {
    const recommendations = [];

    if (avgTasksPerNode > 5) {
      recommendations.push('考虑增加Compaction线程数或集群扩容');
    }

    if (highCSCount > 100) {
      recommendations.push('制定分批Compaction计划，优先处理高CS分区');
    }

    if (recommendations.length === 0) {
      recommendations.push('当前Compaction容量充足，保持现有配置');
    }

    return recommendations;
  }

  /**
   * 生成Compaction专业建议
   */
  generateCompactionRecommendations(diagnosis, data) {
    const recommendations = [];

    [...diagnosis.criticals, ...diagnosis.warnings].forEach(issue => {
      switch (issue.type) {
        case 'emergency_compaction_score':
        case 'critical_compaction_score':
          recommendations.push({
            category: 'emergency_compaction',
            priority: 'HIGH',
            title: '紧急Compaction Score处理',
            description: '立即处理高CS分区以恢复查询性能',
            professional_actions: [
              {
                action: '识别最高CS分区',
                command: 'SELECT DB_NAME, TABLE_NAME, PARTITION_NAME, MAX_CS FROM information_schema.partitions_meta WHERE MAX_CS > 500 ORDER BY MAX_CS DESC LIMIT 10;',
                purpose: '定位需要紧急处理的分区'
              },
              {
                action: '手动触发Compaction',
                command: 'ALTER TABLE {database}.{table} COMPACT PARTITION {partition};',
                risk_level: 'LOW',
                estimated_time: '10-60分钟（取决于数据量）',
                note: '替换实际的数据库、表和分区名称'
              },
              {
                action: '监控Compaction进度',
                command: 'SELECT * FROM information_schema.be_cloud_native_compactions WHERE TABLET_ID IN (SELECT TABLET_ID FROM {table_tablets});',
                purpose: '确认Compaction任务正常执行'
              }
            ],
            batch_processing_strategy: {
              description: '对于多个高CS分区，建议分批处理',
              steps: [
                '按CS从高到低排序',
                '每次处理5-10个分区',
                '监控系统资源使用情况',
                '在低峰期进行批量处理'
              ]
            }
          });
          break;

        case 'low_compaction_threads':
          recommendations.push({
            category: 'thread_optimization',
            priority: 'MEDIUM',
            title: 'Compaction线程数优化',
            description: `优化节点 ${issue.node} 的Compaction线程配置`,
            professional_actions: [
              {
                action: '评估当前资源使用',
                steps: [
                  '监控CPU使用率',
                  '检查内存使用情况',
                  '评估磁盘IO负载'
                ]
              },
              {
                action: '调整线程配置',
                command: `ADMIN SET be_config ("compact_threads" = "${issue.metrics.recommended_min}") FOR "${issue.node}";`,
                risk_level: 'LOW',
                estimated_time: '立即生效',
                note: `建议设置为 ${issue.metrics.recommended_min} 线程`
              },
              {
                action: '监控调整效果',
                monitoring_metrics: [
                  'Compaction任务完成速度',
                  'CPU使用率变化',
                  '新CS产生速度'
                ],
                monitoring_duration: '24-48小时'
              }
            ]
          });
          break;

        case 'stalled_compaction_tasks':
          recommendations.push({
            category: 'task_recovery',
            priority: 'HIGH',
            title: '停滞任务恢复处理',
            description: '处理停滞的Compaction任务',
            professional_actions: [
              {
                action: '分析停滞原因',
                investigation_steps: [
                  '检查BE节点日志',
                  '验证磁盘空间是否充足',
                  '确认网络连接状态',
                  '检查系统资源是否足够'
                ]
              },
              {
                action: '尝试任务恢复',
                note: '谨慎操作，建议在维护窗口进行',
                risk_level: 'MEDIUM'
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
      configuration_optimization: [],
      operational_optimization: [],
      monitoring_enhancement: []
    };

    // 配置优化建议
    if (data.thread_config.length > 0) {
      suggestions.configuration_optimization.push({
        area: 'thread_configuration',
        suggestion: '根据CPU核心数和工作负载动态调整Compaction线程数',
        implementation: '定期评估线程配置与系统负载的匹配度'
      });
    }

    // 运维优化建议
    if (data.high_cs_partitions.length > 0) {
      suggestions.operational_optimization.push({
        area: 'proactive_compaction',
        suggestion: '建立主动Compaction策略，避免CS过高',
        implementation: '设置CS阈值告警，定期批量处理高CS分区'
      });
    }

    // 监控增强建议
    suggestions.monitoring_enhancement.push({
      area: 'compaction_monitoring',
      suggestion: '增强Compaction监控和告警机制',
      key_metrics: [
        '分区CS分布统计',
        'Compaction任务成功率',
        '任务平均执行时间',
        '系统Compaction容量利用率'
      ]
    });

    return suggestions;
  }

  /**
   * 计算Compaction健康分数
   */
  calculateCompactionHealthScore(diagnosis) {
    let score = 100;

    // 基于不同问题类型的扣分策略
    diagnosis.criticals.forEach(issue => {
      switch (issue.type) {
        case 'emergency_compaction_score':
          score -= 30; // 紧急CS问题扣分最多
          break;
        case 'critical_compaction_score':
          score -= 20;
          break;
        case 'stalled_compaction_tasks':
          score -= 25;
          break;
        default:
          score -= 15;
      }
    });

    diagnosis.warnings.forEach(issue => {
      switch (issue.type) {
        case 'warning_compaction_score':
          score -= 10;
          break;
        case 'low_compaction_threads':
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
   * 生成Compaction诊断摘要
   */
  generateCompactionSummary(criticals, warnings, issues) {
    if (criticals.length > 0) {
      const emergencyIssues = criticals.filter(c => c.type.includes('emergency')).length;
      if (emergencyIssues > 0) {
        return `Compaction系统存在 ${emergencyIssues} 个紧急问题，严重影响查询性能，需立即处理`;
      }
      return `Compaction系统发现 ${criticals.length} 个严重问题，需要立即处理`;
    } else if (warnings.length > 0) {
      return `Compaction系统发现 ${warnings.length} 个警告问题，建议近期优化`;
    } else {
      return 'Compaction系统运行状态良好，压缩效率正常';
    }
  }

  /**
   * 获取高 Compaction Score 分区
   */
  async getHighCompactionPartitions(connection, limit = 10, minScore = 100) {
    try {
      const [partitions] = await connection.query(`
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
      `, [minScore, limit]);

      return {
        success: true,
        data: {
          partitions: partitions,
          total_count: partitions.length,
          filters: {
            min_score: minScore,
            limit: limit
          },
          analysis: this.analyzeHighCompactionPartitions(partitions)
        }
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve high compaction partitions: ${error.message}`,
        data: {
          partitions: [],
          total_count: 0
        }
      };
    }
  }

  /**
   * 获取 Compaction 线程配置
   */
  async getCompactionThreads(connection) {
    try {
      const [threadConfig] = await connection.query(`
        SELECT
          BE_ID as be_id,
          IP as ip,
          VALUE as thread_count
        FROM information_schema.be_configs
        WHERE name = 'compact_threads'
        ORDER BY BE_ID
      `);

      // 获取BE节点信息以便分析
      const [backends] = await connection.query('SHOW BACKENDS');

      const analysis = threadConfig.map(config => {
        const beInfo = backends.find(be => be.BackendId === config.be_id.toString());
        const threads = parseInt(config.thread_count);
        const cpuCores = beInfo ? parseInt(beInfo.CpuCores) || 1 : 1;

        return {
          be_id: config.be_id,
          ip: config.ip,
          thread_count: threads,
          cpu_cores: cpuCores,
          threads_per_core: (threads / cpuCores).toFixed(2),
          recommended_min: Math.max(this.rules.thread_config.absolute_min_threads,
                                   Math.ceil(cpuCores * this.rules.thread_config.min_threads_per_core)),
          recommended_max: Math.min(this.rules.thread_config.absolute_max_threads,
                                   Math.ceil(cpuCores * this.rules.thread_config.max_threads_per_core)),
          status: this.evaluateThreadConfig(threads, cpuCores)
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
            avg_threads_per_core: (analysis.reduce((sum, a) => sum + parseFloat(a.threads_per_core), 0) / analysis.length).toFixed(2)
          },
          analysis: this.analyzeThreadConfiguration(analysis)
        }
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve compaction thread configuration: ${error.message}`,
        data: {
          thread_configurations: [],
          cluster_summary: null
        }
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
    const lowConfigNodes = analysis.filter(a => a.status === 'LOW');
    const highConfigNodes = analysis.filter(a => a.status === 'HIGH');
    const optimalNodes = analysis.filter(a => a.status === 'OPTIMAL');

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
        high: highConfigNodes.length
      },
      recommendations: recommendations
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
            new_threads: threadCount
          });
        } catch (error) {
          results.push({
            be_id: backend.BackendId,
            ip: backend.IP,
            status: 'FAILED',
            error: error.message
          });
        }
      }

      const successCount = results.filter(r => r.status === 'SUCCESS').length;
      const failureCount = results.filter(r => r.status === 'FAILED').length;

      return {
        success: failureCount === 0,
        data: {
          operation: 'set_compaction_threads',
          target_thread_count: threadCount,
          results: results,
          summary: {
            total_nodes: backends.length,
            successful_updates: successCount,
            failed_updates: failureCount
          }
        }
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to set compaction threads: ${error.message}`,
        data: {
          operation: 'set_compaction_threads',
          target_thread_count: threadCount,
          results: []
        }
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
          running_tasks: includeDetails ? tasks : tasks.map(t => ({
            be_id: t.be_id,
            tablet_id: t.tablet_id,
            progress: t.progress,
            status: t.status
          })),
          task_count: tasks.length,
          analysis: taskAnalysis
        }
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to retrieve running compaction tasks: ${error.message}`,
        data: {
          running_tasks: [],
          task_count: 0
        }
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
        status: 'IDLE'
      };
    }

    const now = new Date();
    const longRunningTasks = tasks.filter(task => {
      const startTime = new Date(task.start_time);
      const runningHours = (now - startTime) / (1000 * 60 * 60);
      return runningHours > this.rules.task_execution.slow_task_threshold_hours;
    });

    const stalledTasks = tasks.filter(task =>
      task.progress < 50 && task.runs > 5
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
      avg_progress: tasks.length > 0 ? (tasks.reduce((sum, t) => sum + t.progress, 0) / tasks.length).toFixed(1) : 0
    };
  }

  /**
   * 分析高 Compaction Score 原因
   */
  async analyzeHighCompactionScore(connection, targetDatabase = null, minScore = 100) {
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
          recommendations: this.generateCompactionScoreRecommendations(analysis)
        }
      };
    } catch (error) {
      return {
        success: false,
        error: `Failed to analyze high compaction scores: ${error.message}`,
        data: {
          high_score_partitions: [],
          analysis: null
        }
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
        severity: 'NORMAL'
      };
    }

    const scores = partitions.map(p => p.max_compaction_score);
    const maxScore = Math.max(...scores);
    const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;

    // 按数据库分组分析
    const byDatabase = {};
    partitions.forEach(p => {
      if (!byDatabase[p.database_name]) {
        byDatabase[p.database_name] = [];
      }
      byDatabase[p.database_name].push(p);
    });

    // 按表分组分析
    const byTable = {};
    partitions.forEach(p => {
      const tableKey = `${p.database_name}.${p.table_name}`;
      if (!byTable[tableKey]) {
        byTable[tableKey] = [];
      }
      byTable[tableKey].push(p);
    });

    let severity = 'NORMAL';
    if (maxScore >= this.rules.compaction_score.emergency_threshold) {
      severity = 'EMERGENCY';
    } else if (maxScore >= this.rules.compaction_score.critical_threshold) {
      severity = 'CRITICAL';
    } else if (maxScore >= this.rules.compaction_score.warning_threshold) {
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
        affected_tables: Object.keys(byTable).length
      },
      by_database: Object.entries(byDatabase).map(([db, parts]) => ({
        database: db,
        partition_count: parts.length,
        max_score: Math.max(...parts.map(p => p.max_compaction_score)),
        avg_score: (parts.reduce((sum, p) => sum + p.max_compaction_score, 0) / parts.length).toFixed(2)
      })),
      by_table: Object.entries(byTable).map(([table, parts]) => ({
        table: table,
        partition_count: parts.length,
        max_score: Math.max(...parts.map(p => p.max_compaction_score)),
        avg_score: (parts.reduce((sum, p) => sum + p.max_compaction_score, 0) / parts.length).toFixed(2)
      }))
    };
  }

  /**
   * 生成 Compaction Score 建议
   */
  generateCompactionScoreRecommendations(analysis) {
    const recommendations = [];

    if (analysis.severity === 'EMERGENCY') {
      recommendations.push({
        priority: 'URGENT',
        action: '立即手动触发最高 CS 分区的 compaction',
        reason: '防止查询性能严重下降'
      });
    }

    if (analysis.severity === 'CRITICAL' || analysis.severity === 'EMERGENCY') {
      recommendations.push({
        priority: 'HIGH',
        action: '增加 compaction 线程数',
        reason: '提高 compaction 处理能力'
      });
    }

    if (analysis.statistics.affected_tables > 10) {
      recommendations.push({
        priority: 'MEDIUM',
        action: '制定分批 compaction 计划',
        reason: '避免同时处理过多表影响系统性能'
      });
    }

    recommendations.push({
      priority: 'LOW',
      action: '建立 CS 监控告警',
      reason: '及早发现和处理高 CS 问题'
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
          message: `Compaction initiated for partition ${database}.${table}.${partition}`
        }
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
          status: 'FAILED'
        }
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
        recommendations: []
      };
    }

    const maxScore = Math.max(...partitions.map(p => p.max_compaction_score));
    const avgScore = partitions.reduce((sum, p) => sum + p.max_compaction_score, 0) / partitions.length;

    let severity = 'NORMAL';
    let recommendations = [];

    if (maxScore >= this.rules.compaction_score.emergency_threshold) {
      severity = 'EMERGENCY';
      recommendations.push('立即进行手动 compaction，避免严重影响查询性能');
    } else if (maxScore >= this.rules.compaction_score.critical_threshold) {
      severity = 'CRITICAL';
      recommendations.push('优先处理高分区的 compaction 任务');
    } else if (maxScore >= this.rules.compaction_score.warning_threshold) {
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
      affected_tables: [...new Set(partitions.map(p => `${p.database_name}.${p.table_name}`))].length
    };
  }
}

export { StarRocksCompactionExpert };