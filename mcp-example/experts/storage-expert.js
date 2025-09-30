/**
 * StarRocks 存储专家模块
 * 负责：磁盘使用、Tablet健康、副本状态、数据分布等存储相关诊断
 */

class StarRocksStorageExpert {
  constructor() {
    this.name = 'storage';
    this.version = '1.0.0';
    this.description = 'StarRocks 存储系统专家 - 负责磁盘、Tablet、副本管理等存储相关诊断';

    // 存储专业知识规则库
    this.rules = {
      // 磁盘使用规则
      disk_usage: {
        warning_threshold: 85,
        critical_threshold: 95,
        emergency_threshold: 98,
        free_space_minimum_gb: 10
      },

      // Tablet 健康规则
      tablet_health: {
        error_tablet_threshold: 10,
        max_tablet_per_be: 50000,
        replica_missing_threshold: 5
      },

      // 数据分布规则
      data_distribution: {
        imbalance_threshold: 20, // 数据分布不均衡阈值(%)
        single_node_data_limit: 30 // 单节点数据占比上限(%)
      },

      // 存储性能规则
      storage_performance: {
        io_util_threshold: 80,
        disk_queue_threshold: 10,
        slow_disk_threshold: 100 // ms
      }
    };

    // 专业术语和解释
    this.terminology = {
      tablet: 'StarRocks中数据的基本存储单元，每个分区的数据分片',
      replica: 'Tablet的副本，用于数据冗余和高可用',
      compaction_score: '衡量数据文件碎片化程度的指标，分数越高说明碎片越多'
    };
  }

  /**
   * 存储系统综合诊断
   */
  async diagnose(connection, includeDetails = true) {
    try {
      const startTime = new Date();

      // 1. 收集存储相关数据
      const storageData = await this.collectStorageData(connection);

      // 2. 执行专业诊断分析
      const diagnosis = this.performStorageDiagnosis(storageData);

      // 3. 生成专业建议
      const recommendations = this.generateStorageRecommendations(diagnosis, storageData);

      // 4. 计算存储健康分数
      const healthScore = this.calculateStorageHealthScore(diagnosis);

      const endTime = new Date();
      const analysisTime = endTime - startTime;

      return {
        expert: this.name,
        version: this.version,
        timestamp: new Date().toISOString(),
        analysis_duration_ms: analysisTime,
        storage_health: healthScore,
        diagnosis_results: diagnosis,
        professional_recommendations: recommendations,
        raw_data: includeDetails ? storageData : null,
        next_check_interval: this.suggestNextCheckInterval(diagnosis)
      };
    } catch (error) {
      throw new Error(`存储专家诊断失败: ${error.message}`);
    }
  }

  /**
   * 收集存储相关数据
   */
  async collectStorageData(connection) {
    const data = {};

    // 1. BE节点存储信息
    const [backends] = await connection.query('SHOW BACKENDS;');
    data.backends = backends;

    // 2. Tablet统计信息
    try {
      const [tabletStats] = await connection.query(`
        SELECT
          COUNT(*) as total_tablets,
          COUNT(CASE WHEN ErrTabletNum > 0 THEN 1 END) as nodes_with_errors,
          SUM(ErrTabletNum) as total_error_tablets,
          SUM(TabletNum) as total_tablets_on_nodes
        FROM information_schema.backends;
      `);
      data.tablet_statistics = tabletStats[0];
    } catch (error) {
      console.warn('Failed to collect tablet statistics:', error.message);
      data.tablet_statistics = null;
    }

    // 3. 分区存储信息
    try {
      const [partitionStorage] = await connection.query(`
        SELECT
          DB_NAME, TABLE_NAME, PARTITION_NAME,
          DATA_SIZE, ROW_COUNT, STORAGE_SIZE,
          BUCKETS, REPLICATION_NUM
        FROM information_schema.partitions_meta
        ORDER BY STORAGE_SIZE DESC
        LIMIT 50;
      `);
      data.partition_storage = partitionStorage;
    } catch (error) {
      console.warn('Failed to collect partition storage info:', error.message);
      data.partition_storage = [];
    }

    // 4. 磁盘IO统计 (如果可用)
    try {
      const [diskIO] = await connection.query(`
        SELECT * FROM information_schema.be_metrics
        WHERE metric_name LIKE '%disk%' OR metric_name LIKE '%io%'
        LIMIT 20;
      `);
      data.disk_io_metrics = diskIO;
    } catch (error) {
      data.disk_io_metrics = [];
    }

    return data;
  }

  /**
   * 执行存储专业诊断
   */
  performStorageDiagnosis(data) {
    const issues = [];
    const warnings = [];
    const criticals = [];
    const insights = [];

    // 1. 磁盘使用诊断
    this.diagnoseDiskUsage(data.backends, issues, warnings, criticals);

    // 2. Tablet健康诊断
    this.diagnoseTabletHealth(data, issues, warnings, criticals);

    // 3. 数据分布诊断
    this.diagnoseDataDistribution(data.backends, data.partition_storage, insights, warnings);

    // 4. 存储性能诊断
    this.diagnoseStoragePerformance(data, warnings, criticals);

    return {
      total_issues: issues.length + warnings.length + criticals.length,
      criticals: criticals,
      warnings: warnings,
      issues: issues,
      insights: insights,
      summary: this.generateStorageSummary(criticals, warnings, issues)
    };
  }

  /**
   * 磁盘使用诊断
   */
  diagnoseDiskUsage(backends, issues, warnings, criticals) {
    backends.forEach(be => {
      const diskUsage = parseFloat(be.MaxDiskUsedPct?.replace('%', '')) || 0;
      const availGB = this.parseStorageSize(be.AvailCapacity);

      if (diskUsage >= this.rules.disk_usage.emergency_threshold) {
        criticals.push({
          type: 'disk_emergency',
          node: be.IP,
          severity: 'CRITICAL',
          message: `节点 ${be.IP} 磁盘使用率达到紧急水平 (${be.MaxDiskUsedPct})`,
          metrics: { usage: diskUsage, available_gb: availGB },
          impact: '可能导致写入失败和服务中断',
          urgency: 'IMMEDIATE',
          estimated_time_to_full: this.estimateTimeToFull(availGB, be.IP)
        });
      } else if (diskUsage >= this.rules.disk_usage.critical_threshold) {
        criticals.push({
          type: 'disk_critical',
          node: be.IP,
          severity: 'CRITICAL',
          message: `节点 ${be.IP} 磁盘使用率过高 (${be.MaxDiskUsedPct})`,
          metrics: { usage: diskUsage, available_gb: availGB },
          impact: '写入性能下降，可能导致数据导入失败',
          urgency: 'WITHIN_HOURS'
        });
      } else if (diskUsage >= this.rules.disk_usage.warning_threshold) {
        warnings.push({
          type: 'disk_warning',
          node: be.IP,
          severity: 'WARNING',
          message: `节点 ${be.IP} 磁盘使用率较高 (${be.MaxDiskUsedPct})`,
          metrics: { usage: diskUsage, available_gb: availGB },
          impact: '需要关注存储空间，建议制定清理计划',
          urgency: 'WITHIN_DAYS'
        });
      }

      // 检查最小可用空间
      if (availGB < this.rules.disk_usage.free_space_minimum_gb) {
        criticals.push({
          type: 'low_free_space',
          node: be.IP,
          severity: 'CRITICAL',
          message: `节点 ${be.IP} 可用空间不足 (${be.AvailCapacity})`,
          metrics: { available_gb: availGB },
          impact: '极高风险，可能立即导致写入失败',
          urgency: 'IMMEDIATE'
        });
      }
    });
  }

  /**
   * Tablet健康诊断
   */
  diagnoseTabletHealth(data, issues, warnings, criticals) {
    const backends = data.backends;
    const tabletStats = data.tablet_statistics;

    // 检查错误Tablet
    backends.forEach(be => {
      const errorTablets = parseInt(be.ErrTabletNum) || 0;
      if (errorTablets > 0) {
        const severity = errorTablets >= this.rules.tablet_health.error_tablet_threshold ? 'CRITICAL' : 'WARNING';

        (severity === 'CRITICAL' ? criticals : warnings).push({
          type: 'error_tablets',
          node: be.IP,
          severity: severity,
          message: `节点 ${be.IP} 发现 ${errorTablets} 个错误Tablet`,
          metrics: { error_count: errorTablets, total_tablets: be.TabletNum },
          impact: severity === 'CRITICAL' ?
            '数据可用性受影响，可能导致查询失败' :
            '数据完整性风险，建议检查副本状态',
          urgency: severity === 'CRITICAL' ? 'IMMEDIATE' : 'WITHIN_DAYS',
          recommended_actions: [
            'SHOW PROC "/dbs/{db_id}/{table_id}"; -- 查看具体错误Tablet',
            'ADMIN REPAIR TABLE {table_name}; -- 尝试修复',
            '检查磁盘和网络状态'
          ]
        });
      }

      // 检查Tablet数量分布
      const tabletCount = parseInt(be.TabletNum) || 0;
      if (tabletCount > this.rules.tablet_health.max_tablet_per_be) {
        warnings.push({
          type: 'high_tablet_count',
          node: be.IP,
          severity: 'WARNING',
          message: `节点 ${be.IP} Tablet数量过多 (${tabletCount})`,
          metrics: { tablet_count: tabletCount },
          impact: '可能影响节点性能和故障恢复时间',
          urgency: 'WITHIN_WEEKS',
          recommended_actions: [
            '考虑集群扩容',
            '检查表分区策略是否合理',
            '评估Tablet分布均衡性'
          ]
        });
      }
    });

    // 全局Tablet统计分析
    if (tabletStats && tabletStats.total_error_tablets > 0) {
      const errorRate = (tabletStats.total_error_tablets / tabletStats.total_tablets_on_nodes) * 100;

      if (errorRate > 1) { // 错误率超过1%
        criticals.push({
          type: 'high_error_tablet_rate',
          severity: 'CRITICAL',
          message: `集群错误Tablet比例过高 (${errorRate.toFixed(2)}%)`,
          metrics: {
            error_tablets: tabletStats.total_error_tablets,
            total_tablets: tabletStats.total_tablets_on_nodes,
            error_rate: errorRate
          },
          impact: '集群数据完整性存在严重风险',
          urgency: 'IMMEDIATE'
        });
      }
    }
  }

  /**
   * 数据分布诊断
   */
  diagnoseDataDistribution(backends, partitions, insights, warnings) {
    // 计算数据分布均衡性
    const dataSizes = backends.map(be => this.parseStorageSize(be.DataUsedCapacity));
    const totalData = dataSizes.reduce((sum, size) => sum + size, 0);

    if (totalData > 0) {
      const avgDataPerNode = totalData / backends.length;

      backends.forEach(be => {
        const nodeData = this.parseStorageSize(be.DataUsedCapacity);
        const deviationPercent = Math.abs((nodeData - avgDataPerNode) / avgDataPerNode) * 100;

        if (deviationPercent > this.rules.data_distribution.imbalance_threshold) {
          warnings.push({
            type: 'data_imbalance',
            node: be.IP,
            severity: 'WARNING',
            message: `节点 ${be.IP} 数据分布不均衡，偏差 ${deviationPercent.toFixed(1)}%`,
            metrics: {
              node_data_gb: nodeData,
              cluster_avg_gb: avgDataPerNode,
              deviation_percent: deviationPercent
            },
            impact: '可能导致热点节点和查询性能不均衡',
            urgency: 'WITHIN_WEEKS'
          });
        }
      });
    }

    // 分析大表分区
    if (partitions && partitions.length > 0) {
      const largePartitions = partitions.filter(p =>
        this.parseStorageSize(p.DATA_SIZE) > 10 // 大于10GB的分区
      );

      if (largePartitions.length > 0) {
        insights.push({
          type: 'large_partitions_analysis',
          message: `发现 ${largePartitions.length} 个大分区 (>10GB)`,
          details: largePartitions.slice(0, 5).map(p => ({
            partition: `${p.DB_NAME}.${p.TABLE_NAME}.${p.PARTITION_NAME}`,
            size: p.DATA_SIZE,
            rows: p.ROW_COUNT,
            buckets: p.BUCKETS
          })),
          recommendations: [
            '考虑优化大表的分区策略',
            '评估是否需要增加分桶数',
            '检查数据导入模式是否合理'
          ]
        });
      }
    }
  }

  /**
   * 存储性能诊断
   */
  diagnoseStoragePerformance(data, warnings, criticals) {
    // 这里可以添加磁盘IO性能分析
    // 当前先检查基本的磁盘相关指标

    data.backends.forEach(be => {
      const memUsage = parseFloat(be.MemUsedPct?.replace('%', '')) || 0;

      // 高内存使用可能影响磁盘缓存性能
      if (memUsage > 90) {
        warnings.push({
          type: 'high_memory_affecting_io',
          node: be.IP,
          severity: 'WARNING',
          message: `节点 ${be.IP} 高内存使用率 (${be.MemUsedPct}) 可能影响存储性能`,
          metrics: { memory_usage: memUsage },
          impact: '磁盘缓存效率下降，IO性能受影响',
          urgency: 'WITHIN_DAYS'
        });
      }
    });
  }

  /**
   * 生成存储专业建议
   */
  generateStorageRecommendations(diagnosis, data) {
    const recommendations = [];

    // 针对不同类型的问题生成专业建议
    [...diagnosis.criticals, ...diagnosis.warnings].forEach(issue => {
      switch (issue.type) {
        case 'disk_emergency':
        case 'disk_critical':
          recommendations.push({
            category: 'emergency_disk_management',
            priority: 'HIGH',
            title: '紧急磁盘空间处理',
            description: `节点 ${issue.node} 磁盘空间严重不足`,
            professional_actions: [
              {
                action: '立即清理临时文件和日志',
                command: 'find /data/be/log -name "*.log.*" -mtime +7 -delete',
                risk_level: 'LOW',
                estimated_time: '5分钟'
              },
              {
                action: '手动触发Compaction清理过期数据',
                command: 'ALTER TABLE {table} COMPACT;',
                risk_level: 'LOW',
                estimated_time: '10-30分钟',
                note: '需要根据具体表名替换{table}'
              },
              {
                action: '紧急扩容或数据迁移',
                risk_level: 'MEDIUM',
                estimated_time: '30-60分钟',
                prerequisites: ['备份重要数据', '通知相关团队']
              }
            ],
            monitoring_after_fix: [
              '监控磁盘使用率变化',
              '检查Compaction是否正常进行',
              '确认数据导入是否恢复正常'
            ]
          });
          break;

        case 'error_tablets':
          recommendations.push({
            category: 'tablet_repair',
            priority: issue.severity === 'CRITICAL' ? 'HIGH' : 'MEDIUM',
            title: 'Tablet错误修复',
            description: `修复节点 ${issue.node} 上的错误Tablet`,
            professional_actions: [
              {
                action: '诊断错误Tablet详情',
                command: 'SHOW PROC "/dbs";',
                note: '查找对应数据库ID，然后进一步查看表和Tablet详情'
              },
              {
                action: '尝试自动修复',
                command: 'ADMIN REPAIR TABLE {database}.{table};',
                risk_level: 'LOW',
                estimated_time: '5-15分钟'
              },
              {
                action: '检查副本状态',
                command: 'SHOW PROC "/dbs/{db_id}/{table_id}";',
                note: '确认副本数量和健康状态'
              }
            ],
            root_cause_investigation: [
              '检查磁盘是否有坏道',
              '验证网络连接是否稳定',
              '查看BE日志中的错误信息',
              '确认系统资源是否充足'
            ]
          });
          break;

        case 'data_imbalance':
          recommendations.push({
            category: 'data_rebalancing',
            priority: 'MEDIUM',
            title: '数据分布均衡优化',
            description: '优化集群数据分布均衡性',
            professional_actions: [
              {
                action: '分析数据倾斜原因',
                steps: [
                  '检查表的分桶策略是否合理',
                  '分析数据导入模式',
                  '评估分区策略是否需要调整'
                ]
              },
              {
                action: '考虑数据重分布',
                note: '在低峰期进行，避免影响业务',
                risk_level: 'MEDIUM',
                estimated_time: '数小时到数天（取决于数据量）'
              }
            ]
          });
          break;
      }
    });

    // 添加预防性建议
    recommendations.push(this.generatePreventiveRecommendations(data));

    return recommendations.filter(rec => rec); // 过滤空值
  }

  /**
   * 生成预防性建议
   */
  generatePreventiveRecommendations(data) {
    return {
      category: 'preventive_maintenance',
      priority: 'LOW',
      title: '存储系统预防性维护建议',
      description: '定期维护建议，保持存储系统最佳状态',
      professional_actions: [
        {
          action: '定期监控磁盘使用趋势',
          frequency: '每日',
          automation_possible: true
        },
        {
          action: '定期检查Tablet健康状态',
          frequency: '每周',
          command: 'SELECT SUM(ErrTabletNum) FROM information_schema.backends;'
        },
        {
          action: '定期分析数据增长模式',
          frequency: '每月',
          note: '有助于容量规划和性能优化'
        }
      ],
      capacity_planning: {
        recommendation: '基于当前增长趋势，建议提前3-6个月进行扩容规划',
        factors_to_consider: [
          '数据增长速率',
          '业务发展计划',
          '查询复杂度变化',
          '高可用性要求'
        ]
      }
    };
  }

  /**
   * 计算存储健康分数
   */
  calculateStorageHealthScore(diagnosis) {
    let score = 100;

    // 扣分规则
    score -= diagnosis.criticals.length * 25;
    score -= diagnosis.warnings.length * 10;
    score -= diagnosis.issues.length * 5;

    score = Math.max(0, score);

    let level = 'EXCELLENT';
    if (score < 50) level = 'POOR';
    else if (score < 70) level = 'FAIR';
    else if (score < 85) level = 'GOOD';

    return {
      score: score,
      level: level,
      status: diagnosis.criticals.length > 0 ? 'CRITICAL' :
              diagnosis.warnings.length > 0 ? 'WARNING' : 'HEALTHY'
    };
  }

  /**
   * 生成存储诊断摘要
   */
  generateStorageSummary(criticals, warnings, issues) {
    if (criticals.length > 0) {
      return `存储系统发现 ${criticals.length} 个严重问题，需要立即处理`;
    } else if (warnings.length > 0) {
      return `存储系统发现 ${warnings.length} 个警告问题，建议近期处理`;
    } else if (issues.length > 0) {
      return `存储系统发现 ${issues.length} 个一般问题，可安排时间处理`;
    } else {
      return '存储系统运行状态良好，未发现异常问题';
    }
  }

  /**
   * 建议下次检查间隔
   */
  suggestNextCheckInterval(diagnosis) {
    if (diagnosis.criticals.length > 0) {
      return '30分钟'; // 严重问题需要频繁检查
    } else if (diagnosis.warnings.length > 0) {
      return '2小时';  // 警告问题适中频率检查
    } else {
      return '12小时'; // 正常状态定期检查
    }
  }

  /**
   * 解析存储大小字符串为GB
   */
  parseStorageSize(sizeStr) {
    if (!sizeStr) return 0;

    const match = sizeStr.match(/^([\d.]+)\s*(\w+)$/);
    if (!match) return 0;

    const value = parseFloat(match[1]);
    const unit = match[2].toUpperCase();

    const toGB = {
      'B': 1 / (1024 ** 3),
      'KB': 1 / (1024 ** 2),
      'MB': 1 / 1024,
      'GB': 1,
      'TB': 1024
    };

    return value * (toGB[unit] || 0);
  }

  /**
   * 估算磁盘满盈时间
   */
  estimateTimeToFull(availableGB, nodeIP) {
    // 简化估算，实际应该基于历史数据增长趋势
    if (availableGB < 1) return '立即';
    if (availableGB < 5) return '1-2小时';
    if (availableGB < 10) return '4-8小时';
    return '1-2天';
  }
}

export { StarRocksStorageExpert };