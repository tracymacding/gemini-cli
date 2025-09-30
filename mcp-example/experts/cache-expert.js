/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * StarRocks 缓存专家模块
 * 负责：Data Cache 命中率、缓存容量、缓存抖动等缓存性能诊断
 */

/* eslint-disable no-undef, @typescript-eslint/no-unused-vars */

class StarRocksCacheExpert {
  constructor() {
    this.name = 'cache';
    this.version = '1.0.0';
    this.description =
      'StarRocks 缓存系统专家 - 负责 Data Cache 命中率、容量和性能诊断';

    // 缓存专业知识规则库
    this.rules = {
      // 缓存命中率规则
      hit_ratio: {
        excellent_threshold: 90, // 命中率 > 90% 为优秀
        good_threshold: 70, // 命中率 > 70% 为良好
        warning_threshold: 50, // 命中率 < 50% 为警告
        critical_threshold: 30, // 命中率 < 30% 为严重
      },

      // 缓存容量规则
      capacity: {
        warning_threshold: 85, // 使用率 > 85% 为警告
        critical_threshold: 95, // 使用率 > 95% 为严重
      },

      // 缓存抖动检测规则
      jitter: {
        // 命中率标准差阈值
        hit_ratio_std_threshold: 15, // 标准差 > 15% 认为存在抖动
        // 命中率变化率阈值
        hit_ratio_change_threshold: 20, // 短期变化 > 20% 认为存在剧烈波动
      },

      // 推荐的缓存配置
      recommended: {
        min_cache_size_gb: 10, // 最小缓存大小
        cache_to_data_ratio: 0.2, // 推荐缓存占数据比例 20%
      },
    };

    // 专业术语和解释
    this.terminology = {
      data_cache:
        'StarRocks Shared-Data 架构中 Compute Node 的本地缓存，用于缓存热数据减少对象存储访问',
      hit_ratio: '缓存命中率，表示从缓存中成功读取数据的比例',
      cache_jitter:
        '缓存命中率的波动，可能由冷启动、查询模式变化或缓存淘汰引起',
    };
  }

  /**
   * 缓存系统综合诊断
   */
  async diagnose(connection, includeDetails = true) {
    try {
      const startTime = new Date();

      // 1. 收集缓存相关数据
      const cacheData = await this.collectCacheData(connection);

      // 2. 执行专业诊断分析
      const diagnosis = this.performCacheDiagnosis(cacheData);

      // 3. 生成专业建议
      const recommendations = this.generateCacheRecommendations(
        diagnosis,
        cacheData,
      );

      // 4. 计算缓存健康分数
      const healthScore = this.calculateCacheHealthScore(diagnosis);

      const endTime = new Date();
      const analysisTime = endTime - startTime;

      return {
        expert: this.name,
        version: this.version,
        timestamp: new Date().toISOString(),
        analysis_duration_ms: analysisTime,
        cache_health: healthScore,
        diagnosis_results: diagnosis,
        professional_recommendations: recommendations,
        raw_data: includeDetails ? cacheData : null,
        next_check_interval: this.suggestNextCheckInterval(diagnosis),
      };
    } catch (error) {
      throw new Error(`缓存专家诊断失败: ${error.message}`);
    }
  }

  /**
   * 收集缓存相关数据
   */
  async collectCacheData(connection) {
    const data = {
      cache_metrics: [],
      compute_nodes: [],
      architecture_type: null,
    };

    try {
      // 1. 检测架构类型
      data.architecture_type = await this.detectArchitectureType(connection);

      if (data.architecture_type !== 'shared_data') {
        console.log('当前集群为存算一体架构，不支持 Data Cache 分析');
        return data;
      }

      // 2. 获取 Compute Nodes 信息
      try {
        const [computeNodes] = await connection.query('SHOW COMPUTE NODES;');
        data.compute_nodes = computeNodes;
      } catch (error) {
        console.error('获取 Compute Nodes 失败:', error.message);
      }

      // 3. 获取缓存指标
      try {
        const [cacheMetrics] = await connection.query(`
          SELECT * FROM information_schema.be_cache_metrics;
        `);
        data.cache_metrics = cacheMetrics;
      } catch (error) {
        console.error('获取缓存指标失败:', error.message);
      }
    } catch (error) {
      console.error('收集缓存数据失败:', error.message);
    }

    return data;
  }

  /**
   * 检测集群架构类型
   */
  async detectArchitectureType(connection) {
    try {
      const [config] = await connection.query(`
        ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode';
      `);

      if (config && config.length > 0) {
        const runMode = config[0].Value || config[0].value;
        return runMode === 'shared_data' ? 'shared_data' : 'shared_nothing';
      }

      return 'shared_nothing';
    } catch (error) {
      // 回退：尝试查询 COMPUTE NODES
      try {
        const [computeNodes] = await connection.query('SHOW COMPUTE NODES;');
        if (computeNodes && computeNodes.length > 0) {
          return 'shared_data';
        }
      } catch (cnError) {
        // Ignore
      }

      return 'shared_nothing';
    }
  }

  /**
   * 执行缓存诊断
   */
  performCacheDiagnosis(data) {
    const issues = [];
    const warnings = [];
    const criticals = [];
    const insights = [];

    if (data.architecture_type !== 'shared_data') {
      return {
        status: 'not_applicable',
        message: '当前集群为存算一体架构，不适用于 Data Cache 分析',
        total_issues: 0,
        criticals: [],
        warnings: [],
        issues: [],
        insights: [],
      };
    }

    // 1. 缓存命中率诊断
    this.diagnoseCacheHitRatio(data.cache_metrics, issues, warnings, criticals);

    // 2. 缓存容量诊断
    this.diagnoseCacheCapacity(data.cache_metrics, warnings, criticals);

    // 3. 缓存抖动检测（需要历史数据）
    this.detectCacheJitter(data.cache_metrics, warnings, insights);

    return {
      total_issues: issues.length + warnings.length + criticals.length,
      criticals: criticals,
      warnings: warnings,
      issues: issues,
      insights: insights,
      summary: this.generateCacheSummary(criticals, warnings, issues),
    };
  }

  /**
   * 缓存命中率诊断
   */
  diagnoseCacheHitRatio(cacheMetrics, issues, warnings, criticals) {
    if (!cacheMetrics || cacheMetrics.length === 0) {
      warnings.push({
        type: 'no_cache_metrics',
        severity: 'WARNING',
        message: '无法获取缓存指标数据',
        impact: '无法评估缓存性能',
        recommended_actions: [
          '检查 information_schema.be_cache_metrics 表是否可访问',
          '确认 Compute Nodes 是否正常运行',
        ],
      });
      return;
    }

    // 计算整体命中率
    let totalHits = 0;
    let totalRequests = 0;

    cacheMetrics.forEach((metric) => {
      const hitCount = parseInt(metric.hit_count) || 0;
      const missCount = parseInt(metric.miss_count) || 0;
      const requests = hitCount + missCount;

      totalHits += hitCount;
      totalRequests += requests;

      // 单节点命中率分析
      if (requests > 0) {
        const hitRatio = (hitCount / requests) * 100;
        const nodeId = metric.BE_ID || 'unknown';

        if (hitRatio < this.rules.hit_ratio.critical_threshold) {
          criticals.push({
            type: 'low_cache_hit_ratio',
            node: nodeId,
            severity: 'CRITICAL',
            message: `节点 ${nodeId} 缓存命中率过低 (${hitRatio.toFixed(2)}%)`,
            metrics: {
              hit_ratio: hitRatio,
              hit_count: hitCount,
              miss_count: missCount,
            },
            impact: '大量请求访问对象存储，查询性能差，延迟高',
            urgency: 'IMMEDIATE',
          });
        } else if (hitRatio < this.rules.hit_ratio.warning_threshold) {
          warnings.push({
            type: 'low_cache_hit_ratio',
            node: nodeId,
            severity: 'WARNING',
            message: `节点 ${nodeId} 缓存命中率偏低 (${hitRatio.toFixed(2)}%)`,
            metrics: {
              hit_ratio: hitRatio,
              hit_count: hitCount,
              miss_count: missCount,
            },
            impact: '缓存效果不佳，可能影响查询性能',
            urgency: 'WITHIN_DAYS',
          });
        }
      }
    });

    // 整体命中率评估
    if (totalRequests > 0) {
      const overallHitRatio = (totalHits / totalRequests) * 100;

      if (overallHitRatio < this.rules.hit_ratio.warning_threshold) {
        issues.push({
          type: 'overall_low_hit_ratio',
          severity:
            overallHitRatio < this.rules.hit_ratio.critical_threshold
              ? 'CRITICAL'
              : 'WARNING',
          message: `集群整体缓存命中率偏低 (${overallHitRatio.toFixed(2)}%)`,
          metrics: {
            hit_ratio: overallHitRatio,
            total_hits: totalHits,
            total_requests: totalRequests,
          },
          impact: '整体查询性能受影响，建议优化缓存策略',
        });
      }
    }
  }

  /**
   * 缓存容量诊断
   */
  diagnoseCacheCapacity(cacheMetrics, warnings, criticals) {
    if (!cacheMetrics || cacheMetrics.length === 0) return;

    cacheMetrics.forEach((metric) => {
      const capacity = parseInt(metric.disk_cache_capacity_bytes) || 0;
      const used = parseInt(metric.disk_cache_bytes) || 0;

      if (capacity > 0) {
        const usagePercent = (used / capacity) * 100;
        const nodeId = metric.BE_ID || 'unknown';

        if (usagePercent >= this.rules.capacity.critical_threshold) {
          criticals.push({
            type: 'cache_capacity_critical',
            node: nodeId,
            severity: 'CRITICAL',
            message: `节点 ${nodeId} 缓存空间接近满载 (${usagePercent.toFixed(2)}%)`,
            metrics: {
              usage_percent: usagePercent,
              capacity_gb: (capacity / 1024 ** 3).toFixed(2),
              used_gb: (used / 1024 ** 3).toFixed(2),
            },
            impact: '缓存淘汰频繁，严重影响命中率和性能',
            urgency: 'IMMEDIATE',
          });
        } else if (usagePercent >= this.rules.capacity.warning_threshold) {
          warnings.push({
            type: 'cache_capacity_warning',
            node: nodeId,
            severity: 'WARNING',
            message: `节点 ${nodeId} 缓存使用率较高 (${usagePercent.toFixed(2)}%)`,
            metrics: {
              usage_percent: usagePercent,
              capacity_gb: (capacity / 1024 ** 3).toFixed(2),
              used_gb: (used / 1024 ** 3).toFixed(2),
            },
            impact: '缓存可能开始频繁淘汰，建议关注',
            urgency: 'WITHIN_DAYS',
          });
        }
      }
    });
  }

  /**
   * 缓存抖动检测
   * 注意：当前实现基于单次查询，无法检测时序抖动
   * 需要结合 Grafana 监控数据或历史查询来实现完整的抖动检测
   */
  detectCacheJitter(cacheMetrics, warnings, insights) {
    if (!cacheMetrics || cacheMetrics.length === 0) return;

    // 计算各节点命中率的方差
    const hitRatios = [];

    cacheMetrics.forEach((metric) => {
      const hitCount = parseInt(metric.hit_count) || 0;
      const missCount = parseInt(metric.miss_count) || 0;
      const requests = hitCount + missCount;

      if (requests > 0) {
        const hitRatio = (hitCount / requests) * 100;
        hitRatios.push(hitRatio);
      }
    });

    if (hitRatios.length > 1) {
      const mean =
        hitRatios.reduce((sum, val) => sum + val, 0) / hitRatios.length;
      const variance =
        hitRatios.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) /
        hitRatios.length;
      const stdDev = Math.sqrt(variance);

      if (stdDev > this.rules.jitter.hit_ratio_std_threshold) {
        warnings.push({
          type: 'cache_hit_ratio_variance',
          severity: 'WARNING',
          message: `各节点缓存命中率差异较大 (标准差: ${stdDev.toFixed(2)}%)`,
          metrics: {
            mean_hit_ratio: mean.toFixed(2),
            std_dev: stdDev.toFixed(2),
            node_count: hitRatios.length,
          },
          impact: '可能存在数据倾斜或节点性能不均',
          recommended_actions: [
            '检查各节点的查询负载是否均衡',
            '分析是否存在热点数据',
            '评估缓存容量配置是否一致',
          ],
        });
      }

      insights.push({
        type: 'cache_hit_ratio_distribution',
        message: '缓存命中率分布分析',
        metrics: {
          mean: mean.toFixed(2),
          std_dev: stdDev.toFixed(2),
          min: Math.min(...hitRatios).toFixed(2),
          max: Math.max(...hitRatios).toFixed(2),
        },
        note: '建议结合 Grafana 监控查看时序趋势以检测抖动',
      });
    }
  }

  /**
   * 生成缓存专业建议
   */
  generateCacheRecommendations(diagnosis, data) {
    const recommendations = [];

    if (diagnosis.status === 'not_applicable') {
      return recommendations;
    }

    // 针对不同类型的问题生成专业建议
    [...diagnosis.criticals, ...diagnosis.warnings].forEach((issue) => {
      switch (issue.type) {
        case 'low_cache_hit_ratio':
        case 'overall_low_hit_ratio':
          recommendations.push({
            category: 'cache_hit_ratio_optimization',
            priority: issue.severity === 'CRITICAL' ? 'HIGH' : 'MEDIUM',
            title: '提升缓存命中率',
            description: '优化缓存配置和查询模式以提高命中率',
            professional_actions: [
              {
                action: '增加缓存容量',
                command: '调整 datacache_disk_path 配置，增加本地磁盘缓存空间',
                risk_level: 'LOW',
                estimated_time: '需要重启 Compute Node',
              },
              {
                action: '分析查询模式',
                steps: [
                  '识别常用查询和热点表',
                  '评估是否有大量全表扫描',
                  '检查是否有缓存污染（大查询挤占缓存）',
                ],
              },
              {
                action: '调整缓存淘汰策略',
                command: '评估 datacache_evict_policy 配置',
                note: '可选策略: LRU, LFU 等',
              },
            ],
            monitoring_after_fix: [
              '监控命中率变化趋势',
              '观察对象存储访问量是否下降',
              '评估查询延迟改善情况',
            ],
          });
          break;

        case 'cache_capacity_critical':
        case 'cache_capacity_warning':
          recommendations.push({
            category: 'cache_capacity_expansion',
            priority: issue.severity === 'CRITICAL' ? 'HIGH' : 'MEDIUM',
            title: '扩展缓存容量',
            description: `节点 ${issue.node} 缓存空间不足，需要扩容`,
            professional_actions: [
              {
                action: '增加本地磁盘容量',
                steps: [
                  '为 Compute Node 添加更多本地磁盘',
                  '更新 datacache_disk_path 配置',
                  '重启 Compute Node 使配置生效',
                ],
                risk_level: 'MEDIUM',
              },
              {
                action: '增加 Compute Node 数量',
                note: '扩展集群总缓存容量',
                estimated_time: '30-60分钟',
              },
            ],
          });
          break;

        case 'cache_hit_ratio_variance':
          recommendations.push({
            category: 'load_balancing',
            priority: 'MEDIUM',
            title: '优化负载均衡',
            description: '改善各节点间的缓存命中率差异',
            professional_actions: [
              {
                action: '检查查询路由策略',
                note: '确保查询在节点间均匀分布',
              },
              {
                action: '分析数据分布',
                steps: [
                  '检查是否存在数据倾斜',
                  '评估分区和分桶策略',
                  '考虑数据重分布',
                ],
              },
            ],
          });
          break;
      }
    });

    // 添加预防性建议
    recommendations.push(this.generatePreventiveRecommendations(data));

    return recommendations.filter((rec) => rec);
  }

  /**
   * 生成预防性建议
   */
  generatePreventiveRecommendations(data) {
    return {
      category: 'preventive_maintenance',
      priority: 'LOW',
      title: '缓存系统预防性维护建议',
      description: '定期维护建议，保持缓存系统最佳性能',
      professional_actions: [
        {
          action: '持续监控缓存命中率',
          frequency: '实时',
          automation_possible: true,
          note: '建议在 Grafana 设置命中率告警',
        },
        {
          action: '定期分析查询模式',
          frequency: '每周',
          note: '识别缓存效率低的查询并优化',
        },
        {
          action: '评估缓存容量规划',
          frequency: '每月',
          note: '根据数据增长趋势调整缓存容量',
        },
      ],
      grafana_monitoring: {
        recommendation: '建议在 Grafana 监控以下指标',
        key_metrics: [
          'Cache Hit Ratio 趋势图（检测抖动）',
          'Cache Capacity Usage（容量监控）',
          'Cache Hit/Miss Count（请求量分析）',
          'Object Storage Access Rate（评估缓存效果）',
        ],
      },
    };
  }

  /**
   * 计算缓存健康分数
   */
  calculateCacheHealthScore(diagnosis) {
    if (diagnosis.status === 'not_applicable') {
      return {
        score: 0,
        level: 'N/A',
        status: 'NOT_APPLICABLE',
      };
    }

    let score = 100;

    // 严重问题扣分
    score -= diagnosis.criticals.length * 20;
    // 警告扣分
    score -= diagnosis.warnings.length * 10;
    // 一般问题扣分
    score -= diagnosis.issues.length * 5;

    score = Math.max(0, score);

    let level = 'EXCELLENT';
    if (score < 50) level = 'POOR';
    else if (score < 70) level = 'FAIR';
    else if (score < 85) level = 'GOOD';

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
   * 生成缓存诊断摘要
   */
  generateCacheSummary(criticals, warnings, issues) {
    if (criticals.length > 0) {
      return `发现 ${criticals.length} 个严重缓存问题需要立即处理`;
    } else if (warnings.length > 0) {
      return `发现 ${warnings.length} 个缓存警告需要关注`;
    } else if (issues.length > 0) {
      return `发现 ${issues.length} 个缓存问题建议优化`;
    }
    return '缓存系统运行正常';
  }

  /**
   * 建议下次检查间隔
   */
  suggestNextCheckInterval(diagnosis) {
    if (diagnosis.status === 'not_applicable') {
      return 'N/A';
    }

    if (diagnosis.criticals.length > 0) {
      return '5分钟'; // 严重问题需要频繁检查
    } else if (diagnosis.warnings.length > 0) {
      return '30分钟'; // 警告问题适中频率检查
    } else {
      return '1小时'; // 正常状态定期检查
    }
  }

  /**
   * 分析缓存命中率时序数据（需要 Grafana 或历史数据）
   * 这是一个占位方法，实际实现需要对接 Grafana API 或历史监控数据
   */
  async analyzeCacheHitRatioTimeSeries(connection, timeRange = '1h') {
    // TODO: 对接 Grafana API 或 Prometheus 获取时序数据
    // 示例返回格式
    return {
      status: 'not_implemented',
      message:
        '缓存抖动检测需要对接 Grafana 监控系统，暂未实现。建议在 Grafana 查看 Cache Hit Ratio 趋势图',
      recommended_grafana_panels: [
        {
          name: 'Cache Hit Ratio',
          query: 'rate(cache_hit_count[5m]) / rate(cache_request_count[5m])',
          alert_condition: 'std_dev > 15%',
        },
        {
          name: 'Cache Miss Rate',
          query: 'rate(cache_miss_count[5m])',
          alert_condition: 'increase > 50%',
        },
      ],
    };
  }

  /**
   * 获取此专家提供的 MCP 工具处理器
   */
  getToolHandlers() {
    return {
      analyze_cache_performance: async (args, context) => {
        console.log(
          '🎯 Tool handler 接收到的参数:',
          JSON.stringify(args, null, 2),
        );

        const connection = context.connection;
        const result = await this.diagnose(
          connection,
          args.include_details !== false,
        );

        const report = this.formatCacheReport(result);

        return {
          content: [
            {
              type: 'text',
              text: report,
            },
            {
              type: 'text',
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      },
    };
  }

  /**
   * 格式化缓存分析报告
   */
  formatCacheReport(analysis) {
    let report = '📊 StarRocks Data Cache 性能分析\n';
    report += '========================================\n\n';

    if (
      analysis.diagnosis_results.status === 'not_applicable' ||
      analysis.raw_data?.architecture_type !== 'shared_data'
    ) {
      report += 'ℹ️  当前集群为存算一体架构，不支持 Data Cache 分析\n';
      return report;
    }

    // 健康评分
    const health = analysis.cache_health;
    const healthEmoji =
      health.status === 'CRITICAL'
        ? '🔴'
        : health.status === 'WARNING'
          ? '🟡'
          : '🟢';
    report += `${healthEmoji} **缓存健康评分**: ${health.score}/100 (${health.level})\n\n`;

    // 缓存指标概览
    if (analysis.raw_data?.cache_metrics?.length > 0) {
      let totalHits = 0;
      let totalRequests = 0;
      let totalCapacity = 0;
      let totalUsed = 0;

      analysis.raw_data.cache_metrics.forEach((metric) => {
        const hitCount = parseInt(metric.hit_count) || 0;
        const missCount = parseInt(metric.miss_count) || 0;
        totalHits += hitCount;
        totalRequests += hitCount + missCount;
        totalCapacity += parseInt(metric.disk_cache_capacity_bytes) || 0;
        totalUsed += parseInt(metric.disk_cache_bytes) || 0;
      });

      const hitRatio =
        totalRequests > 0 ? ((totalHits / totalRequests) * 100).toFixed(2) : 0;
      const capacityUsage =
        totalCapacity > 0 ? ((totalUsed / totalCapacity) * 100).toFixed(2) : 0;

      report += '📦 **整体缓存指标**:\n';
      report += `   总缓存容量: ${(totalCapacity / 1024 ** 3).toFixed(2)} GB\n`;
      report += `   已用容量: ${(totalUsed / 1024 ** 3).toFixed(2)} GB (${capacityUsage}%)\n`;
      report += `   整体命中率: ${hitRatio}%\n`;
      report += `   总请求数: ${totalRequests.toLocaleString()}\n\n`;
    }

    // 问题汇总
    const diagnosis = analysis.diagnosis_results;
    if (diagnosis.criticals.length > 0) {
      report += '🔴 **严重问题**:\n';
      diagnosis.criticals.forEach((issue) => {
        report += `   • ${issue.message}\n`;
        report += `     影响: ${issue.impact}\n`;
      });
      report += '\n';
    }

    if (diagnosis.warnings.length > 0) {
      report += '🟡 **警告**:\n';
      diagnosis.warnings.forEach((issue) => {
        report += `   • ${issue.message}\n`;
      });
      report += '\n';
    }

    // 优化建议
    if (analysis.professional_recommendations.length > 0) {
      report += '💡 **优化建议** (Top 3):\n';
      const topRecs = analysis.professional_recommendations
        .filter((rec) => rec.priority !== 'LOW')
        .slice(0, 3);

      topRecs.forEach((rec, index) => {
        const priorityEmoji =
          rec.priority === 'HIGH'
            ? '🔴'
            : rec.priority === 'MEDIUM'
              ? '🟡'
              : '🔵';
        report += `  ${index + 1}. ${priorityEmoji} [${rec.priority}] ${rec.title}\n`;
        report += `     ${rec.description}\n`;
      });
    }

    report += `\n⏱️  分析耗时: ${analysis.analysis_duration_ms}ms\n`;
    report += `📋 下次检查建议: ${analysis.next_check_interval}\n`;
    report +=
      '\n💡 提示: 建议结合 Grafana 监控 Cache Hit Ratio 趋势图以检测抖动';

    return report;
  }

  /**
   * 获取此专家提供的 MCP 工具定义
   */
  getTools() {
    return [
      {
        name: 'analyze_cache_performance',
        description: `📊 **Data Cache 性能分析** (仅存算分离架构)

**功能**: 分析 StarRocks Shared-Data 架构中 Compute Node 的本地缓存性能，包括命中率、容量使用、抖动检测等。

**诊断内容**:
- ✅ 缓存命中率分析（整体和各节点）
- ✅ 缓存容量使用率监控
- ✅ 节点间命中率差异检测
- ✅ 缓存配置优化建议

**适用场景**:
- 查询性能慢，怀疑缓存命中率低
- 对象存储访问量大，需要优化缓存
- 缓存容量规划和扩容评估
- 定期缓存性能健康检查

**不适用于**:
- ❌ 存算一体架构（无 Data Cache）
- ❌ 磁盘使用率分析（使用 storage_expert_analysis）
- ❌ Compaction 分析（使用 compaction_expert_analysis）

**注意**:
- 当前版本基于单次查询快照，无法检测时序抖动
- 建议结合 Grafana 监控查看 Cache Hit Ratio 趋势图以检测抖动
- 对于抖动分析，需要查看至少 1 小时的时序数据`,
        inputSchema: {
          type: 'object',
          properties: {
            include_details: {
              type: 'boolean',
              description: '是否包含详细的原始指标数据',
              default: true,
            },
          },
          required: [],
        },
      },
    ];
  }
}

export { StarRocksCacheExpert };
