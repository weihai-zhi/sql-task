-- ========================================
-- Spark参数优化配置指南
-- 针对ka_customer_tishen_brandgood_order_s_d表优化
-- 基于实际数据量和集群资源配置
-- ========================================

-- ========================================
-- 1. 核心内存和并行度参数
-- ========================================

-- 基础并行度设置（根据集群Executor数量调整）
-- 公式：并行度 = Executor数量 × 每个Executor的core数 × 2-3
SET spark.sql.shuffle.partitions = 400; -- 建议值：200-800，根据集群规模调整

-- 广播Join阈值（控制小表广播）
SET spark.sql.autoBroadcastJoinThreshold = 209715200; -- 200MB，适合中小维度表

-- 自适应查询执行（AQE）- 必须开启
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true; -- 处理数据倾斜
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 134217728; -- 128MB

-- ========================================
-- 2. 内存管理参数
-- ========================================

-- Executor内存配置（根据集群实际情况调整）
-- 建议每个Executor 8-16GB内存
-- spark.executor.memory = 12g

-- Driver内存配置
-- spark.driver.memory = 4g

-- 内存比例优化
SET spark.memory.fraction = 0.6; -- 执行内存占比
SET spark.memory.storageFraction = 0.5; -- 存储内存占比

-- 堆外内存（如果集群支持）
SET spark.memory.offHeap.enabled = true;
SET spark.memory.offHeap.size = 2g;

-- ========================================
-- 3. 数据倾斜处理参数
-- ========================================

-- 倾斜Join优化
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256000000; -- 256MB

-- 倾斜分区重分区
SET spark.sql.adaptive.forceApply = true;

-- ========================================
-- 4. Shuffle优化参数
-- ========================================

-- Shuffle文件压缩
SET spark.shuffle.compress = true;
SET spark.shuffle.spill.compress = true;

-- Shuffle分区数动态调整
SET spark.sql.adaptive.localShuffleReader.enabled = true;

-- Shuffle服务（如果集群开启）
SET spark.shuffle.service.enabled = true;

-- ========================================
-- 5. 序列化和压缩参数
-- ========================================

-- 序列化格式
SET spark.serializer = org.apache.spark.serializer.KryoSerializer;

-- 压缩算法
SET spark.sql.parquet.compression.codec = snappy;
SET spark.sql.orc.compression.codec = snappy;

-- ========================================
-- 6. 针对具体SQL的优化参数
-- ========================================

-- 针对您的SQL特点优化：
-- 1. 大表Join小表场景
SET spark.sql.autoBroadcastJoinThreshold = 209715200; -- 200MB

-- 2. 窗口函数优化
SET spark.sql.windowExec.buffer.spill.threshold = 10000;

-- 3. 分区表优化
SET spark.sql.hive.convertMetastoreParquet = true;
SET spark.sql.hive.convertMetastoreOrc = true;

-- ========================================
-- 7. 执行计划优化参数
-- ========================================

-- Join策略优化
SET spark.sql.join.preferSortMergeJoin = false; -- 优先使用BroadcastJoin
SET spark.sql.autoBroadcastJoinThreshold = 209715200;

-- 子查询优化
SET spark.sql.subquery.reuse = true;

-- 公共表达式消除
SET spark.sql.exchange.reuse = true;

-- ========================================
-- 8. 监控和调试参数
-- ========================================

-- 执行计划输出
SET spark.sql.executionPlanOnly = false;

-- 指标收集
SET spark.eventLog.enabled = true;

-- 动态分配（如果集群支持）
SET spark.dynamicAllocation.enabled = true;
SET spark.dynamicAllocation.minExecutors = 2;
SET spark.dynamicAllocation.maxExecutors = 20;
SET spark.dynamicAllocation.initialExecutors = 4;

-- ========================================
-- 9. 针对不同集群规模的参数建议
-- ========================================

/*
-- 小型集群（<50个Executor）
SET spark.sql.shuffle.partitions = 200;
SET spark.sql.autoBroadcastJoinThreshold = 104857600; -- 100MB
-- spark.executor.memory = 8g
-- spark.executor.cores = 2

-- 中型集群（50-200个Executor）
SET spark.sql.shuffle.partitions = 400;
SET spark.sql.autoBroadcastJoinThreshold = 209715200; -- 200MB
-- spark.executor.memory = 12g
-- spark.executor.cores = 4

-- 大型集群（>200个Executor）
SET spark.sql.shuffle.partitions = 800;
SET spark.sql.autoBroadcastJoinThreshold = 536870912; -- 512MB
-- spark.executor.memory = 16g
-- spark.executor.cores = 8
*/

-- ========================================
-- 10. 实际执行时的参数设置示例
-- ========================================

-- 针对您的SQL，推荐的完整参数设置：
SET spark.sql.shuffle.partitions = 400;
SET spark.sql.autoBroadcastJoinThreshold = 209715200;
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 134217728;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256000000;
SET spark.sql.join.preferSortMergeJoin = false;
SET spark.sql.subquery.reuse = true;
SET spark.sql.exchange.reuse = true;
SET spark.serializer = org.apache.spark.serializer.KryoSerializer;
SET spark.sql.parquet.compression.codec = snappy;

-- ========================================
-- 11. 性能监控SQL
-- ========================================

/*
-- 执行前检查集群资源
SELECT * FROM system.runtime.nodes;

-- 监控SQL执行情况
SELECT query_id, state, user, created, elapsed 
FROM system.queries 
WHERE state = 'RUNNING' 
ORDER BY created DESC;

-- 检查数据倾斜情况
SELECT * FROM (
    SELECT 
        design_id,
        COUNT(*) as cnt
    FROM kdw_ods.ods_dcsproduce_order_design_s_d 
    WHERE ds = '${ds}' 
    GROUP BY design_id 
    ORDER BY cnt DESC 
    LIMIT 10
) t;

-- 检查分区数据分布
SELECT ds, COUNT(*) 
FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d 
GROUP BY ds 
ORDER BY ds DESC;
*/

-- ========================================
-- 12. 参数调优建议
-- ========================================

/*
1. 并行度调优：
   - 从200开始，逐步增加到400、800
   - 观察任务执行时间和资源利用率
   - 目标：CPU利用率80%以上

2. 广播阈值调优：
   - 小表<200MB：设置为200MB
   - 小表<500MB：设置为512MB
   - 观察Join类型是否变为BroadcastJoin

3. 内存调优：
   - Executor内存：8-16GB
   - 避免GC频繁，监控GC时间
   - 如果OOM，减少并行度或增加内存

4. 数据倾斜处理：
   - 开启skewJoin优化
   - 检查倾斜字段分布
   - 考虑对倾斜字段加盐处理

5. 监控指标：
   - 任务完成时间
   - 资源利用率
   - GC时间占比
   - Shuffle数据量
*/