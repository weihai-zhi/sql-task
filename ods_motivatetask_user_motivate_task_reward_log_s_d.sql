-- 创建ODS表
create table if not exists `ods_motivatetask_user_motivate_task_reward_log_s_d` (
  `id` bigint comment '主键'
 ,`task_id` bigint comment '任务id'
 ,`user_id` bigint comment '用户id'
 ,`received` bigint comment '0表示奖励未发放,1表示奖励已经发放'
 ,`user_task_id` bigint comment '用户进度任务id'
 ,`turn` bigint comment '连续任务对应的轮次,从0开始计数'
 ,`attach` string comment '附加信息'
 ,`deleted` boolean
 ,`created` timestamp 
 ,`lastmodified` timestamp 
 ,`reward_snapshot` STRING   COMMENT '奖励快照'
 ,`receive_detail`  string  comment '领取详情'
)
comment '用户任务奖励表'
partitioned by (`ds` string)
lifecycle 3650
;

-- 临时表：获取去重后的增量数据
create temporary table if not exists temp_increment_data as
select *
from (
    select *
          ,row_number() over(partition by id order by lastmodified desc) as rn
    from kdw_ods.stage_motivatetask_user_motivate_task_reward_log_s_d_increment
    where ds = '${bdp.system.bizdate}'
) t
where rn = 1;

-- 直接插入ODS表：跳过stage表，减少中间写入操作
-- 1. 先处理增量表中的数据（新增和更新）
-- 2. 再添加快照表中未被增量表覆盖的数据
insert overwrite table ods_motivatetask_user_motivate_task_reward_log_s_d partition (ds='${bdp.system.bizdate}') 
-- 处理增量数据（新增和更新）
select 
    inc.`id`,
    inc.`task_id`,
    inc.`user_id`,
    inc.`received`,
    inc.`user_task_id`,
    inc.`turn`,
    inc.`attach`,
    inc.`deleted`,
    inc.`created`,
    inc.`lastmodified`,
    inc.`reward_snapshot`,
    inc.`receive_detail`
from temp_increment_data inc

union all

-- 添加快照表中未被增量表覆盖的数据
-- 使用NOT EXISTS的优势：
-- 1. 半连接优化：EXISTS是半连接操作，一旦找到匹配就停止扫描（短路评估）
-- 2. 性能优势：对于60亿行的快照表，短路评估可节省大量时间
-- 3. 内存效率：不需要存储连接后的完整结果集
-- 4. 避免重复：不会因子查询有多个匹配项而导致主表记录重复
-- 5. 语义清晰：直观表达"不存在匹配项"的逻辑
select 
    snp.`id`,
    snp.`task_id`,
    snp.`user_id`,
    snp.`received`,
    snp.`user_task_id`,
    snp.`turn`,
    snp.`attach`,
    snp.`deleted`,
    snp.`created`,
    snp.`lastmodified`,
    snp.`reward_snapshot`,
    snp.`receive_detail`
from kdw_ods.stage_motivatetask_user_motivate_task_reward_log_s_d snp
where ds = '${last1date}'
and not exists (
    select 1 from temp_increment_data inc where inc.id = snp.id
);

-- Spark 2.4优化参数建议
-- 基于DAG执行图的优化配置
-- 1. 执行器配置
--    - 快照表实际扫描5.92亿行（非60亿）
--    - Sort操作峰值内存40.9GB，需要增加executor内存
--    - 优化后避免了99.3GB的大规模shuffle
spark.executor.instances=120
spark.executor.memory=16g
spark.executor.cores=4
spark.driver.memory=8g

-- 2. 分区和并行度配置
--    - 根据实际数据量调整，避免过多小分区
--    - 优化后shuffle数据量大幅减少
spark.sql.shuffle.partitions=1000
spark.default.parallelism=1000

-- 3. Broadcast Join配置
--    - 增量表仅21.1万行，适合广播
--    - 保持100MB阈值即可
spark.sql.autoBroadcastJoinThreshold=104857600
spark.sql.broadcastTimeout=3600

-- 4. Shuffle优化
--    - 启用外部shuffle服务
--    - 增加shuffle文件缓冲区和重试机制
--    - 优化后shuffle量大幅减少
spark.shuffle.service.enabled=true
spark.shuffle.file.buffer=256k
spark.reducer.maxSizeInFlight=96m
spark.shuffle.io.maxRetries=10
spark.shuffle.io.retryWait=60s
spark.shuffle.sort.bypassMergeThreshold=400

-- 5. 内存管理优化
--    - 增加执行内存比例，减少存储内存比例
--    - 针对Sort操作的高内存需求
spark.memory.fraction=0.8
spark.memory.storageFraction=0.1
spark.executor.memoryOverhead=4g

-- 6. Parquet优化
--    - 启用向量化读取和过滤下推
--    - 实际DAG显示使用了ORC格式，保持兼容
spark.sql.hive.convertMetastoreParquet=true
spark.sql.parquet.enableVectorizedReader=true
spark.sql.parquet.filterPushdown=true
spark.sql.parquet.dictionaryFiltering=true
spark.sql.hive.convertMetastoreOrc=true
spark.sql.orc.enableVectorizedReader=true
spark.sql.orc.filterPushdown=true

-- 7. 分区覆盖模式
spark.sql.sources.partitionOverwriteMode=dynamic

-- 8. 其他优化
spark.sql.hive.verifyPartitionPath=true
spark.sql.crossJoin.enabled=true
spark.sql.codegen.wholeStage=true

-- Executor配置调整依据（基于DAG执行图）：
-- 1. 数据规模：快照表实际5.92亿行，增量表21.1万行
-- 2. 内存需求：Sort操作峰值内存40.9GB，需要更大的executor内存
-- 3. Shuffle优化：优化后避免了99.3GB的大规模shuffle
-- 4. 资源使用：120个executor × 16g内存 = 1.92TB总内存，480核
-- 5. 监控重点：
--    - 观察Sort操作的内存使用情况
--    - 监控NOT EXISTS的执行效率
--    - 检查广播join是否生效
-- 6. 调优方向：
--    - 如果内存不足，增加executor.memory或减少executor.instances
--    - 如果CPU利用率低，增加executor.instances
--    - 如果shuffle时间长，调整shuffle参数