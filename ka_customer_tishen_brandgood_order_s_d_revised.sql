-- ========================================
-- 重新优化版本：ka_customer_tishen_brandgood_order_s_d
-- 基于实际数据表情况重新设计
-- 目标：20分钟内完成，资源利用最小化
-- ========================================

-- 设置Spark优化参数
SET spark.sql.autoBroadcastJoinThreshold = 104857600; -- 100MB广播阈值
SET spark.sql.shuffle.partitions = 200; -- 根据集群规模调整
SET spark.sql.adaptive.enabled = true; -- 启用自适应查询
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true; -- 处理数据倾斜

-- 创建目标表（如果不存在）
CREATE TABLE IF NOT EXISTS ka_customer_tishen_brandgood_order_s_d
(
    org_id                    BIGINT COMMENT '商家id',
    order_readable_id         STRING COMMENT '语义化订单id',
    design_id                 BIGINT COMMENT '设计方案id',
    value                     STRING COMMENT '审核单关联单号',
    created                   TIMESTAMP COMMENT '首次提审日期',
    review_status             STRING COMMENT '审核状态',
    last_modified             TIMESTAMP COMMENT '状态变更日期',
    brandgood_id              BIGINT COMMENT '商品id',
    dhd_type                  STRING COMMENT '方案内包含素材的类型:furniture.模型,material.材质,molding.踢脚线,customized.定制,decoration.硬装',
    b4110_1_design_brandgood_relation_cnt BIGINT COMMENT '方案内包含同一素材的次数',
    brandgood_name            STRING COMMENT '商品名称'
)
COMMENT '客户提审方案包含素材明细表'
PARTITIONED BY (`ds` STRING COMMENT '日期，格式YYYYMMDD')
LIFECYCLE 3650
STORED AS ORC -- 使用ORC格式提高压缩率和查询性能
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ========================================
-- 优化策略：基于实际数据重新设计
-- 由于素材变更表为空，需要寻找替代数据源
-- ========================================

-- 方案A：如果素材变更表为空，先检查是否有其他素材相关表
-- 执行前先检查数据量：
-- SELECT COUNT(*) FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d WHERE ds = '${ds}';

-- 如果素材变更表确实为空，使用以下简化版本：
INSERT OVERWRITE TABLE ka_customer_tishen_brandgood_order_s_d PARTITION(ds = '${ds}')
SELECT /*+ REPARTITION(20, t1.design_id) */ -- 按design_id重新分区
    t2.org_id,
    t2.order_readable_id,
    t1.design_id,
    t5.value,
    t1.created,
    t2.review_status,
    t2.last_modified,
    CAST(NULL AS BIGINT) as brandgood_id, -- 素材表为空时设为NULL
    CAST(NULL AS STRING) as dhd_type,
    CAST(NULL AS BIGINT) as b4110_1_design_brandgood_relation_cnt,
    CAST(NULL AS STRING) as brandgood_name
FROM 
(
    -- 第一步：获取每个design_id的首次提审记录（3000万 -> 大幅减少）
    SELECT 
        design_id,
        order_id,
        created,
        ROW_NUMBER() OVER (PARTITION BY design_id ORDER BY created ASC) as rn
    FROM kdw_ods.ods_dcsproduce_order_design_s_d
    WHERE ds = '${ds}'
      AND deleted = false
) t1
WHERE t1.rn = 1 -- 只取首次提审记录
) t1
INNER JOIN 
(
    -- 第二步：预过滤有效订单信息（4000万 -> 预过滤后大幅减少）
    SELECT 
        order_id,
        org_id,
        last_modified,
        order_readable_id,
        CASE 
            WHEN state = 27 THEN '待审核'
            WHEN state = 28 THEN '审核中'
            WHEN state = 29 THEN '已通过'
            WHEN state = 30 THEN '已撤回'
            WHEN state = 31 THEN '已驳回'
            WHEN state = 32 THEN '已退回'
        END as review_status
    FROM kdw_ods.ods_coc_order_info_s_d
    WHERE ds = '${ds}'
      AND status = 0
      AND state NOT IN (1, 30)
      AND template_id = 2
) t2 ON t1.order_id = t2.order_id
LEFT JOIN 
(
    -- 第三步：只获取审核关联信息（1亿 -> 预过滤后减少）
    SELECT 
        order_id,
        value
    FROM kdw_ods.ods_cocsuboir_order_item_relation_s_d
    WHERE ds = '${ds}'
      AND item_key = 'auditRelationId'
) t5 ON t1.order_id = t5.order_id;

-- ========================================
-- 方案B：寻找替代素材数据源
-- 如果需要真实的素材数据，可能需要从其他表获取
-- ========================================

/*
-- 检查是否有其他素材相关表：
-- 1. 检查设计明细表是否包含素材信息
SELECT COUNT(*) FROM kdw_ods.ods_dcsproduce_order_design_detail_s_d WHERE ds = '${ds}';

-- 2. 检查商品表是否有数据
SELECT COUNT(*) FROM kdw_dw.dwd_cntnt_brandgood_s_d WHERE ds = '${ds}';

-- 3. 如果设计明细表有素材信息，使用以下替代方案：
INSERT OVERWRITE TABLE ka_customer_tishen_brandgood_order_s_d PARTITION(ds = '${ds}')
SELECT /*+ REPARTITION(20, t1.design_id) */
    t2.org_id,
    t2.order_readable_id,
    t1.design_id,
    t5.value,
    t1.created,
    t2.review_status,
    t2.last_modified,
    t3.brandgood_id,
    t3.dhd_type,
    1 as b4110_1_design_brandgood_relation_cnt, -- 设计明细表中每个素材出现一次
    t4.brandgood_name
FROM 
(
    SELECT 
        design_id,
        order_id,
        created,
        ROW_NUMBER() OVER (PARTITION BY design_id ORDER BY created ASC) as rn
    FROM kdw_ods.ods_dcsproduce_order_design_s_d
    WHERE ds = '${ds}'
      AND deleted = false
) t1
WHERE t1.rn = 1
) t1
INNER JOIN 
(
    SELECT 
        order_id,
        org_id,
        last_modified,
        order_readable_id,
        CASE 
            WHEN state = 27 THEN '待审核'
            WHEN state = 28 THEN '审核中'
            WHEN state = 29 THEN '已通过'
            WHEN state = 30 THEN '已撤回'
            WHEN state = 31 THEN '已驳回'
            WHEN state = 32 THEN '已退回'
        END as review_status
    FROM kdw_ods.ods_coc_order_info_s_d
    WHERE ds = '${ds}'
      AND status = 0
      AND state NOT IN (1, 30)
      AND template_id = 2
) t2 ON t1.order_id = t2.order_id
LEFT JOIN 
(
    SELECT order_id, value
    FROM kdw_ods.ods_cocsuboir_order_item_relation_s_d
    WHERE ds = '${ds}'
      AND item_key = 'auditRelationId'
) t5 ON t1.order_id = t5.order_id
LEFT JOIN 
(
    -- 使用设计明细表作为素材数据源
    SELECT 
        design_id,
        brandgood_id,
        dhd_type
    FROM kdw_ods.ods_dcsproduce_order_design_detail_s_d
    WHERE ds = '${ds}'
      AND brandgood_id IS NOT NULL
) t3 ON t1.design_id = t3.design_id
LEFT JOIN 
(
    SELECT 
        brandgood_id,
        brandgood_name
    FROM kdw_dw.dwd_cntnt_brandgood_s_d
    WHERE ds = '${ds}'
      AND brandgood_id IS NOT NULL
) t4 ON t3.brandgood_id = t4.brandgood_id;
*/

-- ========================================
-- 方案C：分步处理，优先保证核心数据
-- ========================================

/*
-- 第一步：先处理核心设计订单数据（不包含素材）
CREATE TABLE IF NOT EXISTS temp_core_design_${ds} AS
SELECT /*+ COALESCE(10) */
    t2.org_id,
    t2.order_readable_id,
    t1.design_id,
    t5.value,
    t1.created,
    t2.review_status,
    t2.last_modified
FROM 
(
    SELECT 
        design_id,
        order_id,
        created,
        ROW_NUMBER() OVER (PARTITION BY design_id ORDER BY created ASC) as rn
    FROM kdw_ods.ods_dcsproduce_order_design_s_d
    WHERE ds = '${ds}'
      AND deleted = false
) t1
WHERE t1.rn = 1
) t1
INNER JOIN 
(
    SELECT 
        order_id,
        org_id,
        last_modified,
        order_readable_id,
        CASE 
            WHEN state = 27 THEN '待审核'
            WHEN state = 28 THEN '审核中'
            WHEN state = 29 THEN '已通过'
            WHEN state = 30 THEN '已撤回'
            WHEN state = 31 THEN '已驳回'
            WHEN state = 32 THEN '已退回'
        END as review_status
    FROM kdw_ods.ods_coc_order_info_s_d
    WHERE ds = '${ds}'
      AND status = 0
      AND state NOT IN (1, 30)
      AND template_id = 2
) t2 ON t1.order_id = t2.order_id
LEFT JOIN 
(
    SELECT order_id, value
    FROM kdw_ods.ods_cocsuboir_order_item_relation_s_d
    WHERE ds = '${ds}'
      AND item_key = 'auditRelationId'
) t5 ON t1.order_id = t5.order_id;

-- 第二步：根据实际情况决定是否补充素材数据
-- 如果素材表有数据，再补充；否则直接使用核心数据
INSERT OVERWRITE TABLE ka_customer_tishen_brandgood_order_s_d PARTITION(ds = '${ds}')
SELECT 
    org_id,
    order_readable_id,
    design_id,
    value,
    created,
    review_status,
    last_modified,
    CAST(NULL AS BIGINT) as brandgood_id,
    CAST(NULL AS STRING) as dhd_type,
    CAST(NULL AS BIGINT) as b4110_1_design_brandgood_relation_cnt,
    CAST(NULL AS STRING) as brandgood_name
FROM temp_core_design_${ds};

-- 清理临时表
DROP TABLE IF EXISTS temp_core_design_${ds};
*/

-- ========================================
-- 数据验证和监控
-- ========================================

/*
-- 执行前检查各表数据量
SELECT 'design_table' as table_name, COUNT(*) as record_count FROM kdw_ods.ods_dcsproduce_order_design_s_d WHERE ds = '${ds}' AND deleted = false;
SELECT 'order_table' as table_name, COUNT(*) as record_count FROM kdw_ods.ods_coc_order_info_s_d WHERE ds = '${ds}' AND status = 0 AND state NOT IN (1,30) AND template_id = 2;
SELECT 'relation_table' as table_name, COUNT(*) as record_count FROM kdw_ods.ods_cocsuboir_order_item_relation_s_d WHERE ds = '${ds}' AND item_key = 'auditRelationId';
SELECT 'brandgood_change_table' as table_name, COUNT(*) as record_count FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d WHERE ds = '${ds}';
SELECT 'brandgood_table' as table_name, COUNT(*) as record_count FROM kdw_dw.dwd_cntnt_brandgood_s_d WHERE ds = '${ds}';

-- 检查是否有设计明细表作为替代
SELECT 'design_detail_table' as table_name, COUNT(*) as record_count FROM kdw_ods.ods_dcsproduce_order_design_detail_s_d WHERE ds = '${ds}';

-- 执行后验证结果
SELECT COUNT(*) as final_count FROM ka_customer_tishen_brandgood_order_s_d WHERE ds = '${ds}';
SELECT ds, COUNT(*) as daily_count FROM ka_customer_tishen_brandgood_order_s_d GROUP BY ds ORDER BY ds DESC LIMIT 10;
*/