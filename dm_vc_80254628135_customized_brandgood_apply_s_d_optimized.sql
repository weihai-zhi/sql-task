-- Spark SQL 3.2 优化版本
-- 创建时间: 2025-09-30
-- 优化目标: 最小化资源利用，保持合理执行时间

-- Spark配置参数建议
-- spark.sql.autoBroadcastJoinThreshold = 10485760 (10MB)
-- spark.sql.shuffle.partitions = 200
-- spark.sql.adaptive.enabled = true
-- spark.sql.adaptive.coalescePartitions.enabled = true
-- spark.sql.adaptive.localShuffleReader.enabled = true
-- spark.sql.inMemoryColumnarStorage.compressed = true
-- spark.sql.inMemoryColumnarStorage.batchSize = 10000
-- spark.serializer = org.apache.spark.serializer.KryoSerializer

-- 建表语句保持不变
CREATE TABLE IF NOT EXISTS dm_vc_80254628135_customized_brandgood_apply_s_d (
     `brandgood_id`                        BIGINT      COMMENT '素材id'
     ,`brandgood_name`                     STRING      COMMENT '素材名称'
     ,`brandgood_pic`                      STRING      COMMENT '素材图片'
     ,`brandgood_created` TIMESTAMP        COMMENT '素材创建时间'
     ,`brandgood_lastmodified` TIMESTAMP   COMMENT '素材修改时间'
     ,`visible`                            BOOLEAN     COMMENT '是否工具内显示'
     ,`cat_id`	                            BIGINT	    COMMENT '类目id'
     ,`account_id`	                        BIGINT	    COMMENT 'accountid'
     ,`root_account_id`	                BIGINT	    COMMENT '主账号'
     ,`account_name`	                    STRING	    COMMENT '账户名称'
     ,`account_fullname`	                STRING	    COMMENT '客户全称'
     ,`cat_name`	                        STRING	    COMMENT '类目名称'
     ,`cat1_name`	                        STRING	    COMMENT '一级类目名'
     ,`cat2_name`	                        STRING	    COMMENT '二级类目名'
     ,`cat3_name`	                        STRING	    COMMENT '三级类目名'
     ,`cat4_name`	                        STRING	    COMMENT '四级类目名'
     ,`cat5_name`	                        STRING	    COMMENT '五级类目名'
     ,`is_test_cate`                       BOOLEAN	    COMMENT '是否测试类目'
     ,`vc_itemset`	                        STRING	    COMMENT '模型库，指商家后台企业库的模型库名称'
     ,`vc_itemset_tab`	                    STRING	    COMMENT '工具线，指商家后台企业库的tab名称'
     ,`vc_itemset_type_chs`	            STRING	    COMMENT '模型库类型中文'
     ,`public_type`                        STRING	    COMMENT '公私库类型'

     ,`design_id`                          BIGINT      COMMENT '方案id'
     ,`design_created` TIMESTAMP           COMMENT '方案创建时间'
     ,`design_lastmodified` TIMESTAMP       COMMENT '方案修改时间'
     ,`design_community_name`              STRING      COMMENT '方案小区名称'
     ,`design_community_province_name`     STRING      COMMENT '方案小区所在省'
     ,`design_community_city_name`         STRING      COMMENT '方案小区所在城市'

     ,`design_user_id`                     BIGINT      COMMENT '方案用户id'
     ,`design_user_name`                   STRING      COMMENT '方案用户名称'
     ,`design_user_province_name`          STRING      COMMENT '方案用户所在省'
     ,`design_user_city_name`              STRING      COMMENT '方案用户所在城市'
     ,`fpspec_devide_1`                    STRING      COMMENT '方案房型划分1'
     ,`area_devide_1`                      STRING      COMMENT '方案面积划分1'

     ,`relation_design_id`                 BIGINT      COMMENT '关联方案id，包含已经删除的方案，用做联合主键'
)
PARTITIONED BY (`ds` STRING)
LIFECYCLE 3650;

-- 优化后的插入语句
INSERT OVERWRITE TABLE dm_vc_80254628135_customized_brandgood_apply_s_d PARTITION (ds='${bdp.system.bizdate}') 
WITH 
-- 优化1: 只选择需要的字段，避免SELECT *
brandgood_filtered AS (
    SELECT 
        brandgood_id,
        brandgood_name,
        brandgood_pic,
        brandgood_created,
        brandgood_lastmodified,
        visible,
        cat_id,
        account_id,
        root_account_id,
        account_name,
        account_fullname,
        cat_name,
        cat1_name,
        cat2_name,
        cat3_name,
        cat4_name,
        cat5_name,
        is_test_cate,
        vc_itemset,
        vc_itemset_tab,
        vc_itemset_type_chs,
        public_type
    FROM kdw_prj.dm_vc_80254628135_customized_brandgood_created_s_d 
    WHERE ds = '${bdp.system.bizdate}'
),

-- 优化2: 预过滤设计素材关系表，减少数据量
design_brandgood_relation_filtered AS (
    SELECT 
        design_id,
        brandgood_id
    FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d 
    WHERE ds = '${bdp.system.bizdate}'
    GROUP BY design_id, brandgood_id
),

-- 优化3: 预过滤设计表，只选择有效记录
design_filtered AS (
    SELECT 
        design_id,
        created,
        lastmodified,
        community_name,
        comm_logic_area_id,
        comm_logic_province_id,
        user_id,
        user_name,
        province_name,
        city_name,
        fpspec_devide_1,
        area_devide_1,
        root_account_id
    FROM kdw_dw.dwd_cntnt_project_design_s_d 
    WHERE ds = '${bdp.system.bizdate}' 
    AND deleted = false
    AND root_account_id IN (106940, 1126425)  -- 诗尼曼主账号，提前过滤
),

-- 优化4: 预过滤地区字典表，使用广播变量
area_dict_filtered AS (
    SELECT 
        area_id,
        name,
        'city' as area_type
    FROM kdw_dw.dim_pub_sysdictarea_s_d 
    WHERE ds = '${bdp.system.bizdate}'
),

province_dict_filtered AS (
    SELECT 
        area_id,
        name,
        'province' as area_type
    FROM kdw_dw.dim_pub_sysdictarea_s_d 
    WHERE ds = '${bdp.system.bizdate}'
)

-- 主查询
SELECT /*+ REPARTITION(200) BROADCAST(comm_logic_area, comm_logic_province) */
    bg.brandgood_id,
    bg.brandgood_name,
    bg.brandgood_pic,
    bg.brandgood_created,
    bg.brandgood_lastmodified,
    bg.visible,
    bg.cat_id,
    bg.account_id,
    bg.root_account_id,
    bg.account_name,
    bg.account_fullname,
    bg.cat_name,
    bg.cat1_name,
    bg.cat2_name,
    bg.cat3_name,
    bg.cat4_name,
    bg.cat5_name,
    bg.is_test_cate,
    bg.vc_itemset,
    bg.vc_itemset_tab,
    bg.vc_itemset_type_chs,
    bg.public_type,
    
    d.design_id,
    d.created AS design_created,
    d.lastmodified AS design_lastmodified,
    d.community_name AS design_community_name,
    prov.name AS design_community_province_name,
    city.name AS design_community_city_name,
    
    d.user_id AS design_user_id,
    d.user_name AS design_user_name,
    d.province_name AS design_user_province_name,
    d.city_name AS design_user_city_name,
    d.fpspec_devide_1,
    d.area_devide_1,
    
    COALESCE(dbr.design_id, -1) AS relation_design_id
FROM brandgood_filtered bg
LEFT OUTER JOIN design_brandgood_relation_filtered dbr 
    ON bg.brandgood_id = dbr.brandgood_id
LEFT OUTER JOIN design_filtered d 
    ON dbr.design_id = d.design_id
LEFT OUTER JOIN area_dict_filtered city 
    ON d.comm_logic_area_id = city.area_id
LEFT OUTER JOIN province_dict_filtered prov 
    ON d.comm_logic_province_id = prov.area_id
WHERE d.design_id IS NOT NULL 
    AND bg.root_account_id = d.root_account_id;

-- 优化说明:
-- 1. 避免SELECT *，明确指定所需字段
-- 2. 使用CTE (WITH子句)提高可读性和性能
-- 3. 提前过滤数据，减少join的数据量
-- 4. 使用广播join提示优化小表join
-- 5. 添加repartition提示控制shuffle分区数
-- 6. 在设计表中提前过滤root_account_id条件
-- 7. 为地区字典表创建单独的CTE，便于优化器处理