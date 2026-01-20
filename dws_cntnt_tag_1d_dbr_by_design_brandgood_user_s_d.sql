-- Spark 2.4 配置参数建议（根据集群资源调整）
-- 执行器配置
-- set spark.executor.instances=100;
-- set spark.executor.cores=4;
-- set spark.executor.memory=16g;
-- set spark.executor.memoryOverhead=4g;

-- 驱动配置
-- set spark.driver.memory=8g;
-- set spark.driver.memoryOverhead=2g;

-- 内存管理配置
-- set spark.memory.fraction=0.7;
-- set spark.memory.storageFraction=0.3;

-- 连接优化配置
-- set spark.sql.autoBroadcastJoinThreshold=104857600; -- 100MB
-- set spark.sql.join.preferSortMergeJoin=false;

-- 并行度配置
-- set spark.sql.shuffle.partitions=400;
-- set spark.default.parallelism=400;

-- 其他性能优化
-- set spark.sql.adaptive.enabled=false; -- Spark 2.4中自适应执行不稳定
-- set spark.sql.codegen.wholeStage=true;
-- set spark.sql.inMemoryColumnarStorage.compressed=true;
-- set spark.sql.inMemoryColumnarStorage.batchSize=10000;

-- 优化建表语句，添加适当的注释和表属性
create table if not exists `dws_cntnt_tag_1d_dbr_by_design_brandgood_user_s_d` (
  `design_id` bigint comment '方案id'
 ,`brandgood_id` BIGINT COMMENT '素材id'
 ,`itemset_type` BIGINT COMMENT '商品库类别，1-模型库、2-全屋定制、0-其它'
 ,`user_id` BIGINT COMMENT '生成方案的用户的usersid'
 ,`province` BIGINT COMMENT '用户所在省'
 ,`province_name` STRING COMMENT '省中文'
 ,`city` BIGINT COMMENT '用户所在城市'
 ,`city_name` STRING COMMENT '市中文'
 ,`account_id` bigint comment '账号id'
 ,`org_user_type` bigint comment '表示该账号的类别:1.商家账号,2.apic账号'
 ,`org_id` bigint comment '组织id'
 ,`fpspec_devide_1` string comment '房型划分1'
 ,`area_devide_1` string comment '面积划分1'
 ,`deleted` BOOLEAN COMMENT '方案是否被删除'
 ,`prodcat1_id` BIGINT COMMENT '素材一级类目id'
 ,`prodcat1_name` STRING COMMENT '素材一级类目名'
 ,`prodcat2_id` BIGINT COMMENT '素材二级类目id'
 ,`prodcat2_name` STRING COMMENT '素材二级类目名'
 ,`prodcat3_id` BIGINT COMMENT '素材三级类目id'
 ,`prodcat3_name` STRING COMMENT '素材三级类目名'
 ,`1d_is_created` boolean comment '是否本日新增'
 ,`1m_is_created` boolean comment '是否本月新增'
 ,`item_type` BIGINT COMMENT '素材类型'
 ,`usable_bg_source` STRING COMMENT '素材使用来源，自建、授权、公库'
 ,`src_org_id` bigint comment '授权供给方组织id'
 ,`deco_cat1_id` BIGINT COMMENT '硬装一级类目id'
 ,`deco_cat1_name` STRING COMMENT '硬装一级类目名'
 ,`deco_cat2_id` BIGINT COMMENT '硬装二级类目id'
 ,`deco_cat2_name` STRING COMMENT '硬装二级类目名'
)
 partitioned by (`ds` string)
lifecycle 3650
;


 
 
-- 优化后的SQL，减少资源利用

WITH 
-- 1. 只读取一次大表dws_cntnt_1d_dbr_change_by_design_brandgood_s_d（260亿行），并缓存结果
current_dbr AS (
    SELECT design_id, brandgood_id, dhd_type 
    FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d 
    WHERE ds = '${bdp.system.bizdate}'
),

-- 2. 预计算dbr的itemset_type和transprod_cat_id
processed_dbr AS (
    SELECT 
        dbr.design_id, 
        dbr.brandgood_id, 
        dbr.dhd_type, 
        CASE 
            WHEN dbr.dhd_type='furniture' THEN 1 
            WHEN dbr.dhd_type='customized' AND bg.item_type IN (3,8,9,14,15,19,24) THEN 2 
            WHEN dbr.dhd_type='decoration' AND dc.deco_cat2_id IS NOT NULL THEN 3 
            ELSE 0 
        END AS itemset_type, 
        bg.transprod_cat_id 
    FROM current_dbr dbr
    INNER JOIN (
        SELECT brandgood_id, item_type, transprod_cat_id 
        FROM kdw_dw.dwd_cntnt_brandgood_s_d 
        WHERE ds = '${bdp.system.bizdate}' 
    ) bg ON dbr.brandgood_id = bg.brandgood_id
    LEFT JOIN (
        SELECT CAST(CASE WHEN cat2_id IS NULL THEN cat1_id ELSE cat2_id END AS BIGINT) AS deco_cat2_id 
        FROM kdw_dw.dim_cntnt_deco_category_cockpit_s_d 
        WHERE ds = '${bdp.system.bizdate}' 
    ) dc ON bg.transprod_cat_id = dc.deco_cat2_id
),

-- 3. 处理design相关信息，只选择需要的列
processed_design AS (
    SELECT 
        d.design_id, 
        d.user_id, 
        d.deleted, 
        d.fpspec_devide_1, 
        d.area_devide_1, 
        u.province, 
        u.province_name, 
        u.city, 
        u.city_name, 
        acc.account_id, 
        acc.org_user_type, 
        acc.org_id 
    FROM (
        SELECT design_id, user_id, fpspec_devide_1, area_devide_1, deleted 
        FROM kdw_dw.dwd_cntnt_project_design_s_d 
        WHERE ds = '${bdp.system.bizdate}'
        -- 只保留与当前dbr相关的design，减少6亿行表的处理量
        AND design_id IN (SELECT DISTINCT design_id FROM current_dbr)
    ) d
    INNER JOIN (
        SELECT account_id, org_id, user_id, org_user_type 
        FROM kdw_dw.dim_usr_org_user_s_d 
        WHERE ds = '${bdp.system.bizdate}' 
    ) acc ON d.user_id = acc.user_id
    INNER JOIN (
        SELECT user_id, 
               COALESCE(province, -1) AS province, 
               COALESCE(province_name, '') AS province_name, 
               COALESCE(city, -1) AS city, 
               COALESCE(city_name, '') AS city_name 
        FROM kdw_dw.dwb_usr_user_s_d 
        WHERE ds = '${bdp.system.bizdate}' 
    ) u ON d.user_id = u.user_id
),

-- 4. 处理brandgood信息，只选择需要的列
processed_brandgood AS (
    SELECT 
        brandgood_id, 
        item_type, 
        COALESCE(prodcat1_id, -1) AS prodcat1_id, 
        COALESCE(prodcat1_name, '') AS prodcat1_name, 
        COALESCE(prodcat2_id, -1) AS prodcat2_id, 
        COALESCE(prodcat2_name, '') AS prodcat2_name, 
        COALESCE(prodcat3_id, -1) AS prodcat3_id, 
        COALESCE(prodcat3_name, '') AS prodcat3_name 
    FROM kdw_dw.dwd_cntnt_brandgood_s_d 
    WHERE ds = '${bdp.system.bizdate}'
    -- 只保留与当前dbr相关的brandgood
    AND brandgood_id IN (SELECT DISTINCT brandgood_id FROM current_dbr)
),

-- 5. 处理usable_bg_source信息，避免笛卡尔积
processed_usable AS (
    SELECT 
        org_id, 
        brandgood_id, 
        CASE 
            WHEN authorization_method=0 THEN '自建' 
            WHEN authorization_method IN (1,2,3) THEN '授权' 
            ELSE '公库' 
        END AS usable_bg_source, 
        src_org_id 
    FROM kdw_dw.dwb_cntnt_org_usable_brandgood_s_d 
    WHERE ds = '${bdp.system.bizdate}'
    -- 直接过滤可能用到的org_id和brandgood_id，避免笛卡尔积
    AND org_id IN (SELECT DISTINCT org_id FROM processed_design) 
    AND brandgood_id IN (SELECT DISTINCT brandgood_id FROM processed_brandgood)
),

-- 6. 准备对比日期的数据
lastdate_dbr AS (
    SELECT design_id, brandgood_id, dhd_type
    FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d
    WHERE ds = date_format(date_sub(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), 1), 'yyyyMMdd')
),

-- 7. 准备上月最后一天的数据
lastmonth_dbr AS (
    SELECT design_id, brandgood_id, dhd_type
    FROM kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d
    WHERE ds = date_format(last_day(add_months(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), -1)), 'yyyyMMdd')
)

insert overwrite table dws_cntnt_tag_1d_dbr_by_design_brandgood_user_s_d partition (ds='${bdp.system.bizdate}') 

-- 主查询
SELECT 
     pdbr.design_id 
    ,pdbr.brandgood_id 
    ,pdbr.itemset_type 
    ,pd.user_id 
    ,pd.province 
    ,pd.province_name 
    ,pd.city 
    ,pd.city_name 
    ,pd.account_id 
    ,pd.org_user_type 
    ,pd.org_id 
    ,pd.fpspec_devide_1 
    ,pd.area_devide_1 
    ,pd.deleted 
    ,pb.prodcat1_id 
    ,pb.prodcat1_name 
    ,pb.prodcat2_id 
    ,pb.prodcat2_name 
    ,pb.prodcat3_id 
    ,pb.prodcat3_name 
    -- 使用LEFT JOIN替代NOT EXISTS，符合Spark SQL语法规范
    ,CASE WHEN ld.design_id IS NULL THEN TRUE ELSE FALSE END AS `1d_is_created` 
    ,CASE WHEN lmd.design_id IS NULL THEN TRUE ELSE FALSE END AS `1m_is_created` 
    ,pb.item_type 
    ,pu.usable_bg_source 
    ,pu.src_org_id 
    ,deco.deco_cat1_id 
    ,deco.deco_cat1_name 
    ,deco.deco_cat2_id
    ,deco.deco_cat2_name 

FROM processed_dbr pdbr
INNER JOIN processed_design pd ON pdbr.design_id = pd.design_id
INNER JOIN processed_brandgood pb ON pdbr.brandgood_id = pb.brandgood_id

-- 对比昨天的数据，检查是否为新增
LEFT JOIN lastdate_dbr ld ON pdbr.design_id = ld.design_id 
                          AND pdbr.brandgood_id = ld.brandgood_id 
                          AND pdbr.dhd_type = ld.dhd_type

-- 对比上月最后一天的数据，检查是否为新增
LEFT JOIN lastmonth_dbr lmd ON pdbr.design_id = lmd.design_id 
                           AND pdbr.brandgood_id = lmd.brandgood_id 
                           AND pdbr.dhd_type = lmd.dhd_type

-- 硬装分类信息，只在需要时关联，并确保类型转换正确
LEFT JOIN (
    SELECT 
        CAST(CASE WHEN cat2_id IS NULL THEN cat1_id ELSE cat2_id END AS BIGINT) AS deco_cat2_id, 
        CAST(cat1_id AS BIGINT) AS deco_cat1_id, 
        cat1_name AS deco_cat1_name, 
        CASE WHEN cat2_name IS NULL THEN cat1_name ELSE cat2_name END AS deco_cat2_name 
    FROM kdw_dw.dim_cntnt_deco_category_cockpit_s_d 
    WHERE ds = '${bdp.system.bizdate}' 
) deco ON pdbr.transprod_cat_id = deco.deco_cat2_id 
       AND pdbr.dhd_type = 'decoration'

-- 素材使用来源信息
LEFT JOIN processed_usable pu 
    ON pu.org_id = pd.org_id 
    AND pu.brandgood_id = pb.brandgood_id;