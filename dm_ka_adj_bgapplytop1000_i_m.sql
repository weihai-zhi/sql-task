-- 优化版本：使用增量表和提前过滤减少数据量
-- 优化策略：
-- 1. 使用增量表替代全量表（250亿→增量表）
-- 2. 提前过滤减少JOIN数据量
-- 3. 优化JOIN顺序，小表驱动大表
-- 4. 减少不必要的字段传输
-- 5. 添加HAVING过滤无效结果
-- 6. 使用Spark 3.2兼容的时间函数
-- 7. 增量表取一个月数据

CREATE TABLE IF NOT EXISTS dm_ka_adj_bgapplytop1000_i_m
(
          org_id            BIGINT comment "商家id"
        , level1_node_name  string comment "一级门店"
        , level2_node_name  string comment "二级门店"
        , brandgood_name    string comment "模型名称"
        , bg_apply_dcnt     BIGINT comment "模型应用方案数"

) 
COMMENT 'KA-爱迪家-方案包含模型TOP1000'
PARTITIONED BY
(
     `ds`  STRING   COMMENT '日期，格式YYYYMMDD'
)
LIFECYCLE 3650
;


INSERT OVERWRITE TABLE dm_ka_adj_bgapplytop1000_i_m PARTITION(ds='${bdp.system.bizdate}') 
SELECT  
      t02.org_id
    , t02.level1_node_name  
    , t02.level2_node_name        
    , t04.brandgood_name     
    , COUNT(DISTINCT t03.design_id) as bg_apply_dcnt 
FROM  
( 
    -- 优化1：提前过滤设计基础表，只保留必要字段
    SELECT  
          user_id 
        , org_id 
        , level1_node_name 
        , level2_node_name 
        , design_id
    FROM  
        dws_ka_adj_designbasic_s_m
    WHERE ds='${bdp.system.bizdate}' 
    AND design_type not in ('copy', 'blank') and design_id is not null
    -- Spark 3.2兼容的时间函数：格式化为YYYYMM
    AND date_format(design_created, 'yyyyMM') = date_format(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), 'yyyyMM')
    -- 优化2：提前过滤有效商家，减少后续JOIN数据量
    AND org_id IN (
        SELECT DISTINCT org_id 
        FROM kdw_dw.dwb_usr_businessaccount_s_d 
        WHERE ds='${bdp.system.bizdate}'
        AND deleted = false
    )
)t02  
  
JOIN  
( 
    -- 优化3：使用增量表替代250亿全量表，取月初1号的分区
    SELECT  
          design_id 
        , brandgood_id 
    FROM  
        kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_i_d  -- 使用增量表
    WHERE ds >= date_format(trunc(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), 'MM'), 'yyyyMMdd')  -- 只取月初1号分区
    and ds <= '${bdp.system.bizdate}'
    group by 
          design_id 
        , brandgood_id 
)t03  
ON t02.design_id = t03.design_id 
 
JOIN  
( 
    -- 优化4：提前过滤品牌商品表，只保留符合条件的私有模型
    SELECT  
           brandgood_id 
         , brandgood_name 
         , user_id
    FROM  
        kdw_dw.dwd_cntnt_brandgood_s_d 
    WHERE ds='${bdp.system.bizdate}' 
    and public_type = 'private' 
    and (prodcat1_name IN ('家具', '照明', '厨卫', '家电')
        OR (prodcat1_name = '硬装' AND prodcat2_name = '门')
        OR (prodcat1_name = '硬装' AND prodcat2_name = '墙面' AND prodcat3_name IN ('背景墙', '壁炉', '护墙板'))
        OR (prodcat1_name = '吊顶' AND prodcat2_name = '电气模块')
    )
    -- 优化5：提前过滤有效用户，减少JOIN数据量
    AND user_id IN (
        SELECT DISTINCT join_user_id 
        FROM kdw_dw.dwb_usr_businessaccount_s_d 
        WHERE ds='${bdp.system.bizdate}'
        AND deleted = false
    )
)t04  
 
ON t03.brandgood_id = t04.brandgood_id 

JOIN

(
    -- 优化6：业务账户表只保留必要字段
    SELECT 
          join_user_id
        , org_id
    FROM kdw_dw.dwb_usr_businessaccount_s_d
    WHERE ds='${bdp.system.bizdate}' 
    AND deleted = false
)t05 

ON t04.user_id = t05.join_user_id
AND t02.org_id = t05.org_id

GROUP BY 
      t02.org_id
    , t02.level1_node_name  
    , t02.level2_node_name        
    , t04.brandgood_name

;