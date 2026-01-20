
-- Spark 2.4 优化参数配置
SET spark.executor.instances=120;
SET spark.executor.cores=4;
SET spark.executor.memory=16g;
SET spark.driver.memory=4g;

-- Shuffle 优化
SET spark.shuffle.file.buffer=64k;
SET spark.reducer.maxSizeInFlight=96m;
SET spark.shuffle.sort.bypassMergeThreshold=400;

-- Memory 管理
SET spark.memory.fraction=0.8;
SET spark.memory.storageFraction=0.2;

-- Join 优化
SET spark.sql.autoBroadcastJoinThreshold=10485760;

create table if not exists `dwb_cntnt_org_user_render_oper_s_d_refactor` (
  `pic_id` bigint comment '渲染图id'
 ,`design_id` bigint comment '渲染图片所属方案id'
 ,`camera_type_id` bigint comment '相机类型id'
 ,`camera_type_name` string comment '相机类型名'
 ,`account_id` bigint comment '商家账号id'
 ,`org_id` bigint comment '组织（含apic账号）id'
 ,`user_id` BIGINT COMMENT '用户id'
 ,`org_user_type` bigint comment '表示该账号的类别:1.商家账号,2.apic账号'
 ,`1d_is_created` boolean comment '是否本日新增'
 ,`1d_is_deleted` boolean comment '是否本日删除'
 ,`1m_is_created` boolean comment '是否本月新增'
 ,`1m_is_deleted` boolean comment '是否本月删除'
 ,`deleted` boolean comment '当前是否删除'
)
 partitioned by (`ds` string)
lifecycle 7
;


-- 优化后的查询
INSERT INTO TABLE kdw_dw.dwb_cntnt_org_user_render_oper_s_d_refactor PARTITION(ds='${bdp.system.bizdate}')
SELECT /*+ BROADCAST(org) */
  curr.pic_id AS pic_id,
  curr.design_id,
  CASE WHEN curr.camera_type_id IS NULL THEN -1 ELSE curr.camera_type_id END AS camera_type_id,
  CASE 
    WHEN curr.camera_type_id = 1 THEN '普通图' 
    WHEN curr.camera_type_id = 2 THEN '全景图' 
    WHEN curr.camera_type_id = 3 THEN '俯视图' 
    ELSE '其它' 
  END AS camera_type_name,
  org.account_id,
  org.org_id,
  org.user_id,
  org.org_user_type,
  -- 1. 是否本日新增：按照created的时间去统计
  curr.is_created_today AS 1d_is_created,
  -- 2. 是否本日删除：前一天的数据只获取删除的数据，当天的删除数据和前一天的删除数据对比
  CASE WHEN curr.is_deleted = TRUE AND prev_day_deleted.pic_id IS NULL THEN TRUE ELSE FALSE END AS 1d_is_deleted,
  -- 3. 是否本月新增：按照created的时间去统计
  curr.is_created_this_month AS 1m_is_created,
  -- 4. 是否本月删除：上月末最后一天的分区的数据只获取删除的数据，当天的删除数据和月末分区的删除数据对比
  CASE WHEN curr.is_deleted = TRUE AND prev_month_end_deleted.pic_id IS NULL THEN TRUE ELSE FALSE END AS 1m_is_deleted,
  curr.is_deleted AS deleted
FROM (
  -- 当前数据
  SELECT 
    pic_id,
    design_id,
    camera_type_id,
    CASE WHEN user_id = 1 THEN author_id ELSE user_id END AS user_id,
    CASE WHEN (deleted = TRUE OR user_id = 1) THEN TRUE ELSE FALSE END AS is_deleted,
    -- 本日新增：created时间为本日
    CASE WHEN date_format(created, 'yyyyMMdd') = '${bdp.system.bizdate}' THEN TRUE ELSE FALSE END AS is_created_today,
    -- 本月新增：created时间为本月
    CASE WHEN date_format(created, 'yyyyMM') = date_format(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), 'yyyyMM') THEN TRUE ELSE FALSE END AS is_created_this_month
  FROM kdw_dw.dwd_cntnt_render_oper_pretreatment_s_d
  WHERE ds = '${bdp.system.bizdate}'  AND from_pid IS NULL
) curr
INNER JOIN  (

  -- 组织用户数据
  SELECT account_id, org_id, user_id, org_user_type
  FROM kdw_dw.dim_usr_org_user_s_d
  WHERE ds = '${bdp.system.bizdate}' 
) org 
ON curr.user_id = org.user_id
LEFT JOIN (
  -- 3. 昨天只获取删除的数据
  SELECT pic_id
  FROM kdw_dw.dwd_cntnt_render_oper_pretreatment_s_d
  WHERE ds = '${bdp.system.bizdate.before1}' 
    AND from_pid IS NULL
    AND (deleted = TRUE OR user_id = 1)
) prev_day_deleted ON curr.pic_id = prev_day_deleted.pic_id
LEFT JOIN (
  -- 4. 上月末最后一天只获取删除的数据
  SELECT pic_id
  FROM kdw_dw.dwd_cntnt_render_oper_pretreatment_s_d
  WHERE ds = date_format(last_day(add_months(to_date('${bdp.system.bizdate}', 'yyyyMMdd'), -1)),'yyyyMMdd') 
    AND from_pid IS NULL
    AND (deleted = TRUE OR user_id = 1)
) prev_month_end_deleted ON curr.pic_id = prev_month_end_deleted.pic_id
;


