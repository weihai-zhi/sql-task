
create table if not exists ka_customer_tishen_brandgood_order_s_d
(
    org_id                    BIGINT comment'商家id',
    order_readable_id         STRING comment'语义化订单id',
    design_id                 BIGINT comment'设计方案id',
    value                     STRING comment'审核单关联单号',
    created                   timestamp comment'首次提审日期',
    review_status             STRING comment'审核状态',
    last_modified             timestamp comment'状态变更日期',
    brandgood_id               bigint comment'商品id',
    dhd_type                  string  comment '方案内包含素材的类型:furniture.模型,material.材质,molding.踢脚线,customized.定制,decoration.硬装',
    b4110_1_design_brandgood_relation_cnt BIGINT COMMENT '方案内包含同一素材的次数',
    brandgood_name            string comment '商品名称'
 )COMMENT'客户提审方案包含素材明细表'PARTITIONED BY(
    `ds` STRING COMMENT'日期，格式YYYYMMDD'
 )LIFECYCLE 3650
;



drop table if exists tmp_ka_customer_tishen_brandgood_order_s_d_${ds};
create table tmp_ka_customer_tishen_brandgood_order_s_d_${ds}  as 
select
    t2.org_id
    ,t2.order_readable_id
    ,t1.design_id
    ,t5.value
    ,t1.created
    ,case when(t2.state = 27) then'待审核'
          when(t2.state = 28) then'审核中'
          when(t2.state = 29) then'已通过'
          when(t2.state = 30) then'已撤回'
          when(t2.state = 31) then'已驳回'
          when(t2.state = 32) then'已退回'
          end as review_status
    ,t2.last_modified
from(
    select
    design_id
    ,order_id
    ,created
    ,deleted
        from(select
                    design_id
                    ,order_id
                    ,created
                    ,deleted
                    ,rank() over(partition by design_id order by created asc) as ranks
                from
                    kdw_ods.ods_dcsproduce_order_design_s_d
                where ds ='${ds}'
                and deleted = false
                ) t1
        where
            ranks = 1
 )t1
    inner join(
        select
            order_id
            ,org_id
            ,status
            ,last_modified
            ,order_readable_id
            ,state
        from
            kdw_ods.ods_coc_order_info_s_d
        where ds ='${ds}'
              and status = 0
              and state not in(1,30)
              and template_id = 2
              )t2 on t1.order_id = t2.order_id
    left outer join(
                select
                order_id
                ,value
                from
                    kdw_ods.ods_cocsuboir_order_item_relation_s_d
                where ds ='${ds}'
                and item_key = 'auditRelationId'
                )t5 on t1.order_id = t5.order_id
group by
    t2.org_id
    ,t2.order_readable_id
    ,t1.design_id
    ,t5.value
    ,t1.created
    ,case when(t2.state = 27)then'待审核'when(t2.state = 28)then'审核中'when(t2.state = 29)then'已通过'when(t2.state = 30)then'已撤回'when(t2.state = 31)then'已驳回'when(t2.state = 32)then'已退回'end
    ,t2.last_modified
;

INSERT OVERWRITE TABLE ka_customer_tishen_brandgood_order_s_d PARTITION(ds ='${ds}')
SELECT 
 t1.org_id
,t1.order_readable_id
,t1.design_id
,t1.value
,t1.created
,t1.review_status
,t1.last_modified
,t2.brandgood_id --bigint COMMENT '素材id',
,t2.dhd_type --string COMMENT '方案内包含素材的类型:furniture.模型,material.材质,molding.踢脚线,customized.定制,decoration.硬装' ,
,t2.b4110_1_design_brandgood_relation_cnt --BIGINT COMMENT '方案内包含同一素材的次数'
,t3.brandgood_name
from  
(SELECT 
 org_id
,order_readable_id
,design_id
,value
,created
,review_status
,last_modified
from 
tmp_ka_customer_tishen_brandgood_order_s_d_${ds} ) t1 
left join (select 
    design_id --bigint comment '方案id',
    ,brandgood_id --bigint COMMENT '素材id',
    ,dhd_type --string COMMENT '方案内包含素材的类型:furniture.模型,material.材质,molding.踢脚线,customized.定制,decoration.硬装' ,
    ,b4110_1_design_brandgood_relation_cnt --BIGINT COMMENT '方案内包含同一素材的次数'
    from kdw_dw.dws_cntnt_1d_dbr_change_by_design_brandgood_s_d
    where ds = '${ds}') t2 
on t1.design_id = t2.design_id
left join (SELECT 
    brandgood_id
    ,brandgood_name
    from 
    kdw_dw.dwd_cntnt_brandgood_s_d
    where ds = '${ds}') t3 
on t2.brandgood_id = t3.brandgood_id
;

drop table if exists tmp_ka_customer_tishen_brandgood_order_s_d_${ds};