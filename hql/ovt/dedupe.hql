use ovt;
SET spark.sql.shuffle.partitions=200;
drop table man_ovt_fact_registration_dedup;
create table if not exists man_ovt_fact_registration_dedup as select
* 
from ( select 
*,
row_number() over ( partition by uniq_reg_id order by ACT_OFFRNG_START_TS desc , DW_UPDATED_ON  desc ) as group_rank 
from man_ovt_fact_registration where model_yr>=2010 and uniq_reg_id is not null and uniq_reg_id != 'null'
) t where group_rank=1;

drop table if exists ovt.man_ovt_fact_registration_ext_dedup;
create table if not exists ovt.man_ovt_fact_registration_ext_dedup as 
select * from (
select *,
row_number() over ( partition by reg_key, reg_dt_key  order by ingestion_timestamp desc) as group_rank 
 from ovt.man_ovt_fact_registration_ext
) t where t.group_rank=1;


create table ovt.man_ovt_dim_auction_dedup as select *
from 
(select  *, row_number() over partition by auction_key from ovt.man_ovt_dim_auction where auction_key is not null  ) t  where group_rank=1;


create table ovt.man_ovt_dim_flndr_dedup as select *
from 
(select  *, row_number() over partition by flndr_key from ovt.man_ovt_dim_flndr where ovt_flndr_key is not null  ) t  where group_rank=1;

