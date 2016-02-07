use ovt;
SET spark.sql.shuffle.partitions=200;
drop table man_ovt_fact_registration_dedup_stg;
create table if not exists man_ovt_fact_registration_dedup_stg as select
* 
from ( select 
*,
row_number() over ( partition by uniq_reg_id order by ACT_OFFRNG_START_TS desc , DW_UPDATED_ON  desc ) as group_rank 
from man_ovt_fact_registration where model_yr>=2010
) t where group_rank=1;

