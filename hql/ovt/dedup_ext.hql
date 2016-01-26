drop table if exists ovt.man_ovt_fact_registration_ext_dedup;
create table if not exists ovt.man_ovt_fact_registration_ext_dedup as 
select * from (
select *,
row_number() over ( partition by reg_key, reg_dt_key  order by ingestion_timestamp desc) as group_rank 
 from ovt.man_ovt_fact_registration_ext
) t where t.group_rank=1;
