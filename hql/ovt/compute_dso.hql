use ovt;
create table if not exists  ovt_make_model as select 
mmt.make_desc as make,
 mmt.model_desc as model,                       
 r.model_yr as model_year,                      
 r.offered_cnt,
 r.reg_ts,
unix_timestamp(r.reg_ts) as reg_ts_ts,
 r.vin,
 r.reg_key,
 r.gross_txn_flg,
r.offrng_flg,
r.pur_amt,
r.at_sale_nat_mmr
 from ovt.man_ovt_fact_registration_dedup r  join ( 
select make_model_trim_key, make_desc , model_desc from ovt.man_ovt_dim_make_model_trim group by make_model_trim_key, make_desc, model_desc) mmt
 on mmt.make_model_trim_key = r.make_model_trim_key and unix_timestamp(r.reg_ts)>1420070400;

create  table if not exists make_model_gross_txn	 as select 
s.reg_key,
s.make, 
s.model,
s.model_year, 
s.ts, 
sum(gross_txn_flg) over (partition by s.make, s.model order by unix_timestamp(s.ts) range between 3888000 PRECEDING and current row) as cnt 
from (select model_year, model, make, reg_key, case gross_txn_flg when   1 then  1 else 0 end as gross_txn_flg,  reg_ts as ts from ovt_make_model   order by ts desc ) s;

create  table if not exists make_model_sum_offered_count as select 
s.reg_key,
 s.make,
 s.model,
 s.model_year,
 s.ts,
sum(s.offered_cnt) over (partition by s.make, s.model order by unix_timestamp(s.ts) range between 3888000 PRECEDING and current row) as cnt 
from (select make as make, model as model,reg_key,  model_year as model_year, offered_cnt, reg_ts as ts from ovt_make_model order by ts desc ) s;

create  table if not exists make_model_sum_offerng_flg	 as select 
s.reg_key,
 s.make,
 s.model,
 s.model_year,
 s.ts,
sum(s.offrng_flg) over (partition by s.make, s.model order by unix_timestamp(s.ts) range between 3888000 preceding and current row) as cnt 
from (select model_year, model, make,reg_key,  reg_ts  as ts, case offrng_flg when 1 then 1 else 0 end as offrng_flg  from ovt_make_model order by ts desc ) s;


create  table if not exists make_model_mmr_retention as 
select s.reg_key, s.make, s.model, s.model_year, s.ts,
 sum(s.pur_amt) over (partition by s.make, s.model order by unix_timestamp(s.ts) range between 3888000 PRECEDING and current row) as sum_pur_amt,
 sum(s.at_sale_nat_mmr  ) over (partition by s.make, s.model order by unix_timestamp(s.ts) range between 3888000 PRECEDING and current row) as sum_sale_nat_mmr
 from (select model_year, model, make,reg_key,  reg_ts  as ts, case at_sale_nat_mmr when 'null' then 0 else at_sale_nat_mmr end as at_sale_nat_mmr, case pur_amt when 'null' then 0 else pur_amt end as pur_amt  from ovt_make_model order by ts desc ) s;

drop table if exists make_model_metrics;
create  table make_model_metrics as select 
 m1.reg_key,
 m1.make,
 m1.model,
 m1.model_year,
 m1.ts,
 m1.cnt/m3.cnt as effectiveness,
 m1.cnt/m2.cnt as efficiency,
 m1.cnt as sum_gross_txn,
 m2.cnt as sum_offered_cnt,
 m3.cnt as sum_offerng_flg,
 m4.sum_pur_amt as sum_pur_amt,
 m4.sum_sale_nat_mmr as sum_sale_nat_mmr,
 m4.sum_pur_amt/m4.sum_sale_nat_mmr as mmr_retention 
from make_model_gross_txn m1 
join make_model_sum_offered_count m2  on  m1.reg_key= m2.reg_key
join make_model_sum_offerng_flg  m3  on m1.reg_key = m3.reg_key
join make_model_mmr_retention   m4  on m1.reg_key = m4.reg_key;
