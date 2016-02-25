use insights;
SET spark.sql.shuffle.partitions=6;

drop table retail_market_cached_bkp;
alter table retail_market_cached rename to retail_market_cached_bkp;
alter table retail_market_cached_tmp rename to retail_market_cached;
uncache table retail_market_cached;
cache table retail_market_cached;

drop table sales_report_cached_bkp;
alter table sales_report_cached rename to sales_report_cached_bkp;
alter table sales_report_cached_tmp rename to sales_report_cached;
uncache table sales_report_cached;
cache table sales_report_cached;

drop table inventory_report_cached_bkp;
alter table inventory_report_cached rename to inventory_report_cached_bkp;
alter table inventory_report_cached_tmp rename to inventory_report_cached;
uncache table inventory_report_cached;
cache table inventory_report_cached;

drop table dso_metrics_bkp;
alter table dso_metrics rename to dso_metrics_bkp;
alter table dso_metrics_tmp rename to dso_metrics;
uncache table dso_metrics;
cache table dso_metrics;

drop table appraisal_bkp;
alter table appraisal rename to appraisal_bkp;
alter table appraisal_tmp rename to appraisal;
uncache table appraisal;
cache table appraisal;
