use insights;
SET spark.sql.shuffle.partitions=6;
uncache table sales_report_cached;
cache table sales_report_cached;
uncache table inventory_report_cached;
cache table inventory_report_cached;
uncache table retail_market_cached;
cache table retail_market_cached;

