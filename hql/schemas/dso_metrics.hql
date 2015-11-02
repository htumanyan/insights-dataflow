use insights; 
create table  dso_metrics 
(
make string, 
model string,
modelyear string,
geo_dma_id string,
date  string,
inventory int,
daily_sold int
)
stored as parquet;
