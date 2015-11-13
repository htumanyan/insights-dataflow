--
-- Hive script to create the appraisal table that tracks market price information from various sources 
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
use insights;
drop table IF EXISTS appraisal_tmp;
CREATE  TABLE IF NOT EXISTS `appraisal_tmp`
(      
mid                             INT COMMENT 'Manheim ID',
chrome_style_id                 INT COMMENT 'Chrome Style ID',
modelyear                       INT,
make                            STRING,
model                           STRING,
mileage                         INT,
series                          STRING,
segment                         STRING COMMENT 'Vehicle segment, Ex: COMPACT, LUXURY SUV etc.',
series_detail                   STRING,
is_certified                    INT,
body_description                STRING,
body_type                       STRING,
number_of_doors                 INT,
body_cab_style                  STRING,
body_bed_style                  STRING,
body_roof_style                 STRING,
engine_description              STRING,
engine_cylinder_count           INT,
engine_displacement             FLOAT,
engine_fuel_type_desct          STRING,
transmission_description        STRING,
transmission_type               STRING,
transmission_gear_count         INT,
drive_train_type                STRING,
exterior_colour                 STRING,
exterior_base_color             STRING,
interior_description            STRING,
interior_color                  STRING,
interior_material               STRING,
categorized_equipment_ids       STRING,
veh_type                        STRING,
dma_durable_key                 STRING,
dma_code                        STRING COMMENT 'DMA - Dealer Market Area. Coarser than zip code. Can spawn many cities',
dma_id                          SMALLINT COMMENT 'DMA ID from at.geo table matched against Google map shapes',
dma_desc                        STRING,
city                            STRING,
state_code                      STRING,
zip_code                        STRING COMMENT 'Can be a full zip code or a prefix, i.e. first N digits',
county                          STRING,
country_code                    STRING,
latitude                        DOUBLE,
longitude                       DOUBLE,
submarket                       STRING,
sample_date                     BIGINT  COMMENT 'The date when appraisal took place as a string',
sample_date_ts                  BIGINT  COMMENT 'The date when appraisal took place as a timestamp',
sample_size                     INT     COMMENT 'The size of the sample used for appraisal. Not always available',
value                           INT     COMMENT 'Appraised value',
algorithm                       STRING  COMMENT 'Textual identifier or description of the algorithm - some sources provide it',
source                          TINYINT COMMENT 'Numeric identifier of the source where the data is obtained from. 1-vAuto, 2-MMR, 3-Manheimi Wholesale',
source_type                     TINYINT COMMENT '1 - retail, 2 - wholesale',
ymmd_sum_list_price                  double,
ymmd_total_count                    int

)
COMMENT 'Captures vehicle appraisal information from various sources. Some of the sources are wholesale, while others are retail. This is a consolidated table that tracks them all and allows arbitrary selection of specific appraisal source or specific kind of source (wholesale vs. retail vs. anything else)'
STORED AS PARQUET;
