create table sales_dimension_country_cached as select  distinct  countryid,C.name  from rpt_bmw_soldreport_cached RPT  inner join psa.country_stg C on RPT.countryid = C.id;
create table sales_dimension_make_cached as select  distinct  make  from rpt_bmw_soldreport_cached;
create table sales_dimension_transmission_cached as select distinct transmission, transmissionid  from rpt_bmw_soldreport_cached;
create table sales_dimension_source_cached as select  distinct  sourceid, sourcename  from rpt_bmw_soldreport_cached;
create table concept_channel_tactic_session_cached as select  distinct CommercialConceptName, CommercialConceptId, TacticId, Tacticname, SalesSessionID, SalesSession, SaleChannel, SaleChannelId from psa_shark.rpt_bmw_soldreport_cached where CommercialConceptName is not null;
