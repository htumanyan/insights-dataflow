INSERT INTO insights.sales_report_cached SELECT
m.dmmake as  make,
m.dmmake as makeref,
m.SDTESA as registration,
'n/a' as chassis,
m.SSUBSE as derivative,
abs(hash(m.SSUBSE)) as derivativeid,
m.SDTESA as registrationdate,
m.SCOLOR as exteriorcolor,
m.DMSTDESL as creationdate,
 
m.DMTRANTYPE as salechannel,
m.DMSELLRNM as vendortradingname,
0 as vendorcountryid,
'United States' as vendorcountryname,
m.SSELLR as vendorid,
0 as vehicleageindays,
m.SDTESL as solddate,
unix_timestamp(m.SDTESL) as solddatets,
'n/a' as vatqualified,
m.SSLEPR as  sold_price,
0 as buyerpremium,
0 as delivery,
DMBUYERNM as buyer,
SBUYER as buyerid,
SBUYER as buyercode,
0 as deliverylocation,
m.DMSOLD as activesale,
m.DMMODEL as model,
'n/a' as code,
m.DMMODELYR as modelyear,
m.DMMODEL as modelcode,
m.SMILES as mileageG,
m.AGEDAYS as daysonsale,
m.DMHIGHBID s auctionprice,
case m.STRN when 'A' then  'Automatic' when '3' then '3 Speed' when '4' then '4 Speed' when '5' then '5 Speed' when '6' then '6 Speed' when 'M' then 'Manual' end,
abs(hash(m.STRN)) as transmissionid,
datediff(current_date(), m.SDTESI) as stockage,
m.SVEHIC as vehicle_type,
'none' as salessession,
0 as salesessionid, 
'none' as tacticname, 
0 as tacticid,
DMTRANSUBTYPE as commercialconceptname,
1 as countryid,
'United States' as countryname,
m.SCHGS as totaldamagesnetprice,
vdm.ev_engine_fuel_type_descr as fueltype,
vdm.b_vehicle_id as vehicleid,
m.SSER17 as vin,
0 as sourceid,
'n/a' as sourcename, 
abs(hash(m.DMCAT)) as buyertypeid,
m.DMCAT as buyertypename,
m.DMCAT as buyertypedesc,
m.SLEPR as priceexcludingvat,
0 as directsaleid,
m.DMSELLRNM as sellername,
m.SUID as sellerid,
m.DMSSLLYR as soldyear,
m.DMMONTH as soldmonth,
 '' as ageinweeksbandname,
 0 as ageinweeksbandid,
     CASE WHEN m.stockage/7 >=0 AND m.stockage/7 <=7 THEN 'under 1 '
            WHEN m.stockage/7 >=8 AND m.stockage/7 <=14 THEN '1 - 2'
            WHEN m.stockage/7 >=15 AND m.stockage/7 <=21 THEN '2 - 3'
            WHEN m.stockage/7 >=22 AND m.stockage/7 <=28 THEN '3 - 4'
            WHEN m.stockage/7 >=29 AND m.stockage/7 <=35 THEN '4 - 5'
            WHEN m.stockage/7 >=36 AND m.stockage/7 <=42 THEN '5 - 6'
            WHEN m.stockage/7 >=43 AND m.stockage/7 <=49 THEN '6 - 7'
            WHEN m.stockage/7 >=50 AND m.stockage/7 <=56 THEN '7 - 8'
            WHEN m.stockage/7 >=57 AND m.stockage/7 <=63 THEN '8 - 9'
            WHEN m.stockage/7 >=64 AND m.stockage/7 <=70 THEN '9 - 10'
            WHEN m.stockage/7 >=71 AND m.stockage/7 <=77 THEN '10 - 11'
            WHEN m.stockage/7 >=78 AND m.stockage/7 <=84 THEN '11 - 12'
            WHEN m.stockage/7 >=85 AND m.stockage/7 <=91 THEN '12 - 13'
            WHEN m.stockage/7 >=92 AND m.stockage/7 <=98 THEN '13 - 14'
            WHEN m.stockage/7 >=99 AND m.stockage/7 <=105 THEN '14 - 15'
            WHEN m.stockage/7 >=106 AND m.stockage/7 <=112 THEN '15 - 16'
            WHEN m.stockage/7 >=113 AND m.stockage/7 <=100000 THEN 'over 16'
      end as stockageWeeksBandName,
      CASE  WHEN m.stockage/7 >=0 AND m.stockage/7 <=7 THEN 0
            WHEN m.stockage/7 >=8 AND m.stockage/7 <=14 THEN 1
            WHEN m.stockage/7 >=15 AND m.stockage/7 <=21 THEN 2
            WHEN m.stockage/7 >=22 AND m.stockage/7 <=28 THEN 3
            WHEN m.stockage/7 >=29 AND m.stockage/7 <=35 THEN 4
            WHEN m.stockage/7 >=36 AND m.stockage/7 <=42 THEN 5
            WHEN m.stockage/7 >=43 AND m.stockage/7 <=49 THEN 6
            WHEN m.stockage/7 >=50 AND m.stockage/7 <=56 THEN 7
            WHEN m.stockage/7 >=57 AND m.stockage/7 <=63 THEN 8
            WHEN m.stockage/7 >=64 AND m.stockage/7 <=70 THEN 9
            WHEN m.stockage/7 >=71 AND m.stockage/7 <=77 THEN 10
            WHEN m.stockage/7 >=78 AND m.stockage/7 <=84 THEN 11
            WHEN m.stockage/7 >=85 AND m.stockage/7 <=91 THEN 12
            WHEN m.stockage/7 >=92 AND m.stockage/7 <=98 THEN 13
            WHEN m.stockage/7 >=99 AND m.stockage/7 <=105 THEN 14
            WHEN m.stockage/7 >=106 AND m.stockage/7 <=112 THEN 15
            WHEN m.stockage/7 >=113 AND m.stockage/7 <=100000 THEN 16
      end as stockageWeeksBandId,
      CASE WHEN m.SCHGS >=0 AND m.SCHGS <99 THEN 'under 100'
           WHEN m.SCHGS >=100 AND m.SCHGS <500 THEN '100 - 500'
           WHEN m.SCHGS >=500 AND m.SCHGS <750 THEN '501 - 750'
           WHEN m.SCHGS >=750 AND m.SCHGS <100000 THEN 'over 750'
      end as damagesBandName,
      case WHEN m.SCHGS >=0 AND m.SCHGS <100 THEN 0
           WHEN m.SCHGS >=100 AND m.SCHGS <500 THEN 1
           WHEN m.SCHGS >=500 AND m.SCHGS <750 THEN 2
           WHEN m.SCHGS >=750 AND m.SCHGS <100000 THEN 3
      end as damagesBandId,
     case WHEN m.SMILES >=0 AND m.SMILES <10000 THEN '0-10 000'
          WHEN m.SMILES >=10000 AND m.SMILES <20000 THEN '10 001-20 000'
          WHEN m.SMILES >=20000 AND m.SMILES <30000 THEN '20 001-30 000'
          WHEN m.SMILES >=30000 AND m.SMILES <40000 THEN '30 001-40 000'
          WHEN m.SMILES >=40000 AND m.SMILES <50000 THEN '40 001-50 000'
          WHEN m.SMILES >=50000 AND m.SMILES <75000 THEN '50 001-75 000'
          WHEN m.SMILES >=75000 AND m.SMILES <100000 THEN '75001-100 000'
          WHEN m.SMILES >=100000 AND m.SMILES <150000 THEN '100 001-150 000'
          WHEN m.SMILES >=150000 AND m.SMILES <999999 THEN 'over 150 000'
      end mileageBandName,
    case
            WHEN m.SMILES >=0 AND m.SMILES <10000 THEN 0
            WHEN m.SMILES >=10000 AND m.SMILES <20000 THEN 1
            WHEN m.SMILES >=20000 AND m.SMILES <30000 THEN 2
            WHEN m.SMILES >=30000 AND m.SMILES <40000 THEN 3
            WHEN m.SMILES >=40000 AND m.SMILES <50000 THEN 4
            WHEN m.SMILES >=50000 AND m.SMILES <75000 THEN 5
            WHEN m.SMILES >=75000 AND m.SMILES <100000 THEN 6
            WHEN m.SMILES >=100000 AND m.SMILES <150000 THEN 7
            WHEN m.SMILES >=150000 AND m.SMILES <999999 THEN 8
end mileageBandId,
m.stockage/7 as stockageweeks,
'n/a' as buyercountry,
0 as buyercountryid,
'n/a' as vendortown,
0 as locationid,
0 as commercialconcepttypeid,
2 as pl_id,
vdmo.vdm_options_desc_map,
vdmo.vdm_options_group_map,
vdmv.b_vehicle_id as vdm_b_vehicle_id,
vdmv.vb_vin as vdm_vb_vin,
vdmv.vb_associate_vin as vdm_vb_associate_vin,
vdmv.vb_vehicle_type as vdm_vb_vehicle_type,
vdmv.vb_model_year as vdm_vb_model_year,
vdmv.vb_make as vdm_vb_make,
vdmv.vb_model as vdm_vb_model,
vdmv.ev_trim as vdm_ev_trim,
vdmv.ev_manufacturer_trim as vdm_ev_manufacturer_trim,
vdmv.ev_style_id as vdm_ev_style_id,
vdmv.ev_manufacturer_style_id as vdm_ev_manufacturer_style_id,
vdmv.vb_subdivision_name as vdm_vb_subdivision_name,
vdmv.ev_msrp as vdm_ev_msrp,
vdmv.vb_half_year_ind as vdm_vb_half_year_ind,
vdmv.vb_manufacturer as vdm_vb_manufacturer,
vdmv.vb_number_of_wheels as vdm_vb_number_of_wheels,
vdmv.vb_gvwr as vdm_vb_gvwr,
vdmv.vb_language_code as vdm_vb_language_code,
vdmv.vb_source_code as vdm_vb_source_code,
vdmv.vb_country_of_origin_code as vdm_vb_country_of_origin_code,
vdmv.vb_country_code as vdm_vb_country_code,
vdmv.vb_ext_color_generic_descr as vdm_vb_ext_color_generic_descr,
vdmv.vb_ext_color_mfr_rgb_code as vdm_vb_ext_color_mfr_rgb_code,
vdmv.vb_ext_color_mfr_code as vdm_vb_ext_color_mfr_code,
vdmv.vb_ext_color_mfr_description as vdm_vb_ext_color_mfr_description,
vdmv.vb_ext_color_generic_descr2 as vdm_vb_ext_color_generic_descr2,
vdmv.vb_ext_color_mfr_rgb_code_2 as vdm_vb_ext_color_mfr_rgb_code_2,
vdmv.vb_ext_color_mfr_code_2 as vdm_vb_ext_color_mfr_code_2,
vdmv.vb_ext_color_mfr_description_2 as vdm_vb_ext_color_mfr_description_2,
vdmv.vb_int_color_mfr_code as vdm_vb_int_color_mfr_code,
vdmv.vb_int_color_mfr_description as vdm_vb_int_color_mfr_description,
vdmv.ev_region_code as vdm_ev_region_code,
vdmv.ev_market_class_id as vdm_ev_market_class_id,
vdmv.ev_model_id as vdm_ev_model_id,
vdmv.ev_transmission as vdm_ev_transmission,
vdmv.ev_drivetrain as vdm_ev_drivetrain,
vdmv.ev_wheelbase as vdm_ev_wheelbase,
vdmv.ev_market_class_description as vdm_ev_market_class_description,
vdmv.ev_number_of_doors as vdm_ev_number_of_doors,
vdmv.ev_passenger_capacity as vdm_ev_passenger_capacity,
vdmv.ev_fuel_economy_city_ville as vdm_ev_fuel_economy_city_ville,
vdmv.ev_fuel_economy_highway_route as vdm_ev_fuel_economy_highway_route,
vdmv.ev_fuel_eco_units_of_measure as vdm_ev_fuel_eco_units_of_measure,
vdmv.ev_style_name_without_trim as vdm_ev_style_name_without_trim,
vdmv.ev_body_type_primary as vdm_ev_body_type_primary,
vdmv.ev_body_type_secondary as vdm_ev_body_type_secondary,
vdmv.ev_odometer_reading as vdm_ev_odometer_reading,
vdmv.ev_odometer_reading_capture_ts as vdm_ev_odometer_reading_capture_ts,
vdmv.ev_odometer_digits as vdm_ev_odometer_digits,
vdmv.ev_odometer_units_of_measure as vdm_ev_odometer_units_of_measure,
vdmv.ev_odometer_status as vdm_ev_odometer_status,
vdmv.ev_odometer_type as vdm_ev_odometer_type,
vdmv.ev_engine_type as vdm_ev_engine_type,
vdmv.ev_engine_displacement as vdm_ev_engine_displacement,
vdmv.ev_engine_induction as vdm_ev_engine_induction,
vdmv.ev_engine_fuel_type_descr as vdm_ev_engine_fuel_type_descr,
vdmv.ev_engine_horse_power as vdm_ev_engine_horse_power,
vdmv.ev_vehicle_sub_type as vdm_ev_vehicle_sub_type,
vdmv.ev_line_name as vdm_ev_line_name,
vdmv.ev_ag_series_code as vdm_ev_ag_series_code,
vdmv.ev_in_service_date as vdm_ev_in_service_date,
vdmv.ev_mid as vdm_ev_mid,
vdmv.ev_invoice_wholesale as vdm_ev_invoice_wholesale,
vdmv.vb_created_timestamp as vdm_vb_created_timestamp,
vdmv.vb_created_by as vdm_vb_created_by,
vdmv.vb_last_update_timestamp as vdm_vb_last_update_timestamp,
vdmv.vb_last_update_by as vdm_vb_last_update_by,
mmr.mmr_body as mmr_body,
mmr.mmr_edition as mmr_edition,
mmr.mmr_algorithm as mmr_algorithm,
mmr.mmr_national_value as mmr_nationalvalue,
mmr.mmr_national_sample_size as mmr_nationalsamplesize
from 
(select *,  datediff( from_unixtime(unix_timestamp()), to_date(SDTESA))  from manheim.sales) m
 join on  vdm.vehicles vdmv on vdmv.vb_vin=m.SSER17 
left join mmr.sales mmr on m.SSER17 = mmr.vin;
cache table sales_report_cached;

