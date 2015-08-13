use insights;
drop table sales_report_cached;
CREATE  TABLE `sales_report_cached`(                                                           
   `make` string COMMENT '',
   `makeref` string COMMENT '',
   `registration` string COMMENT '',
   `chassis` string COMMENT '',
   `derivative` string COMMENT '',
   `derivativeid` int COMMENT '',
   `registrationdate` string COMMENT '',
   `exteriorcolour` string COMMENT '',
   `creationdate` string COMMENT '',
   `salechannelid` int COMMENT '',
   `salechannel` string COMMENT '',
   `vendortradingname` string COMMENT '',
   `vendorcountryid` int COMMENT '',
   `vendorcountryname` string COMMENT '',
   `vendorid` int COMMENT '',
   `vehicleageindays` int COMMENT '',
   `solddate` string COMMENT '',
   `solddatets` bigint COMMENT '',
   `vatqualified` string COMMENT '',
   `soldprice` double COMMENT '',
   `buyerpremium` double COMMENT '',
   `delivery` double COMMENT '',
   `buyer` string COMMENT '',
   `buyerid` int COMMENT '',
   `buyercode` string COMMENT '',
   `deliverylocation` double COMMENT '',
   `activesale` string COMMENT '',
   `model` string COMMENT '',
   `code` string COMMENT '',
   `modelyear` int COMMENT '',
   `model_code` string COMMENT '',
   `mileage` int COMMENT '',
   `daysonsale` int COMMENT '',
   `auctionprice` double COMMENT '',
   `transmission` string COMMENT '',
   `transmissionid` int COMMENT '',
   `stockage` int COMMENT '',
   `vehicle_type` string COMMENT '',
   `salessession` string COMMENT '',
   `salessessionid` int COMMENT '',
   `tacticname` string COMMENT '',
   `tacticid` int COMMENT '',
   `commercialconceptname` string COMMENT '',
   `countryid` int COMMENT '',
   `countryname` string COMMENT '',
   `totaldamagesnetprice` double COMMENT '',
   `fueltype` string COMMENT '',
   `vehicleid` int COMMENT '',
   `vin` string COMMENT '',
   `sourceid` int COMMENT '',
   `sourcename` string COMMENT '',
   `buyertypeid` int COMMENT '',
   `buyertypename` string COMMENT '',
   `buyertypedesc` string COMMENT '',
   `priceexcludingvat` double COMMENT '',
   `directsaleid` int COMMENT '',
   `sellername` string COMMENT '',
   `sellerid` int COMMENT '',
   `soldyear` int COMMENT '',
   `soldmonth` int COMMENT '',
   `ageinweeksbandname` string COMMENT '',
   `ageinweeksbandid` int COMMENT '',
   `stockageweeksbandname` string COMMENT '',
   `stockageweeksbandid` int COMMENT '',
   `damagesbandname` string COMMENT '',
   `damagesbandid` int COMMENT '',
   `mileagebandname` string COMMENT '',
   `mileagebandid` int COMMENT '',
   `stockageweeks` bigint COMMENT '',
   `vehicleageweeks` bigint COMMENT '',
   `buyercountry` string COMMENT '',
   `buyercountryid` int COMMENT '',
   `vendortown` string COMMENT '',
   `locationid` int COMMENT '',
   `commercialconcepttypeid` int COMMENT '',
   `pl_id` int COMMENT '',
   `vdm_options_desc_map` Map<Int, String>,
   `vdm_options_group_map` Map<Int, String>,
`vdm_b_vehicle_id` int,
`vdm_vb_vin` string ,
`vdm_vb_associate_vin` string,
`vdm_vb_vehicle_type` string,
`vdm_vb_model_year` string,
`vdm_vb_make` string,
`vdm_vb_model` string,
`vdm_ev_trim` string,
`vdm_ev_manufacturer_trim` string,
`vdm_ev_style_id` int,
`vdm_ev_manufacturer_style_id` int,
`vdm_vb_subdivision_name` string,
`vdm_ev_msrp` int,
`vdm_vb_half_year_ind` int,
`vdm_vb_manufacturer` string,
`vdm_vb_number_of_wheels` string,
`vdm_vb_gvwr` string,
`vdm_vb_language_code` string,
`vdm_vb_source_code` string,
`vdm_vb_country_of_origin_code` string,
`vdm_vb_country_code` string,
`vdm_vb_ext_color_generic_descr` string,
`vdm_vb_ext_color_mfr_rgb_code` string,
`vdm_vb_ext_color_mfr_code` string,
`vdm_vb_ext_color_mfr_description` string,
`vdm_vb_ext_color_generic_descr2` string,
`vdm_vb_ext_color_mfr_rgb_code_2` string,
`vdm_vb_ext_color_mfr_code_2` string,
`vdm_vb_ext_color_mfr_description_2` string,
`vdm_vb_int_color_mfr_code` string,
`vdm_vb_int_color_mfr_description` string,
`vdm_ev_region_code` string,
`vdm_ev_market_class_id` int,
`vdm_ev_model_id` int,
`vdm_ev_transmission` string,
`vdm_ev_drivetrain` string,
`vdm_ev_wheelbase` string,
`vdm_ev_market_class_description` string,
`vdm_ev_number_of_doors` int,
`vdm_ev_passenger_capacity` int,
`vdm_ev_fuel_economy_city_ville` int,
`vdm_ev_fuel_economy_highway_route` int,
`vdm_ev_fuel_eco_units_of_measure` string,
`vdm_ev_style_name_without_trim` string,
`vdm_ev_body_type_primary` string,
`vdm_ev_body_type_secondary` string,
`vdm_ev_odometer_reading` int,
`vdm_ev_odometer_reading_capture_ts` int,
`vdm_ev_odometer_digits` int,
`vdm_ev_odometer_units_of_measure` string,
`vdm_ev_odometer_status` string,
`vdm_ev_odometer_type` string,
`vdm_ev_engine_type` string,
`vdm_ev_engine_displacement` double,
`vdm_ev_engine_induction` string,
`vdm_ev_engine_fuel_type_descr` string,
`vdm_ev_engine_horse_power` int,
`vdm_ev_vehicle_sub_type` string,
`vdm_ev_line_name` string,
`vdm_ev_ag_series_code` string,
`vdm_ev_in_service_date` string,
`vdm_ev_mid` string,
`vdm_ev_invoice_wholesale` string,
`vdm_vb_created_timestamp` string,
`vdm_vb_created_by` string,
`vdm_vb_last_update_timestamp` string,
`vdm_vb_last_update_by` string,
`mmr.mmr_body` string,
`mmr.mmr_edition` string,
`mmr.mmr_algorithm` string,
`mmr.mmr_national_value` string,
`mmr.mmr_national_sample_size` int
);
 
