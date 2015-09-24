use insights;
drop table if exists inventory_report_cached;
 CREATE  TABLE `inventory_report_cached`(                                                           
   `vehicleid` int COMMENT '',                                                                      
   `vin` string COMMENT '',                                                                         
   `countryid` int COMMENT '',                                                                      
   `exteriorcolour` string COMMENT '',                                                              
   `creationts` bigint COMMENT '',                                                                  
   `countryname` string COMMENT '',                                                                 
   `make` string COMMENT '',                                                                        
   `makeref` string COMMENT '',                                                                     
   `salechannelid` int COMMENT '',                                                                  
   `salechannel` string COMMENT '',                                                                 
   `commercialconceptname` string COMMENT '',                                                       
   `vendortradingname` string COMMENT '',                                                           
   `vendorid` int COMMENT '',                                                                       
   `derivative` string COMMENT '',                                                                  
   `derivativeid` int COMMENT '',                                                                   
   `sourceid` int COMMENT '',                                                                       
   `sourcename` string COMMENT '',                                                                  
   `totaldamagesnetprice` double COMMENT '',                                                        
   `mileage` int COMMENT '',                                                                        
   `stockage` int COMMENT '',                                                                       
   `fueltype` string COMMENT '',                                                                    
   `model` string COMMENT '',                                                                       
   `modelref` string COMMENT '',                                                                    
   `code` string COMMENT '',                                                                        
   `modelyear` int COMMENT '',                                                                      
   `model_code` string COMMENT '',                                                                  
   `auctionprice` double COMMENT '',                                                                
   `transmission` string COMMENT '',                                                                
   `transmissionid` int COMMENT '',                                                                 
   `registration` string COMMENT '',                                                                
   `vendorstatusid` int COMMENT '',                                                                 
   `vehicleageindays` int COMMENT '',                                                               
   `stockageweeks` bigint COMMENT '',                                                               
   `vehicleageweeks` bigint COMMENT '',                                                             
   `ageinweeksbandname` string COMMENT '',                                                          
   `ageinweeksbandid` int COMMENT '',                                                               
   `stockageweeksbandname` string COMMENT '',                                                       
   `stockageweeksbandid` int COMMENT '',                                                            
   `damagesbandname` string COMMENT '',                                                             
   `damagesbandid` int COMMENT '',                                                                  
   `mileagebandname` string COMMENT '',                                                             
   `mileagebandid` int COMMENT '',                                                                  
   `statusid` int COMMENT '',                                                                       
   `statusdescription` string COMMENT '',
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
`rpm_lease_start_date` string,
`rpm_lease_start_year` int,
`rpm_lease_start_month` int,
`rpm_lease_start_day` int,
`rpm_lease_end_date` string,
`rpm_lease_end_year` int,
`rpm_lease_end_month` int,
`rpm_lease_end_day` int,
`rpm_lease_start_ts` bigint,
`rpm_lease_end_ts` bigint,
`polk_corporation` string,
`polk_report_year_month` string,
`polk_transaction_date` string,
`polk_transaction_ts` bigint,
`polk_trans_price`  int,
`polk_data_type` string,
`polk_origin` string ,
`polk_purchase_lease` string,
`polk_vehicle_count` int,
`polk_dealer_name` string,
`polk_dealer_address` string,
`polk_dealer_town` string,
`polk_dealer_state` string,
`polk_dealer_zip` string,
`polk_dealer_dma` string,
`polk_fran_ind` string,
`geo_dma_durable_key` string,
`geo_dma_code` string,
`geo_dma_desc` string,
`geo_city` string,
`geo_state_code` string,
`geo_county` string,
`geo_country_code` string,
`geo_latitude` double,
`geo_longitude` double,
`geo_submarket` string,
`geo_tim_zone_desc` string,
`geo_dma_id` smallint
);


