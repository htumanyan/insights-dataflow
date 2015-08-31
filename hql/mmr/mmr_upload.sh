#!/bin/bash

Today=`date +"%Y%m%d"`
Filename='INTS_MMR_INPUT_$Today.csv'
Server='mftstg.manheim.com'
User='rms_insights_mmr'
LDirectory='/mnt/resource/'
RDirectory='Outbox/MMR/INTS'
Password='password'


hive -e "SELECT vin, date_format(solddate, 'M/d/yyyy'), mileage, modelyear, make, model, derivative FROM insights.sales_report_cached;" | sed 's/[\t]/,/g' > $LDirectory$Filename

ftp -n $Server <<End-Of-Session
user $User $Password
cd $RDirectory
lcd $LDirectory
put $Filename
bye
End-Of-Session
