#!/bin/bash

Today=`date +"%Y%m%d"`
Yesterday=`date --date='1 day ago' + %Y%m%d`
Filename='INTS_MMR_OUTPUT_'
Filetype='.csv'
TodayFile=$Filename$Today$Filetype
YesterdayFile=$Filename$Yesterday'.no_head'$Filetype
Server='mft.manheim.com'
User='rms_insights_mmr'
LDirectory='/mnt/resource/'
RDirectory='Inbox/MMR/'
Password='rm$instsmm9'

ftp -n $Server <<End-Of-Session
user $User $Password
cd $RDirectory
lcd $LDirectory
get $TodayFile
bye
End-Of-Session

sed '1d' $TodayFile > $Filename$Today'.nohead'$Filetype
hdfs dfs -put $Filename$Today'.nohead'$Filetype /data/database/mmr/
hdfs dfs -rm $YesterdayFile /data/database/mmr/