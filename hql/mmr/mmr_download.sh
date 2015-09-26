#!/bin/bash

Today=`date +"%Y%m%d"`
Yesterday=`date --date='1 day ago' +%Y%m%d`
Filename=$1
Filetype='.csv'
TodayFile=$Filename$Today$Filetype
YesterdayFile=$Filename$Yesterday'.noquote'$Filetype
Server=$2
User=$3
LDirectory=$4
RDirectory=$5
Password=$6

ftp -n $Server <<End-Of-Session
user $User $Password
cd $RDirectory
lcd $LDirectory
get $TodayFile
bye
End-Of-Session

cd $LDirectory
if sed -e '1d' -e 's/\"//g' $TodayFile > $Filename$Today'.noquote'$Filetype ; then
    if hdfs dfs -put -f $Filename$Today'.noquote'$Filetype /data/database/mmr/ ; then
        hdfs dfs -rm /data/database/mmr/$YesterdayFile
    else
        echo "hdfs put failed"
    fi
else
    echo "file editing failed"
fi
rm $TodayFile
rm $Filename$Today'.noquote'$Filetype