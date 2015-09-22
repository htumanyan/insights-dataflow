#!/bin/bash

Today=`date +"%Y%m%d"`
Filename=$1$Today'.csv'
Server=$2
User=$3
LDirectory=$4
RDirectory=$5
Password=$6

hadoop fs -getmerge /tmp/mmr_dump/ /mnt/resource/mmr_dump/$Filename
hadoop fs -rm -skipTrash /tmp/mmr_dump/*

ftp -n $Server <<End-Of-Session
user $User $Password
cd $RDirectory
lcd $LDirectory
put $Filename
bye
End-Of-Session