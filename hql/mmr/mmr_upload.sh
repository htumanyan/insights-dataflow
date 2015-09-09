#!/bin/bash

Today=`date +"%Y%m%d"`
Filename='INTS_MMR_INPUT_'$Today'.csv'
Server='mftstg.manheim.com'
User='rms_insights_mmr'
LDirectory='/mnt/resource/mmr_dump'
RDirectory='Outbox/MMR/INTS'
Password='password'

hadoop fs -getmerge /tmp/mmr_dump/ /mnt/resource/mmr_dump/$Filename
hadoop fs -rm -skipTrash /tmp/mmr_dump/*

ftp -n $Server <<End-Of-Session
user $User $Password
cd $RDirectory
lcd $LDirectory
put $Filename
bye
End-Of-Session
