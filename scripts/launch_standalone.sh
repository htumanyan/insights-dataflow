#!/bin/bash 

db=$2 

#NAMENODE_HOST='dev-na-lxhdn01.cloudapp.net'
#EDGENODE_HOST='dev-na-LXHDE01'
#SQL_SERVER_HOST='dev-na-LXHDE01'
#MYSQL_HOST='dev-na-LXHDE01'
#JDBC_URL='jdbc:mysql://dev-na-LXHDE01/rpm'
#SQL_USER='insights'
#SQL_PASS='M@ins3a'

current_time=`date -u +%FT%TZ`

current_sharelib=`sudo -E -u oozie "oozie admin -sharelibupdate | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'"`
sudo -u oozie -E 'oozie job -config '`pwd`'/oozie/configs/standalone_jobs/'$db'.properties -run -Ddb_name=$db -Dcurrent_sharelib='$current_sharelib -Dnamenode-host=$NAMENODE_HOST -Dedgenode_host=$EDGENODE_HOST -Dsql_user=$SQL_USER -Dsql_pass=$SQL_PASS -Dnominal_time=$currrent_time 
