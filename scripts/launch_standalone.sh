#!/bin/bash 

env=$1
db=$2 
current_sharelib=`sudo su - oozie -c "oozie admin -sharelibupdate | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'"`
sudo su oozie -c 'oozie job -config '`pwd`'/oozie/configs/'$env'/standalone_jobs/'$db'.properties -run -Dcurrent_sharelib='$current_sharelib
