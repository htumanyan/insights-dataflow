#!/bin/bash

sudo su - oozie -c '/usr/hdp/current/oozie-server/bin/oozie-setup.sh sharelib create -fs /user/oozie/share/lib/' 
libpath=`sudo su - oozie -c 'oozie admin -sharelibupdate' | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'` #find out where is the sharelib
hadoop fs -copyFromLocal -f hql /user/oozie/share
hadoop fs -copyFromLocal -f oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal -f oozie/standalone_jars/* /user/oozie/share/lib
hadoop fs -copyFromLocal -f oozie/sharelib/*  /user/oozie/share/lib/$libpath/ 
sudo su - oozie -c 'oozie admin -sharelibupdate' #update the sharlib for realz
sudo su - oozie -c 'oozie admin -shareliblist'|  grep 'hive-0.13'
if   `sudo su - oozie -c 'oozie admin -shareliblist'|  grep 'hive-0.13'` then
	echo '....sharelib deployed successfully...'
else
	echo '....sharelib not  deployed'
fi


