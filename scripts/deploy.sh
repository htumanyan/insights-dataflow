#!/bin/bash


NUM_MACHINES=$1


sudo pip install quik 
sudo su - oozie -c '/usr/hdp/current/oozie-server/bin/oozie-setup.sh sharelib create -fs /user/oozie/share/lib/' 
libpath=`sudo su - oozie -c 'oozie admin -sharelibupdate' | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'` #find out where is the sharelib
python oozie/generators/workflowGenerator.py rpm $NUM_MACHINES conf/rpm_tables.conf oozie/templates/ oozie/workflows/rpm/
python oozie/generators/workflowGenerator.py psa $NUM_MACHINES conf/psa_tables.conf oozie/templates/ oozie/workflows/psa/
hadoop fs -copyFromLocal -f hql /user/oozie/share
hadoop fs -copyFromLocal -f oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal -f oozie/standalone_jars/* /user/oozie/share/lib
hadoop fs -copyFromLocal -f oozie/sharelib/*  /user/oozie/share/lib/$libpath/ 
 hadoop fs -copyFromLocal /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/hive-site.xml
 hadoop fs -copyFromLocal /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/oozie-hive-site.xml
sudo su - oozie -c 'oozie admin -sharelibupdate' #update the sharlib for realz
sudo su - oozie -c 'oozie admin -shareliblist'|  grep 'hive-0.13'
if   `sudo su - oozie -c 'oozie admin -shareliblist'|  grep 'hive-0.13'`
then
	echo '....sharelib deployed successfully...'
else
	echo '....sharelib not  deployed'
fi



