#!/bin/bash


NUM_MACHINES=`hadoop dfsadmin -report | grep "Live datanodes"| sed 's/.*(\([0-9]*\)):/\1/g'` #we assume that we have equal amount of node manangers and data nodes
eche 'running deploy for '$NUM_MACHINES' nodes'
sudo easy_install  quik 
sudo su -m -l oozie -c '/usr/hdp/current/oozie-server/bin/oozie-setup.sh sharelib create -fs /user/oozie/share/lib/' 
libpath=`sudo su -m -l oozie -c "oozie admin -sharelibupdate | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'"` #find out where is the sharelib
python oozie/generators/workflowGenerator.py rpm $NUM_MACHINES conf/rpm_tables.conf oozie/templates/ oozie/workflows/rpm/
python oozie/generators/workflowGenerator.py psa $NUM_MACHINES conf/psa_tables.conf oozie/templates/ oozie/workflows/psa/
sudo mkdir -p /home/oozie/insights-dataflow/oozie/
sudo cp -r oozie/configs /home/oozie/insights-dataflow/oozie/
sudo chown -R oozie /home/oozie/insights-dataflow/oozie

hadoop fs -copyFromLocal -f hql /user/oozie/share
hadoop fs -copyFromLocal -f oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal -f oozie/standalone_jars/* /user/oozie/share/lib
hadoop fs -copyFromLocal -f oozie/sharelib/*  /user/oozie/share/lib/$libpath/ 
 hadoop fs -copyFromLocal /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/hive-site.xml
 hadoop fs -copyFromLocal /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/oozie-hive-site.xml
sudo su -m -l oozie -c 'oozie admin -sharelibupdate' #update the sharlib for real


