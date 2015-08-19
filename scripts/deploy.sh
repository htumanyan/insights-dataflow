#!/bin/bash

$NAMENODE=$1
oozie admin -sharelibcreate 
$libpath=`oozie admin -sharelibupdate | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'` #find out where is the sharelib
hadoop fs -copyFromLocal hql /user/oozie/share
hadoop fs -copyFromLocal oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal oozie/standalone_jars /user/oozie/share/lib
hadoop fs -copyFromLocal oozie/sharelib/*  /user/oozie/share/lib/$libpath/ 
oozie admin -sharelibupdate #update the sharlib for realz
oozie admin -shareliblist|  grep 'hive-0.13'
if oozie admin -shareliblist|  grep 'hive-0.13' then
	echo '....sharelib deployed successfully...'
else
	echo '....sharelib not  deployed'
fi



