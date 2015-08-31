#!/bin/bash 

NAMENODE=$1

hadoop fs -mkdir -p /data/database/
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database insights;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database rpm;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vdm;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database mmr;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database psa;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database manheim;';
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/schemas/sales.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/schemas/inventory.hql

