#!/bin/bash

table_list=$1
log_file=$2
db_name=$3
warehouse_dir="/data/database/"$db_name"/"

parallel -a $table_list --colsep '\^' -j 16 --joblog $log_file 'hadoop fs -rm -R -skipTrash '$warehouse_dir'{1}; sqoop import -libjars=/home/rmsadmin/insights-dataflow/oozie/standalone_jars/mysql-connector-java.jar --connect "jdbc:mysql://dev-na-LXHDE01/rpm" --username insights --password     M@ins3a  --table {1}  -m {2} --hive-import  --hive-overwrite --hive-table '$db_name'.{1}_stg {3} {4} {5} {6} {7}  --warehouse-dir '$warehouse_dir

