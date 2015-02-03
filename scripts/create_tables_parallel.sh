#!/bin/bash

scripts_path="/util/lib/data_import"
table_list=$1
log_file=$2
db_name="psa"
warehouse_dir="/data/database/"$db_name"/"

parallel -a $table_list --colsep '\^' -j 16 --joblog $log_file 'hadoop fs -rm -R -skipTrash '$warehouse_dir'{1}; sqoop import -libjars=/home/hdfs/lib/data_import/jars/sqljdbc4.jar --connect "jdbc:sqlserver://10.133.10.6:1433;databaseName=Hadoop_Data;" --username Hadoop --password RMS_H@d00p --table {1}  -m {2} --hive-import  --hive-overwrite --hive-table '$db_name'.{1}_stg {3} {4} {5} {6} {7}  --warehouse-dir '$warehouse_dir

