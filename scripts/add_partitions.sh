#!/bin/bash 

db_name=$1
table_name=$2
namenode_host=$3
#assume partitoons are year, month and day

for line in `hadoop fs -ls /data/database/$db_name/$table_name/*/ | grep database | awk '{print $8}'` ; do 
	echo $line
	arr=(${line//\// })
	year=${arr[4]}
	month=${arr[5]}
	day=${arr[6]}
	echo $year $month $day
	#/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$namenode_host:10000 -e "alter table ${db_name}.${table_name} add if not  exists partition (${year}, ${month}, ${day}) location '/data/database/${db_name}/$table_name/${year}/${month}/${day}/'"
	/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$namenode_host:10000 -e "alter table ${db_name}.${table_name} add if not  exists partition (${year}, ${month}) location '/data/database/${db_name}/$table_name/${year}/${month}/'"
done

