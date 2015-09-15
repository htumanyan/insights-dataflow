#!/bin/bash 

db_name=$1
table_name=$2
#assume partitoons are year, month and day

for line in `hadoop fs -ls /data/database/$db_name/$table_name | grep database | awk '{print $8}'` ; do 
	arr=(${line//\// })
	year=${arr[4]}
	month=${arr[3]}
	year=${arr[4]}
	echo $year | $month | $year
done

