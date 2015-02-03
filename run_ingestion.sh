#!/bin/bash

mkdir -p tmp
mkdir -p logs
dt=$(date '+%d%m%Y%H%M%S');

cd tmp && nohup ../scripts/create_tables_parallel.sh ../conf/tables.conf ../logs/$dt.out 2> ../logs/$dt.err >../logs/$dt.log & 
