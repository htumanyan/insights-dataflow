#!/bin/bash

mkdir -p tmp
mkdir -p logs
dt=$(date '+%d%m%Y%H%M%S');

cd tmp && ../scripts/create_tables_parallel.sh ../conf/tables.conf ../logs/$dt.out 
