#!/bin/bash
NUM_MACHINES=`hadoop dfsadmin -report | grep "Live datanodes"| sed 's/.*(\([0-9]*\)):/\1/g'` #we assume that we have equal amount of node manangers and data nodes
echo 'running deploy for '$NUM_MACHINES' nodes'
python oozie/generators/workflowGenerator.py rpm $NUM_MACHINES conf/rpm_tables.conf oozie/templates/ oozie/workflows/rpm/
python oozie/generators/workflowGenerator.py psa $NUM_MACHINES conf/psa_tables.conf oozie/templates/ oozie/workflows/psa/
hadoop fs -mkdir /data/database/3rd_party/dma_bnd/
hadoop fs -copyFromLocal data/dma_bnd4.csv /data/database/3rd_party/dma_bnd/dma_bnd.csv
hadoop fs -copyFromLocal -f hql /user/oozie/share
hadoop fs -copyFromLocal -f oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal -f oozie/coordinators /user/oozie/share

