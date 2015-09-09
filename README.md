# insights-dataflow
Scripts and code for data propagation into Hadoop/Hive/Spark cluster

# Deploing oozie

to deploy, first  checkout this repo on the egde node. cd to insights-dataflowdirectory and run
```
./scripts/deploy.sh
```
this should copy the workflows, hqls and library jars to hdfs and run oozie sharelib update. It will also generate workflow files for rpm and psa databases;

to initialize the tables, run 
```
./scripts/init_database.sh <namenode host name>
```

this script will create the necessary hive databases and tables for static databases (say vdm and manheim for now)

to launch standalon oozie jobs, run 
````
./scripts/launch_stanalone.sh <db_name> [hive] 
```
this will run the oozie injestion for a single run. currently  you can run ingestions for vdm, rpm, insights. manheim insert is included in the insights script.
. You can  add the hive parameter to make the job run  through usual mapreduce hive, instead of spark. This is preferrable for vdm and other long running joins. 

to  run a coordinator, run 
```
./scripts/launch_coordinator.sh <db_name> <frequency> 
```
this will  run oozie jobs for a db at a given frequency in minutes

both for coordinator and the standalone job, the script execution will print the oozie job id. 
to check the status of the job, sudo to oozie, and run 
```
oozie job -info <job/coordinator id>
```
for a running job it will state the hadoop job id. to check the logs of the job in case of a failure, first run 

```
hadoop job -logs <hadoop job id>
```

find out the attempt id and run the command again 

```
hadoop job -logs <hadoop job id> <attempt id>
```

# adding new tables and databases

To add a new table and database, you need to write a hql script that will create the database. an example of a script that creates a external table is vdm.options,located in hql/vdm/options.hql
````
drop  table if exists vdm.options;
CREATE EXTERNAL TABLE `vdm.options`(
`b_vehicle_id` string,
`vb_model_year` string,
`vb_make` string,
`vb_model` string,
`o_option_id` int,
`o_option_code` string,
`o_option_description` string,
`o_option_group` string,
`o_standard_feature_ind` int ,
`o_style_id` int ,
`o_defining_source` string ,
`o_description_type_code` string,
`o_categorytypefilter` string,
`po_package_id` int,
`po_option_id` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/vdm/options/*.dsv'
tblproperties ("skip.header.line.count"="1");

```

pay attention to the field terminator ('|') and the location of the data. Note that  '\n' is the only acceptable terminator. Also, make sure that  you remove the header line from  the csv file, because the  skip.header.line.count doesnt quite work. 

put the new hql scripts in hql directory, under a directory that corresponds to the db name. 

once you have the script ready you can test it by running beeline with  -f parameter

```
 /usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://stg-na-lxhdn01:13001/ -f hql/vdm/options.hql
```

check that all is good by running a count query on the database. if all works well add the new hql to the deploy script in scripts/deploy.sh

after this depending on the type of the data, you can start integrating the new data source into insights tables - inventory and sales. 

if the new data corresponds to new columns in the sales and inventory  tables, just add the new columns to hql/insights/sales_cached.hql and  hql/insights/inventory_cached.hql with appropriate joins. 

if the data is new rows for inventory and sales, you have to create an insert script in which you populate all the columns. a good example of this is hql/manheim/insert.hql 

in the case of new rows you have to make sure that the new rows are added to the dataset when the insights tables are recreated on periodic runs. TO do this add a step to the oozie workflow for insights in oozie/workflows/insights/workflow.xml. See the third step for the example. 

In general, if the data is pushed from an external source and doesnt require pre-processing after loading, you dont need to change anything for oozie, since external tables are updated when the data changes on hdfs. However, if there is pre-processing required( as in case of vdm when we join options and packages), you have to create a new workflow.  


