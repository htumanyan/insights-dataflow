# insights-dataflow
Scripts and code for data propagation into Hadoop/Hive/Spark cluster

to deploy, first  checkout this repo on the egde node. cd to the parent directory and run

./scripts/deploy.sh

this should copy the workflows, hqls and library jars to hdfs and run oozie sharelib update. It will also generate workflow files for rpm and psa databases;

to initialize the tables, run 

./scripts/init_database.sh

this script will create the necessary hive databases and tables for static databases (say vdm and manheim for now)

to launch stanadlong oozie jobs, run 

./scripts/launch_stanalone.sh <db_name> [hive] 

this will run the oozie injestion for a single run. currently  you can run ingestions for vdm, rpm, insights and manheim. You can  add the hive parameter to make the job run  through usual mapreduce hive, instead of spark. This is preferrable for vdm and other long running joins. 

to  run a coordinator, run 

./scripts/launch_coordinator.sh <db_name> <frequency> 

this will  run oozie jobs for a db at a given frequency in minutes

