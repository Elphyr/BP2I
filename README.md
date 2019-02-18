# BP2I Datalake integration job

This job:

1. Integrates files into the datalake from the Raw Layer.

1. Creates Hive tables relative to those files ; or updates existing tables in the Have Layer.

1. Creates Hive tables relative to the datamart model ; or updates existing tables in the App Layer.

## SPARK SUBMIT

    /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode client  --num-executors 3  --driver-memory 2g --executor-memory 2g --class BP2I.IntegrationDatalake.DAG.DataLakeIntegration ./BP2I_Spark-0.4-jar-with-dependencies.jar -p "/user/lc61470/reftec_full_extract/extract_01_19_v2/Datalake/Datalake" -e "dev"

## MVN BUILD

### With dependencies

    mvn clean package

### Without dependencies

    mvn clean compile

### Changelog

0.6 => add the QueryBank for datamart update as a job (18/02/2019)

0.5 => add Java integration job and properties files (17/01/2019)

0.4 => modify how job works to manage new spec (25/11/2018)

0.3 => add job report readable file

0.2 => add job report csv for FX