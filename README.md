# BP2I Datalake integration job
BP2I datalake.

## SPARK SUBMIT
   spark-submit --master yarn --deploy-mode client --num-executors 1  --driver-memory 8g --executor-memory 8g --class BP2I.DAG.DataLakeIntegration ./BP2I_Spark-0.2-jar-with-dependencies.jar -p "/user/lc61470/reftec_integration_test/"

## MVN BUILD

### With dependencies
   mvn clean package

### Without dependencies
   mvn clean compile

### Changelog

0.4 => modify how job works to manage new spec (25/11/2018)

0.3 => add job report readable file

0.2 => add job report csv for FX