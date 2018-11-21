package BP2I.DAG

import BP2I.Reporting.IntegrationReport.writeReportRawLayer
import BP2I.Utils.HiveFunctions._
import BP2I.Utils.Param.{logger, spark, warehouseLocation}

object IntegrationRawData {

  def main(args: String): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    logger.warn("===> INITIALIZING HIVE TABLES <===")

    logger.warn("Step 1: initializing table name and .des path")
    val desPath = args + "/*.des"
    logger.warn("Step 1: files red: " + "\n" + s"$desPath")

    logger.warn("Step 2: read the .des file and create Hive query accordingly")
    val (newDataTableInformation, primaryColumn, hiveQuery) = readDesFile(desPath)

    val newDataTableApplication = newDataTableInformation.head
    val newDataTableName = newDataTableInformation.takeRight(4).mkString("_").replaceAll("-", "")

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $newDataTableApplication LOCATION '$warehouseLocation/${newDataTableApplication.toLowerCase()}'")

    logger.warn("Step 3: creating external table")
    createExternalTable(newDataTableApplication, newDataTableName, hiveQuery, args)

    logger.warn("Step 4: creating internal table")
    //createInternalTable(newDataTableApplication, newDataTableName + "_int", newDataTableName, hiveQuery)
    val newDataTableDF = spark.sql(s"SELECT * FROM $newDataTableApplication.$newDataTableName")

    val tableName = newDataTableInformation(1).replaceAll("-", "")

    logger.warn("Step 5: checking if data table already exists")
    if (spark.catalog.tableExists(s"$newDataTableApplication.$tableName")) {

      logger.warn(s"Step 5: table with same name found, updating table named: $tableName")
      val (addedTableDF, newTableDF) = feedNewDataIntoTable(newDataTableApplication, tableName, newDataTableDF, primaryColumn, hiveQuery)

      writeReportRawLayer(addedTableDF, newTableDF, newDataTableInformation)

      logger.warn(s"===> JOB COMPLETED FOR TABLE $tableName <===")
    } else {

      logger.warn(s"Step 5: no table with the same name found, creating new table as '$tableName'")
      dropNatureAction(newDataTableDF, newDataTableApplication, tableName, hiveQuery)

      writeReportRawLayer(newDataTableDF, newDataTableDF, newDataTableInformation)

      logger.warn(s"===> JOB COMPLETED FOR TABLE $tableName <===")
    }
  }
}