package BP2I.IntegrationDatalake.DAG

import BP2I.IntegrationDatalake.Func.HiveFunctions._
import BP2I.IntegrationDatalake.Utils.Params.{logger, spark, warehouseLocation}
import BP2I.Reporting.IntegrationReport.writeReportRawLayer

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
    val newDataTableFullName = newDataTableInformation.takeRight(4).mkString("_").replaceAll("-", "")

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $newDataTableApplication LOCATION " +
      s"'$warehouseLocation/${newDataTableApplication.toLowerCase()}'")
    spark.catalog.setCurrentDatabase(s"${newDataTableApplication.toLowerCase()}")

    logger.warn("Step 3: creating external table")
    createExternalTable(newDataTableApplication, newDataTableFullName, hiveQuery, args)

    val newDataTableDF = spark.sql(s"SELECT * FROM $newDataTableApplication.$newDataTableFullName")

    val tableName = newDataTableInformation(1).replaceAll("-", "")

    logger.warn("Step 4: checking if data table already exists")
    if (spark.catalog.tableExists(s"$tableName")) {

      logger.warn(s"Step 4: table with same name found, updating table named: $tableName")
      val (addedTableDF, newTableDF) = feedNewDataIntoTable(newDataTableApplication, tableName, newDataTableDF, primaryColumn, hiveQuery)

      writeReportRawLayer(addedTableDF, newTableDF, newDataTableInformation)

      logger.warn(s"===> JOB COMPLETED FOR TABLE $tableName <===")

    } else {

      logger.warn(s"Step 4: no table with the same name found, creating new table as '$tableName'")
      dropNatureAction(newDataTableDF, newDataTableApplication, tableName, hiveQuery)

      writeReportRawLayer(newDataTableDF, newDataTableDF, newDataTableInformation)

      logger.warn(s"===> JOB COMPLETED FOR TABLE $tableName <===")
    }
  }
}