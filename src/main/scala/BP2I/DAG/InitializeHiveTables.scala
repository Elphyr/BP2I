package BP2I.DAG

import BP2I.Utils.Arguments
import BP2I.Utils.HiveFunctions._
import BP2I.Utils.MiscFunctions.{splitFullFileName, writeReport}
import BP2I.Utils.Param.{logger, spark}

object InitializeHiveTables {

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    logger.warn("===> INITIALIZING HIVE TABLES <===")

    val dataDir = Arguments(args).folder.get

    logger.warn("Step 1: initializing table name and .des path")
    val desPath = dataDir + "*.des"
    logger.warn("Step 1: files red: " + "\n" + s"$desPath")

    logger.warn("Step 2: read the .des file and create Hive query accordingly")
    val (newDataTableName, primaryColumn, hiveQuery) = readDesFile(desPath)

    logger.warn("Step 3: creating external table")
    createExternalTable(newDataTableName, hiveQuery, dataDir + "*.dat")

    logger.warn("Step 4: creating internal table")
    createInternalTable(newDataTableName + "_int", newDataTableName, hiveQuery)
    val newDataTableDF = spark.sql(s"SELECT * FROM ${newDataTableName}_int")

    val tableName = splitFullFileName(newDataTableName).head

    logger.warn("Step 5: checking if data table already exists")
    if (spark.catalog.tableExists(s"$tableName")) {

      logger.warn(s"Step 5: table with same name found, updating table named: $tableName")
      val (oldTableDF, addedTableDF, newTableDF) = feedNewDataIntoTable(tableName, newDataTableDF, primaryColumn, hiveQuery)

      //writeReport(oldTableDF, addedTableDF, newTableDF)

      spark.sql("SHOW TABLES").show(100, false)
      spark.sql("SHOW TABLES").count()
      
    } else {

      logger.warn(s"Step 5: no table with the same name found, creating new table as '$tableName'")
      dropNatureAction(newDataTableDF, tableName, hiveQuery)

      spark.sql("SHOW TABLES").show(100, false)
      spark.sql("SHOW TABLES").count()

    }
  }
}