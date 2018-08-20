package BP2I.DAG

import BP2I.Utils.HiveFunctions._
import BP2I.Utils.MiscFunctions.removeHeader
import BP2I.Utils.Param.{REFTEC_DIRECTORY, logger, spark}

object App {

  def main(args: Array[String]): Unit = {

    logger.info("Step 1: initializing table name and .des path")
    val desPath = REFTEC_DIRECTORY + "*.des"

    logger.info("Step 2: read the .des file and create Hive query accordingly")
    val (tableName, hiveQuery) = writeAutoHiveQuery(desPath)

    logger.info("Step 3: creating external table")
    createExternalTableQuery(tableName, hiveQuery) //name of the table: $tableName

    logger.info("Step 4: creating internal table")
    createInternalTableQuery(tableName, hiveQuery) //name of the table: $tableName_int

    val sqlDF = spark.sql(s"SELECT * FROM ${tableName}_int")

    val finalDF = removeHeader(sqlDF) //had to remove header because of Spark 2.1

    finalDF.show(false)

    println("COUNT ITEMS == " + finalDF.count()) //should be 491 in this example

    finalDF.printSchema()
  }
}