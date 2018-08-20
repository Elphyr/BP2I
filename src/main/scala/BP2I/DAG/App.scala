package BP2I.DAG

import BP2I.Utils.Functions._
import BP2I.Utils.Param.{REFTEC_DIRECTORY, logger, spark}

object App {

  def main(args: Array[String]): Unit = {

    logger.info("Step 1: initializing table name and .des path")
    val tableName = "REFTEC_CA_COMPANY_02082018_1"
    val desPath = REFTEC_DIRECTORY + "REFTEC_CA_COMPANY_02082018_1.des"

    logger.info("Step 2.1: read the .des file and create Hive query accordingly")
    val hiveQuery = automaticHiveQuery(tableName, desPath)

    logger.info("Step 2.2: this is the Hive query used : " + "\n" + hiveQuery)

    logger.info("Step 3: creating external and internal tables")
    createExternalTableQuery(tableName, hiveQuery) //name of the table: $tableName

    createInternalTableQuery(tableName, hiveQuery) //name of the table: my$tableName

    val sqlDF = spark.sql(s"SELECT * FROM my$tableName")

    val finalDF = removeHeader(sqlDF) //had to remove header because of Spark 2.1

    finalDF.show(false)

    println("COUNT ITEMS == " + finalDF.count()) //should be 491 in this example

    finalDF.printSchema()
  }
}
