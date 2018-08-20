package BP2I.Utils

import BP2I.Utils.MiscFunctions.getFileName
import BP2I.Utils.Param.{REFTEC_DIRECTORY, logger, spark}
import org.apache.spark.sql.DataFrame

object HiveFunctions {


  def writeAutoHiveQuery(desPath: String): (String, List[String]) = {
  import spark.sqlContext.implicits._

    val desDF = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(desPath)

    val tableName = getFileName(desDF)
    logger.info("Step 2: this is the name of the tables created : " + "\n" + tableName)

    val columns = desDF.select("COLUMN_NAME").map(x => x.getString(0)).collect.toList

    val types = desDF.select("DATA_TYPE").map(x => x.getString(0)).collect.toList

    val adaptedTypes = adaptTypes(types)

    var hiveQuery = List[String]()
    for (x <- 0 until desDF.count().toInt) {

      hiveQuery ::= columns(x) + " " + adaptedTypes(x)
    }

    val finalHiveQuery = hiveQuery.reverse

    logger.info("Step 2: this is the Hive query used : " + "\n" + finalHiveQuery)

    (tableName, finalHiveQuery)
  }

  /**
    * Goal: create an external table that loads the data in the right directory.
    * TODO : add the directory to the parameters?
    * @param tableName
    * @param columnsAndTypes
    * @return
    */
  def createExternalTableQuery(tableName: String, columnsAndTypes: List[String]): DataFrame = {

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    val externalTableQuery = s"CREATE EXTERNAL TABLE $tableName (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE " +
      s"LOCATION '$REFTEC_DIRECTORY/dat/' "

    spark.sql(externalTableQuery)
  }

  /**
    * Goal: create an internal table that matches the data loaded in the external table.
    * @param tableName
    * @param columnsAndTypes
    * @return
    */
  def createInternalTableQuery(tableName: String, columnsAndTypes: List[String]): DataFrame = {

    spark.sql(s"DROP TABLE IF EXISTS my$tableName")
    spark.sql(s"DROP TABLE IF EXISTS ${tableName}_int")

    val internalTableQuery = s"CREATE TABLE ${tableName}_int (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"STORED AS PARQUET"

    spark.sql(internalTableQuery)
    spark.sql(s"INSERT OVERWRITE TABLE ${tableName}_int SELECT * FROM $tableName")
  }

  /**
    * Goal: change types to make Hive understand them.
    * nvarchar => STRING ; binary => STRING ; timestamp => STRING
    * TODO : find better options, because 'binary' and 'timestamp' should be kept.
    * @param types
    * @return
    */
  def adaptTypes(types: List[String]): List[String] = {

    types.map { case "nvarchar" => "STRING" ; case "binary" => "STRING" ; case "timestamp" => "STRING" ; case x => x.toUpperCase }
  }
}
