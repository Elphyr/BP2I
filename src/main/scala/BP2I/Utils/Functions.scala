package BP2I.Utils

import BP2I.Utils.Param.{REFTEC_DIRECTORY, spark}
import org.apache.spark.sql.DataFrame

object Functions {

  /**
    * Goal: by reading a .des file, build a Hive query with the right types.
    * @param tableName
    * @param desPath
    * @return
    */
  def automaticHiveQuery(tableName: String, desPath: String): List[String] = {
  import spark.sqlContext.implicits._

    val desDF = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(desPath)

    val columns = desDF.select("COLUMN_NAME").map(x => x.getString(0)).collect.toList

    val types = desDF.select("DATA_TYPE").map(x => x.getString(0)).collect.toList

    val adaptedTypes = adaptTypes(types)

    var columnsAndTypes = List[String]()
    for (x <- 0 until desDF.count().toInt) {

      columnsAndTypes ::= columns(x) + " " + adaptedTypes(x)
    }

    columnsAndTypes.reverse
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

    val internalTableQuery = s"CREATE TABLE my$tableName (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"STORED AS PARQUET"

    spark.sql(internalTableQuery)
    spark.sql(s"INSERT OVERWRITE TABLE my$tableName SELECT * FROM $tableName")
  }

  /**
    * Spark 2.1 keeps the header whatever we do, we need to remove it.
    * @param sqlDF
    * @return
    */
  def removeHeader(sqlDF: DataFrame): DataFrame = {

    val header = sqlDF.first()

    val sqlDFWOHeader = sqlDF.filter(row => row != header)

    sqlDFWOHeader
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
