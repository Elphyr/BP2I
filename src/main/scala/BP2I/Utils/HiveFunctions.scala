package BP2I.Utils

import BP2I.Utils.MiscFunctions._
import BP2I.Utils.Param.{logger, spark}
import org.apache.spark.sql.DataFrame


object HiveFunctions {

  /**
    * Goal: read the .des file, and write a Hive query accordingly.
    * @param desPath
    * @return
    */
  def readDesFile(desPath: String): (String, String, List[String]) = {
  import spark.sqlContext.implicits._

    val desDF = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(desPath)

    val tableName = getFileName(desDF, ".des")
    logger.warn("Step 2: this is the core component of the name of the tables created: " + "\n" + tableName)

    val columns = desDF.select("COLUMN_NAME").map(x => x.getString(0)).collect.toList

    val types = desDF.select("DATA_TYPE").map(x => x.getString(0)).collect.toList

    val primaryColumn = desDF.filter($"IS_NULLABLE" === "NO").select("COLUMN_NAME").map(x => x.getString(0)).collect.toList.head

    val adaptedTypes = adaptTypes(types)

    var columnsAndTypes = List[String]("Nature_Action CHAR")
    for (x <- 0 until desDF.count().toInt) {

      columnsAndTypes ::= columns(x) + " " + adaptedTypes(x)
    }

    val orderedColumnsAndTypes = columnsAndTypes.reverse
    logger.warn("Step 2: this is the Hive query used : " + "\n" + orderedColumnsAndTypes.mkString(", "))

    (tableName, primaryColumn, orderedColumnsAndTypes)
  }

  /**
    * Goal: create an external table that loads the data in the right directory.
    * @param tableName
    * @param columnsAndTypes
    * @return
    */
  def createExternalTableQuery(tableName: String, columnsAndTypes: List[String], dataDirPath: String): DataFrame = {

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    val externalTableQuery = s"CREATE EXTERNAL TABLE $tableName (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE " +
      s"LOCATION '$dataDirPath*.dat' "

    spark.sql(externalTableQuery)
  }

  /**
    * Goal: create an internal table that matches the data loaded in the external table.
    * @param tableName
    * @param columnsAndTypes
    * @return
    */
  def createInternalTableQuery(tableName: String, columnsAndTypes: List[String]): DataFrame = {

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

    types.map { case "nvarchar" => "VARCHAR" ;  case "binary" => "STRING" ; case "timestamp" => "STRING" ; case "ntext" => "STRING" ; case x => x.toUpperCase }
  }

  /**
    * Goal: remove the now-useless column 'Nature_Action' and write the dataframe into the main one.
    * @param dataFrame
    * @param tableName
    * @param columnsAndTypes
    */
  def dropNatureAction(dataFrame: DataFrame, tableName: String, columnsAndTypes: List[String]): DataFrame = {

    val columnsAndTypesWONatureAction = columnsAndTypes.filterNot(_.contains("Nature_Action"))

    val finalDF = dataFrame.drop("Nature_Action")

    val internalTableQuery = s"CREATE TABLE IF NOT EXISTS $tableName (" +
      s"${columnsAndTypesWONatureAction.mkString(", ")}) " +
      s"STORED AS PARQUET"

    spark.sql(internalTableQuery)

    finalDF.write.insertInto(s"$tableName")

    finalDF
  }

  /**
    * Goal: when you add informations into a table, it checks if there are new columns and then merge both tables.
    * The past records without the new column have a 'null' put into the columns.
    * @param tableName
    * @param newDataTable
    * @param columnsAndTypes
    */
  def feedNewDataIntoTable(tableName: String, newDataTable: DataFrame, primaryColumn: String, columnsAndTypes: List[String]): DataFrame = {

    val tmpDir = "/home/raphael/workspace/BP2I_Spark/tmp_newTable"

    deleteTmpDirectory(tmpDir)

    val currentTableDF = spark.sql(s"SELECT * FROM $tableName")

    val newTableDF = unionDifferentTables(currentTableDF, newDataTable)
      .distinct()

    val filterDeletedLinesNewTableDF = checkForDeletes(newTableDF, primaryColumn)

    val filteredNewTableDF = checkForUpdates(filterDeletedLinesNewTableDF, primaryColumn)
        .drop("Nature_Action")

    filteredNewTableDF.write.parquet(s"$tmpDir")

    val columnsAndTypesWONatureAction = columnsAndTypes.filterNot(_.contains("Nature_Action"))

    val externalTableQuery = s"CREATE EXTERNAL TABLE ${tableName}_tmp (" +
      s"${columnsAndTypesWONatureAction.mkString(", ")}) " +
      s"STORED AS PARQUET " +
      s"LOCATION '$tmpDir'"

    spark.sql(externalTableQuery)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    val internalTableQuery = s"CREATE TABLE $tableName (" +
      s"${columnsAndTypesWONatureAction.mkString(", ")}) " +
      s"STORED AS PARQUET"

    spark.sql(internalTableQuery)

    spark.sql(s"INSERT OVERWRITE TABLE $tableName SELECT * FROM ${tableName}_tmp")

    val finalTableDF = spark.sql(s"SELECT * FROM $tableName")

    spark.sql(s"DROP TABLE IF EXISTS ${tableName}_tmp")

    deleteTmpDirectory(tmpDir)

    finalTableDF
  }
}