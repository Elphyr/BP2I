package BP2I.IntegrationDatalake.Func

import BP2I.IntegrationDatalake.Func.FileFunctions.{deleteTmpDirectory, getFileInformation}
import BP2I.IntegrationDatalake.Func.MiscFunctions.{checkForDeletes, checkForUpdates, unionDifferentTables}
import BP2I.IntegrationDatalake.Utils.Params.{logger, spark}
import BP2I.IntegrationDatalake.Utils.ScalaProperties
import org.apache.spark.sql.DataFrame

object HiveFunctions {

  /**
    * Goal: read the .des file, and write a Hive query accordingly.
    * @param desPath
    * @return
    */
  def readDesFile(desPath: String): (Seq[String], String, List[String]) = {
  import spark.sqlContext.implicits._

    val desDF = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(desPath)
      .drop("IS_NULLABLE")
    // TODO : remove it when reftec follows the specs

    val desDFColumnsName = desDF.columns.head
    val deDFColumnsType = desDF.columns(1)

    val tableInformation = getFileInformation(desDF, ".des")

    val columns = desDF.select(desDFColumnsName).map(x => x.getString(0)).collect.toList

    val types = desDF.select(deDFColumnsType).map(x => x.getString(0)).collect.toList

    val primaryColumn = desDF.select(desDFColumnsName).map(x => x.getString(0)).collect.toList
      .filter(x => x.toLowerCase().contains("id") || x.toLowerCase().contains("name") || x.toLowerCase != "nature_action")
      .head

    val adaptedTypes = adaptTypes(types)

    var columnsAndTypes = List[String]()
    for (x <- 0 until desDF.count().toInt) {

      columnsAndTypes ::= columns(x) + " " + adaptedTypes(x)
    }

    val orderedColumnsAndTypes = columnsAndTypes.reverse
    logger.warn("Step 2: this is the columns and types red from the file: " + "\n" + orderedColumnsAndTypes.mkString(", "))

    (tableInformation, primaryColumn, orderedColumnsAndTypes)
  }

  /**
    * Goal: create an external table that loads the data in the right directory.
    * @param tableName
    * @param columnsAndTypes
    * @param dataDirPath
    * @param dataFormat
    * @return
    */
  def createExternalTable(tableApplication: String, tableName: String, columnsAndTypes: List[String], dataDirPath: String, dataFormat: String = "csv", delTmpDir: Boolean = true): Unit = {

    spark.sql(s"DROP TABLE IF EXISTS $tableApplication.$tableName")

    val format = dataFormat.toLowerCase() match {

      case "parquet" => "STORED AS PARQUET"

      case "orc" => "STORED AS ORC"

      case "csv" => "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"

      case _ => logger.warn("Format must be either 'csv', 'parquet' or 'orc', chose 'csv' by default")

        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"
    }

    val dataDirPathFinal =
    if (delTmpDir) dataDirPath + "/*.dat"
    else dataDirPath

    val externalTableQuery = s"CREATE EXTERNAL TABLE $tableApplication.$tableName (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"$format " +
      s"LOCATION '$dataDirPathFinal' "

    spark.sql(externalTableQuery)

    if (delTmpDir) deleteTmpDirectory(dataDirPathFinal)
  }

  /**
    * Goal: create an internal table that matches the data loaded in the external table.
    * @param tableName
    * @param extTableName
    * @param columnsAndTypes
    * @param dataFormat
    * @return
    */
  def createInternalTable(tableApplication: String, tableName: String, extTableName: String, columnsAndTypes: List[String], dataFormat: String = "csv"): DataFrame = {

    spark.sql(s"DROP TABLE IF EXISTS $tableApplication.$tableName")

    val format = dataFormat.toLowerCase() match {

      case "parquet" => "STORED AS PARQUET"

      case "orc" => "STORED AS ORC"

      case "csv" => "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"

      case _ => logger.warn("Format must be either 'csv', 'parquet' or 'orc', chose 'csv' by default")

        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"
    }

    val internalTableQuery = s"CREATE TABLE $tableApplication.$tableName (" +
      s"${columnsAndTypes.mkString(", ")}) " +
      s"$format"

    spark.sql(internalTableQuery)

    spark.sql(s"INSERT OVERWRITE TABLE $tableApplication.$tableName SELECT * FROM $tableApplication.$extTableName")
  }

  /**
    * Goal: change types to make Hive understand them.
    * nvarchar => STRING ; binary => STRING ; timestamp => STRING
    * TODO : find better options, because 'binary' and 'timestamp' should be kept.
    * @param types
    * @return
    */
  def adaptTypes(types: List[String]): List[String] = {

    types.map { case "nvarchar" => "STRING" ; case "varchar" => "STRING" ; case "char" => "STRING" ; case "nchar" => "STRING" ;
    case "binary" => "STRING" ; case "varbinary" => "STRING" ; case "timestamp" => "STRING" ; case "datetime" => "STRING" ;
    case "ntext" => "STRING"; case "image" => "STRING" ; case "money" => "DOUBLE" ; case x => x.toUpperCase }
  }

  /**
    * Goal: remove the now-useless column 'Nature_Action' and write the dataframe into the main one.
    * @param dataFrame
    * @param tableName
    * @param columnsAndTypes
    */
  def dropNatureAction(dataFrame: DataFrame, tableApplication: String, tableName: String, columnsAndTypes: List[String]): DataFrame = {

    val finalDF = dataFrame.drop("Nature_Action")

    finalDF.createOrReplaceTempView(s"${tableName}_tmp")

    spark.sql(s"DROP TABLE IF EXISTS $tableApplication.$tableName")
    spark.sql(s"CREATE TABLE $tableApplication.$tableName AS SELECT * FROM ${tableName}_tmp")

    finalDF
  }

  /**
    * Goal: when you add information into a table, it checks if there are new columns and then merge both tables.
    * The past records without the new column have a 'null' put into the columns.
    * @param tableName
    * @param newDataTable
    * @param columnsAndTypes
    */
  def feedNewDataIntoTable(tableApplication: String, tableName: String, newDataTable: DataFrame, primaryColumn: String, columnsAndTypes: List[String]): (DataFrame, DataFrame) = {

    val tmpDir = ScalaProperties.hiveTmpDir

    deleteTmpDirectory(tmpDir)

    val currentTableDF = spark.sql(s"SELECT * FROM $tableName")

    val newTableDF = unionDifferentTables(currentTableDF, newDataTable)
      .distinct()

    val filterDeletedLinesNewTableDF = checkForDeletes(newTableDF, primaryColumn)

    val filteredNewTableDF = checkForUpdates(filterDeletedLinesNewTableDF, primaryColumn)
      .drop("Nature_Action")

    filteredNewTableDF.write.parquet(s"$tmpDir")

    val columnsAndTypesWONatureAction = columnsAndTypes.filterNot(_.contains("Nature_Action"))

    createExternalTable(tableApplication, tableName + "_tmp", columnsAndTypesWONatureAction, tmpDir, "parquet", delTmpDir = false)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    createInternalTable(tableApplication, tableName, tableName + "_tmp", columnsAndTypesWONatureAction)

    spark.sql(s"INSERT OVERWRITE TABLE $tableName SELECT * FROM ${tableName}_tmp")

    val finalTableDF = spark.sql(s"SELECT * FROM $tableName")

    spark.sql(s"DROP TABLE IF EXISTS ${tableName}_tmp")

    deleteTmpDirectory(tmpDir)

    (newDataTable, finalTableDF)
  }
}
