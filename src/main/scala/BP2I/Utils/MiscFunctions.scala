package BP2I.Utils

import BP2I.Utils.Param.{logger, spark}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{input_file_name, _}
import org.apache.spark.sql.{Column, DataFrame}

object MiscFunctions {

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
    * Goal: get the file name from the whole path and remove the extension (.des, .dat, etc.).
    * @param dataFrame
    * @param extension
    * @return
    */
  def getFileName(dataFrame: DataFrame, extension: String): String = {
  import spark.sqlContext.implicits._

    val fileName = dataFrame
      .select(input_file_name()).map(x => x.getString(0)).collect().toList.last
      .split("/").last
      .replaceAll(extension, "")

    fileName
  }

  /**
    * Goal: split the full file name to get the true name of the table.
    * Full file name: Application_Table_Date_Heure_Version
    * Example: REFTEC_CA_COMPANY_02082018_1 => REFTEC_CA_COMPANY (main table) + 02082018 (date) + 1 (version)
    * @param fullFileName
    * @return
    */
  def splitFullFileName(fullFileName: String): Seq[String] = {

    val splitFileName = fullFileName.split("_[0-9]")

    splitFileName
  }

  /**
    * Goal: when a table is updated, we need to check weather or not there are new columns and update the schema accordingly.
    * Example: first day we receive TABLE (id STRING, value INT), next day we receive TABLE (id STRING, value INT, location STRING)
    * The TABLE will automatically update itself into TABLE (id STRING, value INT, location STRING) and put 'null' into column location
    * for older records.
    * @param df1
    * @param df2
    * @return
    */
  def unionDifferentTables(df1: DataFrame, df2: DataFrame): DataFrame = {

    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val total = cols1 ++ cols2

    val order = df1.columns ++ df2.columns
    val sortOrder = total.toList.sortWith((a, b) => order.indexOf(a) < order.indexOf(b))

    def compareColumns(myCols: Set[String], allCols: List[String]): List[Column] = {

      allCols.map {
        case x if myCols.contains(x) => col(x)
        case y => lit(null).as(y)
      }
    }

    df1.select(compareColumns(cols1, sortOrder): _*).union(df2.select(compareColumns(cols2, sortOrder): _*))
  }

  /**
    * Goal: if a table with 'Nature_Action' == 'D' is uploaded, check for all lines with the same primary column and delete them.
    * @param dataFrame
    * @param primaryColumn
    * @return
    */
  def checkForDeletes(dataFrame: DataFrame, primaryColumn: String): DataFrame = {
    import spark.sqlContext.implicits._

    val linesToDelete = dataFrame.filter($"Nature_Action" === "D")
      .select(primaryColumn)
      .map(x => x.getString(0)).collect.toList

    val filteredDF = dataFrame.filter(!col(primaryColumn).isin(linesToDelete:_*))

    filteredDF
  }

  /**
    * Goal: if a table with 'Nature_Action' == 'U' is uploaded, check for similar lines and delete them.
    * @param dataFrame
    * @param primaryColumn
    * @return
    */
  def checkForUpdates(dataFrame: DataFrame, primaryColumn: String): DataFrame = {
    import spark.sqlContext.implicits._

    val filteredDF = dataFrame.orderBy($"Nature_Action".desc_nulls_last)
        .dropDuplicates(primaryColumn)
        .drop("Nature_Action")

    filteredDF
  }

  /**
    * Goal: check if a directory exists, and delete it afterward. Works with HDFS directories.
    * @param path
    * @return
    */
  def deleteTmpDirectory(path: String): AnyVal = {

    val hadoopFileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(hadoopFileSystem.exists(new Path(path)))
      hadoopFileSystem.delete(new Path(path), true)
  }

  /**
    * Goal: write a complete report on what happened.
    * 1. What was before: amount of lines, schema.
    * 2. What was added: amount of lines, schema. Amount of Insert, Update and Delete.
    * 3. What is now: amount of lines, schema.
    */
  def writeReport(oldTableDF: DataFrame, addedTableDF: DataFrame, newTableDF: DataFrame): Unit = {
    import spark.sqlContext.implicits._

    logger.warn("*** FINAL REPORT 1: WHAT WAS BEFORE ***")
    println("AMOUNT OF LINES = " + oldTableDF.count)

    println("OLD SCHEMA = ")
    oldTableDF.printSchema()

    logger.warn("*** FINAL REPORT 2: WHAT WAS ADDED ***")
    println("AMOUNT OF LINES = " + addedTableDF.count)
    println("NEW SCHEMA = ")
    addedTableDF.printSchema()
    println("AMOUNT OF 'INSERT' = " + addedTableDF.filter($"Nature_Action" === "I").count())
    println("AMOUNT OF 'UPDATE' = " + addedTableDF.filter($"Nature_Action" === "U").count())
    println("AMOUNT OF 'DELETE' = " + addedTableDF.filter($"Nature_Action" === "D").count())

    logger.warn("*** FINAL REPORT 3: WHAT IS NOW ***")
    println("AMOUNT OF LINES = " + newTableDF.count)
    println("NEW SCHEMA = ")
    newTableDF.printSchema()
  }
}
