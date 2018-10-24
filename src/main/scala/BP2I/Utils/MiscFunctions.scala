package BP2I.Utils

import BP2I.Utils.FileFunctions.deleteTmpDirectory
import BP2I.Utils.Param.{logger, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object MiscFunctions {

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
    * Goal: take a simple dataframe and transpose it.
    * @param dataFrame
    * @return
    */
  def transposeDF(dataFrame: DataFrame, tableName: String): DataFrame = {

    val numericDataFrame = dataFrame

    val columns = numericDataFrame.columns.toSeq

    val rows = numericDataFrame.collect.map(_.toSeq.toArray).head.toSeq.map(_.toString)

    val transposedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(columns.zip(rows)))
      .withColumn("tableName", lit(tableName))
      .withColumnRenamed("_1", "columnName")
      .withColumnRenamed("_2", "amountOfItems")

    transposedDataFrame
  }

  /**
    * Goal: write a complete report on what happened.
    * 1. What was before: amount of lines, schema.
    * 2. What was added: amount of lines, schema. Amount of Insert, Update and Delete.
    * 3. What is now: amount of lines, schema.
    */
  def writeReportRawLayer(addedTableDF: DataFrame, newTableDF: DataFrame, tableName: String): Unit = {
    import spark.sqlContext.implicits._

    val reportDir = s"./job_reports/report_$tableName"

    deleteTmpDirectory(reportDir)

    val amountOfLinesInNewFile = addedTableDF.count()

    val newSchema = addedTableDF.schema.mkString("\n")

    val amountOfInsert = addedTableDF.filter($"Nature_Action" === "I").count()
    val amountOfUpdate = addedTableDF.filter($"Nature_Action" === "U").count()
    val amountOfDelete = addedTableDF.filter($"Nature_Action" === "D").count()

    val amountOfLinesNow = newTableDF.count()
    val amountOfLinesOld = amountOfLinesNow - amountOfDelete

    spark.sparkContext.parallelize(Seq(
      s"Amount of lines in file red:         $amountOfLinesInNewFile",
      s"Amount of Insert:                    $amountOfInsert",
      s"Amount of Update:                    $amountOfUpdate",
      s"Amount of Delete:                    $amountOfDelete",
      s"Amount of lines in table previously: $amountOfLinesOld",
      s"Amount of lines in table now:        $amountOfLinesNow",
      s"New schema:", newSchema))
        .coalesce(1)
        .saveAsTextFile(reportDir)


    logger.warn("===> WRITING REPORT <===")

    logger.warn("*** FINAL REPORT 1: WHAT WAS ADDED ***")
    println("AMOUNT OF LINES = " + addedTableDF.count)
    println("NEW SCHEMA = ")
    addedTableDF.printSchema()
    println("AMOUNT OF 'INSERT' = " + addedTableDF.filter($"Nature_Action" === "I").count())
    println("AMOUNT OF 'UPDATE' = " + addedTableDF.filter($"Nature_Action" === "U").count())
    println("AMOUNT OF 'DELETE' = " + addedTableDF.filter($"Nature_Action" === "D").count())

    logger.warn("*** FINAL REPORT 2: WHAT IS NOW ***")
    println("AMOUNT OF LINES = " + newTableDF.count)
    println("NEW SCHEMA = ")
    newTableDF.printSchema()

    logger.warn("*** FINAL REPORT 3: EMPTY COLUMNS ***")

    val finalReport = if (newTableDF.columns.contains("summary")) {

      newTableDF
        .withColumnRenamed("summary", "summary_tmp")
        .describe().filter($"summary" === "count").drop("summary")
        .withColumnRenamed("summary_tmp", "summary")

    } else newTableDF.describe().filter($"summary" === "count").drop("summary")

    val finalReportTransposed = transposeDF(finalReport, tableName)

    finalReportTransposed.show(false)

    finalReportTransposed.coalesce(1).write.mode("overwrite").option("header", "true").format("csv").save(s"./job_reporting/$tableName")
  }

  def checkSchemaAppLayer(dataFrame: DataFrame, expectedSchema: StructType, appName: String): Unit = {

  val actualSchema = dataFrame.schema

    if (actualSchema.equals(expectedSchema)) {

      logger.warn(s"===> Schema supplied for $appName does match the expected schema <===")

    } else {

      logger.warn(s"===> Schema supplied for $appName does NOT match the expected schema: update needed <===")
      logger.warn(s"Expected schema: ${expectedSchema.printTreeString()}")
      logger.warn(s"Received schema: ${actualSchema.printTreeString()}")
    }
  }
}
