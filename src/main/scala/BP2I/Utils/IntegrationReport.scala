package BP2I.Utils

import BP2I.Utils.FileFunctions.deleteTmpDirectory
import BP2I.Utils.MiscFunctions.transposeDF
import BP2I.Utils.Param.{logger, spark}
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

object IntegrationReport {

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
}
