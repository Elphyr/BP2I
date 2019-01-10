package BP2I.IntegrationDatalake.Func

import BP2I.IntegrationDatalake.AppLayer.{AppLayerQueryBank, AppLayerSchemaBank}
import BP2I.IntegrationDatalake.Utils.Params.{logger, spark}
import org.apache.spark.sql.functions.{col, lit}
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
    *
    * @param dataFrame
    * @param tableName
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
    * Goal: invoke right query from the query bank.
    * @param appName
    * @return
    */
  def getAppLayerQuery(appName: String): String = {

    val appNameWQuery = appName + "Query"

    val listOfQueries = AppLayerQueryBank.getClass.getDeclaredMethods.map(_.getName)

    val rightQuery = listOfQueries.filter(_.equals(appNameWQuery)).head

    AppLayerQueryBank.getClass.getDeclaredMethod(rightQuery).invoke(AppLayerQueryBank).asInstanceOf[String]
  }

  /**
    * Goal: invoke right schema from the schema bank.
    * @param appName
    * @return
    */
  def getAppNameSchema(appName: String): StructType = {

    val appNameWSchema = appName + "Schema"

    val listOfSchemas = AppLayerSchemaBank.getClass.getDeclaredMethods.map(_.getName)

    val rightSchema = listOfSchemas.filter(_.equals(appNameWSchema)).head

    AppLayerSchemaBank.getClass.getDeclaredMethod(rightSchema).invoke(AppLayerSchemaBank).asInstanceOf[StructType]
  }

  /**
    * Goal: check if the schema is still coherent with the one we already registered.
    * @param dataFrame
    * @param appName
    */
  def checkSchemaAppLayer(dataFrame: DataFrame, appName: String): Unit = {

    val expectedSchema = getAppNameSchema(appName)

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
