package BP2I.Utils

import BP2I.Utils.Param.spark
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.input_file_name

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
    import org.apache.spark.sql.functions._

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
}
