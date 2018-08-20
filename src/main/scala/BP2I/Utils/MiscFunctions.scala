package BP2I.Utils

import BP2I.Utils.Param.spark
import org.apache.spark.sql.DataFrame
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

  def getFileName(dataFrame: DataFrame): String = {
  import spark.sqlContext.implicits._

    val fileName = dataFrame
      .select(input_file_name()).map(x => x.getString(0)).collect().toList.last
      .split("/").last
        .replaceAll(".des", "")

    fileName
  }
}
