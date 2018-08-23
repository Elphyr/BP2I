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
    * Goal: split the full file name to get informations.
    * Full file name: Application_Table_Date_Heure_Version
    * Example: REFTEC_CA_COMPANY_02082018_1
    */
  def splitFullFileName(fullFileName: String): Seq[String] = {

    val splitFileName = fullFileName.split("_[0-9]")

    splitFileName
  }
}
