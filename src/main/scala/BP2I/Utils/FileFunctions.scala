package BP2I.Utils

import BP2I.Utils.Param.{spark, logger}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name

object FileFunctions {


  /**
    * Goal: get the list of directories with data we are going to put into the datalake.
    * @param dir
    * @return
    */
  def getListOfDirectories(dir: String): Seq[String] = {

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val directories = fileSystem.listStatus(new Path(dir))

    val filteredDirectories = directories.filter(datFileExists)

    val listOfDirectories = filteredDirectories.filter(_.isDirectory).toSeq.map(_.getPath.toString)

    listOfDirectories
  }

  /**
    * Goal: check whether .dat file exist in the HDFS directory or not.
    * @param directory
    * @return
    */
  def datFileExists(directory: FileStatus): Boolean = {

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val datFilePath = new Path(directory.getPath.toString ++ "/*.dat")

    val globFilePath = fileSystem.globStatus(datFilePath).map(_.getPath)

    if (globFilePath.length >= 1) { true } else {

      logger.warn(s"==> WARNING: ${directory.getPath.toString.split("/").last} contains no or empty .dat file! <===")
      logger.warn(s"==> WARNING : cancelling process for path: $datFilePath <===")

      false }
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
      .replaceAll("-", "")
      .replaceAll(extension, "")

    fileName
  }

  /**
    * Goal: from a dataframe's file name, extract
    * [APPLICATION]_[TABLE]_[DATE]_[HOUR]_[VERSION]
    * @param dataFrame
    * @param extension
    * @return
    */
  def getFileInformation(dataFrame: DataFrame, extension: String): Seq[String] = {
    import spark.sqlContext.implicits._

    val fullFileName = dataFrame
      .select(input_file_name()).map(x => x.getString(0)).collect().toList.last
      .split("/").last
      .replaceAll(extension, "")
      .split("_")

    Seq(fullFileName.head, fullFileName(1), fullFileName(2), fullFileName(3), fullFileName.last)
  }

  /**
    * Goal: check if a directory exists, and delete it afterward. Works with HDFS directories.
    * @param path
    * @return
    */
  def deleteTmpDirectory(path: String): AnyVal = {

    val hadoopFileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (hadoopFileSystem.exists(new Path(path)))
      hadoopFileSystem.delete(new Path(path), true)
  }
}
