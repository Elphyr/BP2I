package BP2I.DAG

import BP2I.Utils.Arguments
import BP2I.Utils.FileFunctions.getListOfDirectories
import BP2I.Utils.Param.{logger, spark}

object DataLakeIntegration {

  def main(args: Array[String]): Unit = {

    val timeBegin = System.nanoTime

    spark.sparkContext.setLogLevel("WARN")

    val argument = Arguments(args)

    if (argument.parentFolder.isDefined) {

      val listOfDirectories = getListOfDirectories(argument.parentFolder.get)

      listOfDirectories.foreach(IntegrationRawData.main)

      val jobDuration = (System.nanoTime - timeBegin) / 1e9d

      logger.warn(s"===> JOB SUCCESSFUL, CLOSING AFTER $jobDuration SECONDS <===")

    } else if (argument.folder.isDefined) {

      IntegrationRawData.main(argument.folder.get)

    } else logger.warn("Please put an argument")
  }
}
