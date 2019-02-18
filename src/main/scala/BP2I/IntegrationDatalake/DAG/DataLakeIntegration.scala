package BP2I.IntegrationDatalake.DAG

import BP2I.AppLayer.QueryBank.dataMartUpdate
import BP2I.IntegrationDatalake.Func.FileFunctions.getListOfDirectories
import BP2I.IntegrationDatalake.Utils.Arguments
import BP2I.IntegrationDatalake.Utils.Params.{logger, spark}
import BP2I.IntegrationDatalake.Utils.ScalaProperties.setPropValues

object DataLakeIntegration {

  def main(args: Array[String]): Unit = {

    val timeBegin = System.nanoTime

    val arguments = Arguments(args)

    setPropValues(arguments.environment.get)

    spark.sparkContext.setLogLevel("WARN")

    if (arguments.parentFolder.isDefined) {

      val listOfDirectories = getListOfDirectories(arguments.parentFolder.get)

      listOfDirectories.foreach(IntegrationRawData.main)

      dataMartUpdate(arguments.applicationName.get)

      val jobDuration = (System.nanoTime - timeBegin) / 1e9d
      logger.warn(s"===> JOB SUCCESSFUL, CLOSING AFTER $jobDuration SECONDS <===")

    } else if (arguments.folder.isDefined) {

      IntegrationRawData.main(arguments.folder.get)

      val jobDuration = (System.nanoTime - timeBegin) / 1e9d
      logger.warn(s"===> JOB SUCCESSFUL, CLOSING AFTER $jobDuration SECONDS <===")

    } else logger.warn("Please put an argument")
  }
}