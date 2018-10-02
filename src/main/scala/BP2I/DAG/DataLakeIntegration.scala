package BP2I.DAG

import java.io.File

import BP2I.Utils.Arguments
import BP2I.Utils.Param.{logger, spark}
import BP2I.Utils.MiscFunctions.getListOfDirectories

object DataLakeIntegration {

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val argument = Arguments(args)

    if (argument.parentFolder.isDefined) {


      val listofdirectories = getListOfDirectories(argument.parentFolder.get)


      listofdirectories.foreach(InitializeHiveTables.main)


    } else if (argument.folder.isDefined) {

      InitializeHiveTables.main(argument.folder.get)

    } else logger.warn("NO ARGUMENT!")
  }
}
