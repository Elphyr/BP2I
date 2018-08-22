package BP2I.Utils

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Param {

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("Spark BP2I example")
    .config("hive.metastore.warehouse.dir", warehouseLocation)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val logger: Logger = Logger.getLogger("BP2I")

  //val REFTEC_DIRECTORY = "/home/raphael/Documents/Lincoln/2018-BP2I/2018-08-Travail-Preliminaire/reftec_corrige/2018-08-18/REFTEC/REFTEC_CA_COMPANY_02082018_1/"
}
