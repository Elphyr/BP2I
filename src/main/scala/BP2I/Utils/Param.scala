package BP2I.Utils

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Param {

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("BP2I DL Integration")
    .config("hive.metastore.warehouse.dir", warehouseLocation)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val logger: Logger = Logger.getLogger("BP2I")
}
