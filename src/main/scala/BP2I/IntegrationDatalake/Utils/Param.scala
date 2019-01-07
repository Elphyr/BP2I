package BP2I.IntegrationDatalake.Utils

import java.io.File

import BP2I.IntegrationDatalake.Utils.SparkProperties.sparkMaster
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Param {

  val warehouseLocation: String = new File("./datalake_bp2i/normalized_layer").getAbsolutePath

  val reportLocation: String = new File("./datalake_bp2i/report_layer").getAbsolutePath

  val spark: SparkSession = SparkSession.builder
    .master(sparkMaster)
    .appName("BP2I DL Integration")
    .config("hive.metastore.warehouse.dir", warehouseLocation)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val logger: Logger = Logger.getLogger("BP2I")
}
