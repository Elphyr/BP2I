package BP2I.IntegrationDatalake.Utils

import java.io.FileInputStream
import java.util.Properties

object SparkProperties {

  var sparkMaster: String = _
  var hiveTmpDir: String = _

  def setPropValues(environment: String): Unit = {

    val prop = new Properties()

    prop.load(new FileInputStream("src/main/resources/" + environment + ".properties"))

    sparkMaster = prop.getProperty("param.spark.master")
    hiveTmpDir = prop.getProperty("param.hive.tmpDir")
  }
}