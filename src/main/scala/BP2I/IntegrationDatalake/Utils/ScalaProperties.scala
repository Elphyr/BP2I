package BP2I.IntegrationDatalake.Utils

import java.util.Properties

object ScalaProperties {

  var sparkMaster: String = _
  var hiveTmpDir: String = _

  def setPropValues(environment: String): Unit = {

    val prop = new Properties()

    prop.load(this.getClass.getClassLoader.getResourceAsStream(environment + ".properties"))

    sparkMaster = prop.getProperty("param.spark.master")
    hiveTmpDir = prop.getProperty("param.hive.tmpDir")
  }
}