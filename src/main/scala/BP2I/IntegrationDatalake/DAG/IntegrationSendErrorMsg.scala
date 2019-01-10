package BP2I.IntegrationDatalake.DAG

import BP2I.IntegrationDatalake.Utils.Params.spark

object IntegrationSendErrorMsg {

  def main(args: String): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val toto = spark.read.csv("/home/raphael/workspace/BP2I/reftec_20181203_105810")

    toto.describe()



  }
}
