package BP2I.DAG

import BP2I.Utils.Param.spark

object IntegrationSendErrorMsg {

  def main(args: String): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val toto = spark.read.csv("/home/raphael/workspace/BP2I/reftec_20181203_105810")

    toto.describe()



  }
}
