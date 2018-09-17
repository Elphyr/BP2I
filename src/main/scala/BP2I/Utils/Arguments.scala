package BP2I.Utils

import scopt.OptionParser

case class Arguments(folder: Option[String] = None)

object Arguments {

  def apply(arguments: Seq[String]): Arguments = {

    val parser: OptionParser[Arguments] = new scopt.OptionParser[Arguments]("scopt") {

      head("scopt", "3.x")

      opt[String]('f', "folder")
        .action((x, c) => c.copy(folder = Some(x)))
        .text("if you want to initialize the Hive query on a single folder")
    }

    parser.parse(arguments, Arguments()).get
  }
}