package BP2I.IntegrationDatalake.Utils

case class Arguments(environment: Option[String] = None,
                     folder: Option[String] = None,
                     parentFolder: Option[String] = None)

object Arguments {

  def apply(arguments: Seq[String]): Arguments = {

    val parser = new scopt.OptionParser[Arguments]("scopt") {

      head("scopt", "3.x")

      opt[String]('e', "environment")
        .action((x, c) => c.copy(environment = Some(x)))
        .text("the working environment: 'local', 'dev', 'qualif', 'prod'")
        .required()


      opt[String]('f', "folder")
        .action((x, c) => c.copy(folder = Some(x)))
        .text("if you want to integrate a single folder in the datalake")

      opt[String]('p', "parentFolder")
        .action((x, c) => c.copy(parentFolder = Some(x)))
        .text("if you want to integrate multiple folders in the datalake by giving the parent folder")
    }

    parser.parse(arguments, Arguments()).get
  }
}