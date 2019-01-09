package BP2I.IntegrationDatalake.Utils

import BP2I.IntegrationDatalake.Func.MiscFunctions.checkSchemaAppLayer
import BP2I.IntegrationDatalake.Utils.Param.spark
import org.apache.spark.sql.functions.{base64, coalesce, from_unixtime, instr}

object AppLayerFunctions {

  def createPseudoDataMart(query: String, appName: String) = {


    val toto = spark.sql(query)


    checkSchemaAppLayer(toto, appName)




  }



  import spark.sqlContext.implicits._

  val query = """Select
                  decode(model_uuid, 'UTF-8') id,
                  cacompany.company_name manufacturer_name,
                  camodeldef.description description,
                  camodeldef.inactive inactive,
                  camodeldef.name model_name,
                  camodeldef.z_autre_code other_code,
                  camodeldef.z_code code,
                  coalesce(A.enum,0) bp2i_marketed,
                  coalesce(B.enum,0) cryptable,
                  date_add('1970/01/01', camodeldef.z_date_maj_score/3600) score_modified_date,
                  zfamillehw.sym hw_family,
                  coalesce(C.enum,0) inventorible,
                  coalesce(D.enum,0) lot,
                  camodeldef.z_model_asset asset_model,
                  coalesce(E.enum,0) secondary_storage_product,
                  camodeldef.z_type_asset asset_type,
                  case when instr( caresourceclass.name, '.' ) != 0 then
                    SUBSTRING (caresourceclass.name, instr( caresourceclass.name, '.' ) + 1, length(caresourceclass.name)) else null end category,
                   case when instr( caresourceclass.name, '.' ) != 0 then
                    SUBSTRING (caresourceclass.name, 0, instr( caresourceclass.name, '.')) else null end class,
                  ztypologie.sym typology,
                  zvalobso.sym scoring,
                  camodeldef.last_update_user reftec_last_update_user,
                  date_add('1970/01/01', camodeldef.last_update_date/3600) reftec_last_update_date,
                  camodeldef.last_update_date tal_last_update_date
                  from
                  reftec.camodeldef
                  inner join reftec.cacompany on camodeldef.manufacturer_uuid = cacompany.company_uuid
                  left outer join reftec.booltab A on A.enum = camodeldef.z_com_bp2i
                  left outer join reftec.booltab B on B.enum = camodeldef.z_cryptable
                  left outer join reftec.booltab C on C.enum = camodeldef.z_inventoriable
                  left outer join reftec.booltab D on D.enum = camodeldef.z_lot
                  left outer join reftec.booltab E on E.enum = camodeldef.z_prd_stock_sec
                  left outer join reftec.zfamillehw on zfamillehw.id = camodeldef.z_famille_hw
                  left outer join reftec.caresourceclass on caresourceclass.id = camodeldef.class_id
                  left outer join reftec.ztypologie on ztypologie.id = camodeldef.z_typologie
                  left outer join reftec.zvalobso on zvalobso.id = camodeldef.z_val_obso"""

  val toto = spark.sql(query)
  spark.sql(query).show(100, false)


  val caModelDef = spark.sql("SELECT * FROM reftec.camodeldef")
  println(caModelDef.count())
  val caCompany = spark.sql("SELECT * FROM reftec.cacompany")
  println(caCompany.count())
  val boolTab = spark.sql("SELECT * FROM reftec.booltab")
  println(boolTab.count())
  val zFamilleHW = spark.sql("SELECT * FROM reftec.zfamillehw")
  println(zFamilleHW.count())
  val caResourceClass = spark.sql("SELECT * FROM reftec.caresourceclass")
  println(caResourceClass.count())
  val zTypologie = spark.sql("SELECT * FROM reftec.ztypologie")
  println(zTypologie.count())
  val zValObso = spark.sql("SELECT * FROM reftec.zvalobso")
  println(zValObso.count())

  val tata = caModelDef.select($"name", $"model_uuid", $"manufacturer_uuid".as("manu")) // 2867
  val tutu = caCompany.select($"company_name", $"company_uuid".as("manu")) //489

  tata.orderBy($"manu".asc).show()
  tutu.orderBy($"manu".asc).show()

  tata.join(tutu, Seq("manu"), "inner")

  val reftecDM = caModelDef
    .withColumn("id", base64($"model_uuid"))
    .withColumn("score_modified_date", from_unixtime($"z_date_maj_score"))
    .withColumn("reftec_last_update_date", from_unixtime($"last_update_date"))
    //.join(caCompany.select($"company_uuid", $"company_name".as("manufacturer_name")), regexp_replace(base64($"company_uuid"), " ", "") === regexp_replace(base64($"manufacturer_uuid"), " ", ""), "inner")
    .join(boolTab.select("enum"), $"enum" === $"z_com_bp2i", "left_outer").withColumnRenamed("enum", "enum_A")
    .join(boolTab.select("enum"), $"enum" === $"z_cryptable", "left_outer").withColumnRenamed("enum", "enum_B")
    .join(boolTab.select("enum"), $"enum" === $"z_inventoriable", "left_outer").withColumnRenamed("enum", "enum_C")
    .join(boolTab.select("enum"), $"enum" === $"z_lot", "left_outer").withColumnRenamed("enum", "enum_D")
    .join(boolTab.select("enum"), $"enum" === $"z_prd_stock_sec", "left_outer").withColumnRenamed("enum", "enum_E")
    .join(zFamilleHW.select($"id", $"sym"), $"zFamilleHW.id" === $"z_famille_hw", "left_outer").drop($"zFamilleHW.id")
    .join(caResourceClass.select($"id"), $"caResourceClass.id" === $"z_typologie", "left_outer").drop($"caResourceClass.id")
    .join(zTypologie.select($"id", $"sym"), $"zTypologie.id" === $"z_typologie", "left_outer").drop($"zTypologie.id")
    .join(zValObso.select($"id", $"sym"), $"zValObso.id" === $"z_val_obso", "left_outer").drop($"zValObso.id")
    .withColumn("cond", instr($"caResourceClass.name", "."))
    //.withColumn("toto", when($"cond" =!= lit(0), substring($"caResourceClass.name", instr($"caResourceClass.name", ".") + 1, length($"caResourceClass.name"))).otherwise(null))
    .select($"id",
    $"description",
    $"inactive",
    $"name".as("model_name"),
    $"z_autre_code".as("other_code"),
    $"z_code".as("code"),
    coalesce($"enum_A").as("bp2i_marketed"),
    coalesce($"enum_B").as("cryptable"),
    $"score_modified_date",
    $"zFamilleHW.sym".as("family"),
    coalesce($"enum_C").as("inventorible"),
    coalesce($"enum_D").as("lot"),
    $"z_model_asset".as("asset_model"),
    coalesce($"enum_E").as("secondary_storage_product"),
    $"z_type_asset".as("asset_type"),
    $"zTypologie.sym".as("typologie"),
    $"zvalobso.sym".as("scoring"),
    $"last_update_user".as("reftec_last_update_user"),
    $"reftec_last_update_date",
    $"last_update_date".as("tal_last_update_date"))

  reftecDM.count()

}
