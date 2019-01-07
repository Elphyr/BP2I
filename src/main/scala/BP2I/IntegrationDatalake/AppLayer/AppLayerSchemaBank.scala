package BP2I.IntegrationDatalake.AppLayer

import org.apache.spark.sql.types._

object AppLayerSchemaBank {

  /**
    * Goal: for each app, write expected schema here and then check if schema is met by the job.
    * If it isn't, return a warning.
    */

  val dimCatalogSchema = StructType(Array(
    StructField("id", StringType, true),
    StructField("manufacturer_name", StringType, true),
    StructField("description", StringType, true),
    StructField("inactive", IntegerType, true),
    StructField("model_name", StringType, true),
    StructField("other_code", StringType, true),
    StructField("code", StringType, true),
    StructField("bp2i_marketed", IntegerType, false),
    StructField("cryptable", IntegerType, false),
    StructField("score_modified_date", DateType, true),
    StructField("hw_family", StringType, true),
    StructField("inventorible", IntegerType, false),
    StructField("lot", IntegerType, false),
    StructField("asset_model", StringType, true),
    StructField("secondary_storage_product", IntegerType, false),
    StructField("asset_type", StringType, true),
    StructField("category", StringType, true),
    StructField("class", StringType, true),
    StructField("typology", StringType, true),
    StructField("scoring", StringType, true),
    StructField("reftec_last_update_user", StringType, true),
    StructField("reftec_last_update_date", DateType, true),
    StructField("tal_last_update_date", IntegerType, true)))


}
