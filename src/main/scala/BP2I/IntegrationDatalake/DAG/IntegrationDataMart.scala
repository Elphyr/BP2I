package BP2I.IntegrationDatalake.DAG

import BP2I.IntegrationDatalake.Utils.Param.spark

object IntegrationDataMart {

  def main(args: String): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val query = """Select
                  CONVERT(VARCHAR(1000), model_uuid  , 2) id,
                  ca_company.company_name manufacturer_name,
                  ca_model_def.description description,
                  ca_model_def.inactive inactive,
                  ca_model_def.name model_name,
                  ca_model_def.z_autre_code other_code,
                  ca_model_def.z_code code,
                  coalesce(A.enum,0) bp2i_marketed,
                  coalesce(B.enum,0) cryptable,
                   DATEADD(ss, ca_model_def.z_date_maj_score ,'1970/01/01') score_modified_date,
                  z_famille_hw.sym hw_family,
                  coalesce(C.enum,0) inventorible,
                  coalesce(D.enum,0) lot,
                  ca_model_def.z_model_asset asset_model,
                  coalesce(E.enum,0) secondary_storage_product,
                  ca_model_def.z_type_asset asset_type,
                  case when CHARINDEX('.' , ca_resource_class.name ) !=0 then
                    SUBSTRING (ca_resource_class.name,CHARINDEX('.' , ca_resource_class.name) +1,
                    len(ca_resource_class.name)) else null end category,
                   case when CHARINDEX('.' , ca_resource_class.name ) !=0 then
                    SUBSTRING (ca_resource_class.name,0,CHARINDEX('.' ,
                    ca_resource_class.name)) else null end class,
                  z_typologie.sym typology,
                  z_val_obso.sym scoring,
                  ca_model_def.last_update_user reftec_last_update_user,
                  DATEADD(ss, ca_model_def.last_update_date ,'1970/01/01') reftec_last_update_date,
                  ca_model_def.last_update_date  tal_last_update_date
                  from
                  ca_model_def
                  inner Join ca_company On ca_model_def.manufacturer_uuid = ca_company.company_uuid
                  left outer join bool_tab A on A.enum = ca_model_def.z_com_bp2i
                  left outer join bool_tab B on B.enum = ca_model_def.z_cryptable
                  left outer join bool_tab C on C.enum = ca_model_def.z_inventoriable
                  left outer join bool_tab D on D.enum = ca_model_def.z_lot
                  left outer join bool_tab E on E.enum = ca_model_def.z_prd_stock_sec
                  left outer join z_famille_hw on z_famille_hw.id = ca_model_def.z_famille_hw
                  left outer join ca_resource_class on ca_resource_class.id = ca_model_def.class_id
                  left outer join z_typologie on z_typologie.id = ca_model_def.z_typologie
                  left outer join z_val_obso on z_val_obso.id = ca_model_def.z_val_obso"""

    println(query)

    spark.sql(query).show(100, false)
  }
}