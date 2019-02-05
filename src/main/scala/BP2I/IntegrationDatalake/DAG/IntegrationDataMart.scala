package BP2I.IntegrationDatalake.DAG

import BP2I.IntegrationDatalake.Utils.Params.spark

object IntegrationDataMart {

  def main(args: String): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val query = """Select
                  model_uuid id,
                  cacompany.company_name manufacturer_name,
                  description description,
                  inactive inactive,
                  name model_name,
                  z_autre_code other_code,
                  z_code code,
                  coalesce(A.enum,0) bp2i_marketed,
                  coalesce(B.enum,0) cryptable,
                  date_add('1970/01/01', z_date_maj_score/86400) score_modified_date,
                  zfamillehw.sym hw_family,
                  coalesce(C.enum,0) inventorible,
                  coalesce(D.enum,0) lot,
                  z_model_asset asset_model,
                  coalesce(E.enum,0) secondary_storage_product,
                  ztypologie.sym typology,
                  zvalobso.sym scoring,
                  last_update_user reftec_last_update_user,
                  last_update_date  tal_last_update_date
                  from reftec.camodeldef
                  inner Join reftec.cacompany On camodeldef.manufacturer_uuid = cacompany.company_uuid
                  left outer join reftec.booltab A on A.enum = camodeldef.z_com_bp2i
                  left outer join reftec.booltab B on B.enum = camodeldef.z_cryptable
                  left outer join reftec.booltab C on C.enum = camodeldef.z_inventoriable
                  left outer join reftec.booltab D on D.enum = camodeldef.z_lot
                  left outer join reftec.booltab E on E.enum = camodeldef.z_prd_stock_sec
                  left outer join reftec.zfamillehw on zfamillehw.id = camodeldef.z_famille_hw
                  left outer join reftec.caresourceclass on caresourceclass.id = camodeldef.class_id
                  left outer join reftec.ztypologie on ztypologie.id = camodeldef.z_typologie
                  left outer join reftec.zvalobso on zvalobso.id = camodeldef.z_val_obso"""

    spark.sql(query).show(100, false)
  }
}