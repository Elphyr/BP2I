package BP2I.AppLayer

object AppLayerQueryBank {

  val dimCatalogQuery =
    """Select
       decode(model_uuid, 'UTF-8') id,
       cacompany.company_name manufacturer_name,
       camodeldef.description description,
       camodeldef.inactive inactive,
       camodeldef.name model_name,
       camodeldef.z_autre_code other_code,
       camodeldef.z_code code,coalesce(A.enum,0) bp2i_marketed,
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
        from reftec.camodeldef
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
}
