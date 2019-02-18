package BP2I.AppLayer

import BP2I.IntegrationDatalake.Utils.Params.spark

object QueryBank {

  /**
    * Goal: from a Scala object, get the (key, values) as follows:
    * key => the name of the dimension.
    * values => the Spark-SQL statement.
    * @param o
    * @return
    */
  def getFields(o: Any = QueryBank): Map[String, Any] = {

    val fieldsAsPairs = for (field <- o.getClass.getDeclaredFields) yield {
      field.setAccessible(true)
      (field.getName, field.get(o))
    }

    Map(fieldsAsPairs :_*)
  }

  /**
    * Update the datamart with the application name.
    * @param applicationName
    */
  def dataMartUpdate(applicationName: String): Unit = {

    for (p <- getFields().filter(_._1.contains(applicationName))) {

      spark.sql(p._2.toString).createOrReplaceTempView(p._1 + "_tmp")

      spark.sql(s"DROP TABLE IF EXISTS ${applicationName.toLowerCase()}.${p._1}")
      spark.sql(s"CREATE TABLE $applicationName.${p._1} AS SELECT * FROM ${p._1}_tmp")
    }
  }

  val reftec_dim_catalog =
    """SELECT
       model_uuid id,
       cacompany.company_name manufacturer_name,
       camodeldef.description description,
       camodeldef.inactive inactive,
       camodeldef.name model_name,
       camodeldef.z_autre_code other_code,
       camodeldef.z_code code,
       COALESCE(A.enum, 0) bp2i_marketed,
       COALESCE(B.enum, 0) cryptable,
       FROM_UNIXTIME(z_date_maj_score) score_modified_date,
       zfamillehw.sym hw_family,
       COALESCE(C.enum, 0) inventorible,
       COALESCE(D.enum, 0) lot,
       z_model_asset asset_model,
       COALESCE(E.enum, 0) secondary_storage_product,
       camodeldef.z_type_asset asset_type,
       CASE WHEN LOCATE('.', caresourceclass.name) != 0 THEN substring_index(caresourceclass.name, '.', -1) ELSE NULL END category,
       CASE WHEN LOCATE('.', caresourceclass.name) != 0 THEN substring_index(caresourceclass.name, '.', 1) ELSE NULL END class,
       ztypologie.sym typology,
       zvalobso.sym scoring,
       camodeldef.last_update_user reftec_last_update_user,
       FROM_UNIXTIME(camodeldef.last_update_date) reftec_last_update_date,
       camodeldef.last_update_date  tal_last_update_date
       FROM reftec.camodeldef
       INNER JOIN reftec.cacompany On camodeldef.manufacturer_uuid = cacompany.company_uuid
       LEFT OUTER JOIN reftec.booltab A on A.enum = camodeldef.z_com_bp2i
       LEFT OUTER JOIN reftec.booltab B on B.enum = camodeldef.z_cryptable
       LEFT OUTER JOIN reftec.booltab C on C.enum = camodeldef.z_inventoriable
       LEFT OUTER JOIN reftec.booltab D on D.enum = camodeldef.z_lot
       LEFT OUTER JOIN reftec.booltab E on E.enum = camodeldef.z_prd_stock_sec
       LEFT OUTER JOIN reftec.zfamillehw on zfamillehw.id = camodeldef.z_famille_hw
       LEFT OUTER JOIN reftec.caresourceclass on caresourceclass.id = camodeldef.class_id
       LEFT OUTER JOIN reftec.ztypologie on ztypologie.id = camodeldef.z_typologie
       LEFT OUTER JOIN reftec.zvalobso on zvalobso.id = camodeldef.z_val_obso"""

/*
  val reftec_dim_environnement = "SELECT id, sym FROM reftec.zenv"

  val reftec_dim_fabricant = "SELECT company_uuid id, company_name FROM reftec.cacompany WHERE company_type = 1000011"

  val reftec_dim_famille = "SELECT id, name FROM reftec.caresourcefamily"

  val reftec_dim_location =
    """SELECT
      calocation.location_uuid id,
      calocation.location_name,
      calocation.address_1,
      calocation.city,
      calocation.z_id_tririga,
      calocation.z_usage,
      calocation.site_id,
      casite.name site,
      calocation.country country_id,
      cacountry.name country
      FROM reftec.calocation
      LEFT OUTER JOIN reftec.casite ON calocation.site_id = casite.id
      LEFT OUTER JOIN reftec.cacountry ON calocation.country = cacountry.id"""

  val reftec_dim_metier_nv_1 = "SELECT id, sym FROM reftec.zproputil1"

  val reftec_dim_metier_nv_2 = "SELECT id, sym FROM reftec.zproputil2"

  val reftec_dim_pays = "SELECT id, nom from reftec.zsalle"

  val reftec_dim_upm = "SELECT id, sym from reftec.zupm"

  val reftec_dim_usage = "SELECT id, sym from reftec.zusage"

  val reftec_dim_statut = "SELECT id, name from reftec.caresourcestatus"

  val reftec_dim_eqp_physique =
    """SELECT
      zextphy.own_resource_uuid ID,
      zextphy.z_architecture architecture,
      zextphy.z_autre_code Other_code,
      zextphy.z_carte_mngt1 Management_Card_1,
      zextphy.z_carte_mngt2 Management_Card_2,
      zextphy.z_criticite Equipment_Criticality,
      FROM_UNIXTIME(zextphy.z_date_prem_tension) First_Power_Up_Date,
      FROM_UNIXTIME(zextphy.z_date_der_detect) Last_Detected_Date,
      FROM_UNIXTIME(zextphy.z_date_livraison) Delivery_Date,
      FROM_UNIXTIME(zextphy.z_date_mes) Service_Delivery_Date,
      FROM_UNIXTIME(zextphy.z_date_integ_maint) Integration_and_Maintenance_Date,
      FROM_UNIXTIME(zextphy.z_date_etape1) Send_for_Finance_Approval,
      FROM_UNIXTIME(zextphy.z_date_etape2) Finance_Approval,
      FROM_UNIXTIME(zextphy.z_date_etape3) Sent_to_CTI,
      FROM_UNIXTIME(zextphy.z_date_etape4) Returned_from_CTI,
      FROM_UNIXTIME(zextphy.z_date_etape5) Identify_Asset,
      FROM_UNIXTIME(zextphy.z_date_etape6) Remove_Asset,
      FROM_UNIXTIME(zextphy.z_date_etape7) Receipt_of_BSD_Reference,
      FROM_UNIXTIME(zextphy.z_date_etape8) Send_to_Finance_for_Update,
      FROM_UNIXTIME(zextphy.z_date_etape9) Out_of_Accounting_Book,
      FROM_UNIXTIME(zextphy.z_date_fin_amorti) Write_Off_Date,
      zextphy.z_dns_principal dns_principal,
      zextphy.z_nom_projet Project_Name,
      zextphy.z_num_tracking Tracking_Number,
      zextphy.z_num_inventaire Inventory_Number,
      zextphy.z_num_smtb SMTB_Number,
      zextphy.z_reference_bsd BSD_Reference,
      zextphy.z_ref_commande Purchase_Order,
      zextphy.z_reference_palette Palet_Reference,
      zextphy.z_ref_maintenance Maintenance_Reference,
      coalesce(booltab.enum,0) RMA_to_be_controlled,
      coalesce(A.enum,0) To_be_saved,
      coalesce(B.enum,0) To_be_supervised,
      coalesce(C.enum,0) Acs_authentication,
      coalesce(D.enum,0) Simple_Hosting,
      coalesce(E.enum,0) inventoriable,
      zferraillage.id id_ferraillage,
      zferraillage.sym lot,
      zmetier.id  id_line_bus_asso_user,
      zmetier.sym Line_Business_Associated_User,
      zmeo.id id_meo,
      zmeo.sym meo,
      coalesce(F.enum,0) Deploy_in_Production,
      znatdecom.id id_nature_decom,
      znatdecom.sym Nature,
      zperimoper.id id_operated_perimetre,
      zperimoper.sym Operated_Perimetre,
      zproputil1.id id_owner_level_1,
      zproputil1.sym Owner_Level_1,
      zproputil2.id id_owner_level_2,
      zproputil2.sym Owner_Level_2,
      zstatutvalidite.id id_validity_status,
      zstatutvalidite.sym Validity_Status,
      ztypeutilisation.id id_usage_type,
      ztypeutilisation.sym Usage_Type,
      ztypemaintenance.id id_maintenance_type,
      ztypemaintenance.sym Maintenance_Type,
      zextphy.z_num_ligne_commande purchase_order_line,
      zextphy.z_duree_amortissement service_life,
      ztypeachat.id id_acquisition_mode,
      ztypeachat.sym acquisition_mode,
      zentitejuridique.id id_legal_entity,
      zentitejuridique.sym legal_entity,
      zdestination.id id_destination,
      zdestination.sym destination,
      FROM_UNIXTIME(zextphy.z_date_sortie_ierp) z_date_sortie_ierp,
      zextphy.z_comment_sortie_ierp,
      zextphy.z_code_projet code_projet,
      zextphy.z_frequence_proc processor_frequency,
      zextphy.z_memoire memory,
      zextphy.z_modele_proc Processor_model,
      zextphy.z_nb_core_cpu number_of_core_by_cpu,
      zextphy.z_nb_cpu number_of_cpu,
      zextphy.z_tpmc_mips tpmc_mips
      FROM reftec.zextphy
      LEFT OUTER JOIN reftec.booltab ON booltab.enum = zextphy.z_a_controler_rma
      LEFT OUTER JOIN reftec.booltab A ON A.enum = zextphy.z_a_sauvegarder
      LEFT OUTER JOIN reftec.booltab B ON B.enum = zextphy.z_a_superviser
      LEFT OUTER JOIN reftec.booltab C ON C.enum = zextphy.z_auth_acs
      LEFT OUTER JOIN reftec.booltab D ON D.enum = zextphy.z_heberge_sec
      LEFT OUTER JOIN reftec.booltab E ON E.enum = zextphy.z_inventoriable
      LEFT OUTER JOIN reftec.zferraillage ON zferraillage.id = zextphy.z_lot_ferraillage
      LEFT OUTER JOIN reftec.zmetier ON zmetier.id = zextphy.z_metier_util_assoc
      LEFT OUTER JOIN reftec.zmeo ON zmeo.id = zextphy.z_meo
      LEFT OUTER JOIN reftec.booltab F ON F.enum = zextphy.z_mise_en_production
      LEFT OUTER JOIN reftec.znatdecom ON znatdecom.id = zextphy.z_nature_decom
      LEFT OUTER JOIN reftec.zperimoper ON zperimoper.id = zextphy.z_perim_oper
      LEFT OUTER JOIN reftec.zproputil1 ON zproputil1.id = zextphy.z_proprietaire_1
      LEFT OUTER JOIN reftec.zproputil2 ON zproputil2.id = zextphy.z_proprietaire_2
      LEFT OUTER JOIN reftec.zstatutvalidite ON zstatutvalidite.id = zextphy.z_statut_validite
     LEFT OUTER JOIN reftec.ztypeutilisation ON ztypeutilisation.id = zextphy.z_type_utilisation
      LEFT OUTER JOIN reftec.ztypemaintenance ON ztypemaintenance.id = zextphy.z_type_maintenance
      LEFT OUTER JOIN reftec.ztypeachat ON zextphy.z_type_achat =  ztypeachat.id
      LEFT OUTER JOIN reftec.zentitejuridique ON zextphy.z_entite_juridique = zentitejuridique.id
      LEFT OUTER JOIN reftec.zdestination ON zextphy.z_destination = zdestination.id"""

  val backup =
    """      #cacontact.contact_uuid id_contact,
      |      #cacontact.last_name Proximity_Team,
      |            #LEFT OUTER JOIN reftec.cacontact ON cacontact.contact_uuid = zextphy.z_eqp_prox
      |"""
*/


}
