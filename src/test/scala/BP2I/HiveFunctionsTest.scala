package BP2I

import BP2I.IntegrationDatalake.Func.HiveFunctions.{readDesFile, createExternalTable}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec

class HiveFunctionsTest extends FlatSpec with DataFrameSuiteBase {

  "The description reader " should "return the full table name, the primary column and the Hive query" in {

    val fullTableName = "MAIN_TEST_31082018_1"

    val primaryColumn = "company_uuid"

    val columnsAndTypes = List("Nature_Action CHAR", "company_uuid STRING", "parent_company_uuid STRING", "company_name VARCHAR",
      "inactive INT", "description VARCHAR", "company_type INT", "alias VARCHAR", "month_fiscal_year_ends INT", "web_address VARCHAR",
      "bbs VARCHAR", "creation_user VARCHAR", "creation_date INT", "last_update_user VARCHAR", "location_uuid STRING", "primary_contact_uuid STRING",
      "version_number INT", "last_update_date INT", "exclude_registration INT", "delete_time INT", "authentication_user_name VARCHAR",
      "authentication_password VARCHAR", "source_type_id INT", "auto_rep_version STRING", "domain_uuid STRING", "tenant STRING", "bsa_flag INT",
      "siia_flag INT", "fast_flag INT", "user_priority_flag INT", "employee_count INT", "desktop_count INT", "asset_count INT", "authoritative INT",
      "duplicate_with_uuid STRING", "division_id INT")

    val (testTableInformations, testPrimaryColumn, testColumnsAndTypes) = readDesFile("../BP2I_Spark/src/test/resources/input/MAIN_TEST_31082018_1/*.des")

    val testFullTableName = testTableInformations(1)

    assert(testFullTableName == fullTableName)

    assert(testPrimaryColumn == primaryColumn)

    assert(testColumnsAndTypes == columnsAndTypes)
  }

  "An external table " should "be successfully created by" in {

    val (testTableInformations, testPrimaryColumn, testColumnsAndTypes) = readDesFile("../BP2I_Spark/src/test/resources/input/MAIN_TEST_31082018_1/*.des")

    createExternalTable("testTable", "mainTestTable", testColumnsAndTypes, "/home/raphael/workspace/BP2I_Spark/src/test/resources/input/MAIN_TEST_31082018_1/*.dat")

    val createdTable = spark.sql("SELECT * FROM mainTestTable")

    assert(createdTable.count() == 491)

    spark.sql("DROP TABLE mainTestTable")
  }
}