package BP2I

import BP2I.IntegrationDatalake.Func.HiveFunctions.{createExternalTable, readDesFile}
import BP2I.IntegrationDatalake.Utils.Arguments
import BP2I.IntegrationDatalake.Utils.ScalaProperties.setPropValues
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec

class HiveFunctionsTest extends FlatSpec with DataFrameSuiteBase {

  val arguments = Arguments(environment = Some("local"))

  setPropValues(arguments.environment.get)

  "The description reader " should "return the full table name, the primary column and the Hive query" in {

    val tableName = "TEST"

    val primaryColumn = "company_uuid"

    val columnsAndTypes = List("Nature_Action CHAR", "company_uuid STRING", "parent_company_uuid STRING", "company_name STRING",
      "inactive INT", "description STRING", "company_type INT", "alias STRING", "month_fiscal_year_ends INT", "web_address STRING",
      "bbs STRING", "creation_user STRING", "creation_date INT", "last_update_user STRING", "location_uuid STRING", "primary_contact_uuid STRING",
      "version_number INT", "last_update_date INT", "exclude_registration INT", "delete_time INT", "authentication_user_name STRING",
      "authentication_password STRING", "source_type_id INT", "auto_rep_version STRING", "domain_uuid STRING", "tenant STRING", "bsa_flag INT",
      "siia_flag INT", "fast_flag INT", "user_priority_flag INT", "employee_count INT", "desktop_count INT", "asset_count INT", "authoritative INT",
      "duplicate_with_uuid STRING", "division_id INT")

    val (testTableInformation, testPrimaryColumn, testColumnsAndTypes) = readDesFile("../BP2I/src/test/resources/input/MAIN_TEST_31082018_1/*.des")

    val testTableName = testTableInformation(1)

    assert(testTableName == tableName)

    assert(testPrimaryColumn == primaryColumn)

    assert(testColumnsAndTypes == columnsAndTypes.filterNot(_.contains("Nature_Action"))
    )
  }

  "An external table " should "be successfully created by" in {

    val (testTableInformation, testPrimaryColumn, testColumnsAndTypes) = readDesFile("../BP2I/src/test/resources/input/MAIN_TEST_31082018_1/*.des")

    spark.sql("CREATE DATABASE IF NOT EXISTS testtable")

    createExternalTable("testTable", "mainTestTable", testColumnsAndTypes, "/home/raphael/workspace/BP2I/src/test/resources/input/MAIN_TEST_31082018_1/*.dat", delTmpDir = false)

    val createdTable = spark.sql("SELECT * FROM testtable.mainTestTable")

    assert(createdTable.count() == 491)

    spark.sql("DROP TABLE testtable.mainTestTable")
    spark.sql("DROP DATABASE testtable cascade")
  }
}