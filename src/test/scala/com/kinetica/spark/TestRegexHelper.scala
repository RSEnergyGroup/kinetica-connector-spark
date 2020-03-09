package com.kinetica.spark

import com.kinetica.spark.util.RegexHelper
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.Matchers._

class TestRegexHelper extends FunSuite {

  def testCorrectTableNameAndSchemaName(input: String, schema: Option[String], table: String, tableContainsSchemaName: Boolean = true): Unit = {

    val testName: String = s"RegexHelper extracts '" + (schema match {
      case Some(s) => s"${schema.get}, $table"
      case _ => table
    }) + s"' from '$input''"
    test(testName) {
      val result = RegexHelper.extractTableAndSchemaName(input, tableContainsSchemaName)

      assert(result.tableName == table)
      assert(result.schemaName == schema)
    }
  }

  testsFor(testCorrectTableNameAndSchemaName("[azure.template].[foo.bar]",Some("azure.template"),"foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("[azure.template].[foo.bar]",None,"[azure.template].[foo.bar]",false))
  testsFor(testCorrectTableNameAndSchemaName("azure.template.foo.bar",Some("azure"),"template.foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("[azure.template].foo.bar",Some("azure.template"),"foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("[azure].[foo.bar]",None,"[azure].[foo.bar]", false))
  testsFor(testCorrectTableNameAndSchemaName("[azure].[foo.bar]",Some("azure"),"foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("[azure].foo.bar",Some("azure"),"foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("azure.foo.bar",Some("azure"),"foo.bar"))
  testsFor(testCorrectTableNameAndSchemaName("RSEG_SPACING_BACKEND.backend_header_directional_survey_view",Some("RSEG_SPACING_BACKEND"),"backend_header_directional_survey_view"))

  test(s"RegexHelper throws exception on empty inputs") {
    intercept[IllegalArgumentException]{
      RegexHelper.extractTableAndSchemaName("", true)
    }
    intercept[IllegalArgumentException]{
      RegexHelper.extractTableAndSchemaName(null, false)
    }
  }
}
