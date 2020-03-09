package com.kinetica.spark.json

import java.io.File
import java.nio.file.Paths

import com.gpudb.{GPUdbException, Type}
import com.kinetica.spark.loader.LoaderConfiguration
import com.kinetica.spark.{SparkConnectorTestBase, SparkConnectorTestFixture, SparkKineticaDriver}
import com.kinetica.spark.util.ConfigurationConstants
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * This trait contains test cases for the spark connector's [[KineticaJsonSchemaHelper]].
 *
 * In order to run these tests, a class is needed that mixes in this trait
 * and uses the `testsFor()` method which invokes the given behavior function.
 *
 */
trait TestJsonSchemaHelper
  extends SparkConnectorTestFixture {
  this: FunSuite =>

  val resourceFolder = getClass.getResource("/")
  val resourceFolderRoot = new Path(resourceFolder.toString, "TestJsonSchemaHelper")

  val columns = List(
    new Type.Column("test_int", classOf[java.lang.Integer]),
    new Type.Column("test_int16", classOf[java.lang.Integer], List("int16").asJava),
    new Type.Column("test_int8", classOf[java.lang.Integer], List("int8").asJava),
    new Type.Column("test_long", classOf[java.lang.Long]),
    new Type.Column("test_long_timestamp", classOf[java.lang.Long], List("timestamp").asJava),
    new Type.Column("test_float", classOf[java.lang.Float]),
    new Type.Column("test_double", classOf[java.lang.Double]),
    new Type.Column("test_string", classOf[java.lang.String]),
    new Type.Column("test_string_char256", classOf[java.lang.String], List("char256")),
    new Type.Column("test_string_char128", classOf[java.lang.String], List("char128")),
    new Type.Column("test_string_char64", classOf[java.lang.String], List("char64")),
    new Type.Column("test_string_char32", classOf[java.lang.String], List("char32")),
    new Type.Column("test_string_char16", classOf[java.lang.String], List("char16")),
    new Type.Column("test_string_char8", classOf[java.lang.String], List("char8")),
    new Type.Column("test_string_char4", classOf[java.lang.String], List("char4")),
    new Type.Column("test_string_char2", classOf[java.lang.String], List("char2")),
    new Type.Column("test_string_char1", classOf[java.lang.String], List("char1")),
    new Type.Column("test_string_ipv4", classOf[java.lang.String], List("ipv4")),
    new Type.Column("test_string_decimal", classOf[java.lang.String], List("decimal")),
    new Type.Column("test_string_date", classOf[java.lang.String], List("date")),
    new Type.Column("test_string_time", classOf[java.lang.String], List("time")),
    new Type.Column("test_string_datetime", classOf[java.lang.String], List("datetime"))
  )

  val expectedJson =
    s"""
       |{
       |  "typeProperties" : {
       |    "test_string_char32" : [ "data", "char32" ],
       |    "test_string_datetime" : [ "data", "datetime" ],
       |    "test_string_char4" : [ "data", "char4" ],
       |    "test_int16" : [ "data", "int16" ],
       |    "test_string_date" : [ "data", "date" ],
       |    "test_int" : [ "data" ],
       |    "test_float" : [ "data" ],
       |    "test_string_char1" : [ "data", "char1" ],
       |    "test_string_time" : [ "data", "time" ],
       |    "test_string_char256" : [ "data", "char256" ],
       |    "test_string_ipv4" : [ "data", "ipv4" ],
       |    "test_string_char64" : [ "data", "char64" ],
       |    "test_long_timestamp" : [ "data", "timestamp" ],
       |    "test_long" : [ "data" ],
       |    "test_string_char2" : [ "data", "char2" ],
       |    "test_string" : [ "data" ],
       |    "test_string_char16" : [ "data", "char16" ],
       |    "test_string_char128" : [ "data", "char128" ],
       |    "test_double" : [ "data" ],
       |    "test_string_char8" : [ "data", "char8" ],
       |    "test_string_decimal" : [ "data", "decimal" ],
       |    "test_int8" : [ "data", "int8" ]
       |  },
       |  "typeSchema" : {
       |    "type" : "record",
       |    "name" : "type_name",
       |    "fields" : [ {
       |      "name" : "test_int",
       |      "type" : "int"
       |    }, {
       |      "name" : "test_int16",
       |      "type" : "int"
       |    }, {
       |      "name" : "test_int8",
       |      "type" : "int"
       |    }, {
       |      "name" : "test_long",
       |      "type" : "long"
       |    }, {
       |      "name" : "test_long_timestamp",
       |      "type" : "long"
       |    }, {
       |      "name" : "test_float",
       |      "type" : "float"
       |    }, {
       |      "name" : "test_double",
       |      "type" : "double"
       |    }, {
       |      "name" : "test_string",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char256",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char128",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char64",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char32",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char16",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char8",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char4",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char2",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_char1",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_ipv4",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_decimal",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_date",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_time",
       |      "type" : "string"
       |    }, {
       |      "name" : "test_string_datetime",
       |      "type" : "string"
       |    } ]
       |  },
       |  "isReplicated" : true
       |}
       |""".stripMargin.trim


  /**
   * Tests for various json Parsing features.
   */
  def jsonParsingFeatures( package_to_test: String, package_description: String ): Unit = {

    val testResourceRoot = new Path(resourceFolderRoot, package_to_test.stripPrefix("[").stripSuffix("]"))

    /**
     * Test for properly requiring a kinetica URL when using the helper
     */
    test(s"""$package_description Case 1: Require a Kinetica URL when using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test,package_description,"json_schema_test_case_01")
      logger.debug( s"Table name '${tableName}'" );

      val loaderConfigOptions = get_default_spark_connector_options()
      loaderConfigOptions(ConfigurationConstants.KINETICA_URL_PARAM) = ""
      loaderConfigOptions(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = tableName

      intercept[GPUdbException]{
        KineticaJsonSchemaHelper(loaderConfigOptions.toMap, this.m_sparkSession)
      }
    }

    /**
     * Test for correctly acquiring the metadata from a constructed table
     */
    test(s"""$package_description Case 2: The correct type schema is retrieved using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test,package_description,"json_schema_test_case_02")
      logger.debug( s"Table name '${tableName}'" );

      val options = get_default_spark_connector_options()
      options(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = tableName

      val helper = KineticaJsonSchemaHelper(options.toMap, this.m_sparkSession)

      // construct a table to grab the schema from
      createKineticaTableWithGivenColumns(
        helper.loaderConfig.tablename, Option(helper.loaderConfig.schemaname), columns.to[ListBuffer], 500, true)

      val schema = helper.getKineticaTableSchema

      val json = KineticaJsonSchema.encodeSchemaMetadataToJson(schema)

      assert(json == expectedJson)
    }

    /**
     * Test for correctly constructing a table from json
     */
    test(s"""$package_description Case 3: The correct table is created using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test,package_description,"json_schema_test_case_02")
      logger.debug( s"Table name '${tableName}'" );

      val options = get_default_spark_connector_options()
      options(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = tableName

      val helper = KineticaJsonSchemaHelper(options.toMap, this.m_sparkSession)

      val schemaMetadata = KineticaJsonSchema.decodeKineticaSchemaMetadataDefinition(expectedJson)

      // construct a table to grab the schema from
      helper.createTableFromSchema(helper.loaderConfig.tablename,helper.loaderConfig.schemaname, schemaMetadata.get)

      // download the schema and compare
      val schema = helper.getKineticaTableSchema
      val json = KineticaJsonSchema.encodeSchemaMetadataToJson(schema)

      assert(json == expectedJson)
    }


    /**
     * Test for correctly acquiring the metadata from a constructed table and saving it to disk, then reloading and verifying
     */
    test(s"""$package_description Case 4: The correct type schema is retrieved, saved, and reloaded using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test,package_description,"json_schema_test_case_04")
      logger.debug( s"Table name '${tableName}'" );

      val options = get_default_spark_connector_options()
      options(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = tableName

      val helper = KineticaJsonSchemaHelper(options.toMap, this.m_sparkSession)

      // construct a table to grab the schema from
      createKineticaTableWithGivenColumns(
        helper.loaderConfig.tablename, Option(helper.loaderConfig.schemaname), columns.to[ListBuffer], 500, true)

      val schema = helper.getKineticaTableSchema

      val schemaPath = new Path(testResourceRoot, helper.loaderConfig.tablename)

      helper.saveKineticaSchema(schemaPath, schema)

      // reload the schema from the filesystem
      val readSchema = helper.loadKineticaSchema(schemaPath)

      // re-verify
      val json = KineticaJsonSchema.encodeSchemaMetadataToJson(readSchema.get)
      assert(json == expectedJson)
    }


    /**
     * Test for correctly reading the json from disk at the time we upload a table
     */
    test(s"""$package_description Case 5: The json is correctly from disk at the time we upload a table using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test,package_description,"json_schema_test_case_05")
      logger.debug( s"Table name '${tableName}'" );

      val options = get_default_spark_connector_options()
      options(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = tableName

      val helper = KineticaJsonSchemaHelper(options.toMap, this.m_sparkSession)

      // construct a table to grab the schema from
      createKineticaTableWithGivenColumns(
        helper.loaderConfig.tablename, Option(helper.loaderConfig.schemaname), columns.to[ListBuffer], 5000, true)

      val tablePath = new Path(testResourceRoot, helper.loaderConfig.tablename)
      val schema = helper.getKineticaTableSchema

      // save the dataframe to disk
      val fetchedRecords = m_sparkSession.sqlContext.read.format( package_to_test ).options(options).load()
      fetchedRecords.write.format("parquet").mode("overwrite").save(tablePath.toString)

      // now the save the schema along the data
      helper.saveKineticaSchema(tablePath, schema)

      // create new options for the write table
      val writeTable = generateTableName(package_to_test,package_description,"json_schema_test_case_05", "_COPY")
      val writerOptions = get_default_spark_connector_options()
      writerOptions(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = writeTable

      // use the loader way, since that will auto-load the json file
      writerOptions(ConfigurationConstants.KINETICA_USETEMPLATES_PARAM) = "true"
      writerOptions(ConfigurationConstants.KINETICA_TEMPLATETYPE_PARAM) = ConfigurationConstants.TEMPLATE_TYPE_JSON
      writerOptions(ConfigurationConstants.LOADERCODEPATH) = "true"
      writerOptions(ConfigurationConstants.CONNECTOR_DATAPATH_PARAM) = tablePath.toString // read from where we saved it
      writerOptions(ConfigurationConstants.CONNECTOR_DATAFORMAT_PARAM) = "parquet"

      // the loader path doesn't work with jdbc url specified
      writerOptions.remove(ConfigurationConstants.KINETICA_JDBCURL_PARAM)

      // write the dataframe from disk up to kinetica using the loader path
      val driver = new SparkKineticaDriver(writerOptions)

      // assign the property file which normally would have been used with the Driver method
      val propertiesFile = getClass.getResource("/spark-sample.properties")
      driver.propFile = Paths.get(propertiesFile.toURI).toFile

      val ss = m_sparkSession.newSession()
      driver.start(ss)

      // create a new instance of the helper for reading the new table
      val newHelper =  KineticaJsonSchemaHelper(writerOptions.toMap, this.m_sparkSession)
      mark_table_for_deletion_at_test_end(newHelper.loaderConfig.tablename)

      // re-verify after table was published with the driver app
      val newSchema = newHelper.getKineticaTableSchema
      val json = KineticaJsonSchema.encodeSchemaMetadataToJson(newSchema)
      assert(json == expectedJson)
    }

    // RSEG_SPACING_BACKEND.backend_header_directional_survey_view
    /**
     * Test for correctly getting the table name
     */
    test(s"""$package_description Case 6: The table name is read correctly using the
            | KineticaJsonSchemaHelper class""".stripMargin.replaceAll("\n", "") ) {

      val tableName = generateTableName(package_to_test, package_description, "json_schema_test_case_06")
      logger.debug(s"Table name '${tableName}'");

      val options = get_default_spark_connector_options()
      options(ConfigurationConstants.KINETICA_TABLENAME_PARAM) = "RSEG_SPACING_BACKEND.backend_header_directional_survey_view"

      val propertiesFile = getClass.getResource("/spark-sample.properties")
      val propFileLocation = Paths.get(propertiesFile.toURI).toFile.toString

      val loader = new LoaderConfiguration(this.m_sparkSession.sparkContext,  options.toMap)

      def createDriverArgs(argMap: scala.collection.Map[String, String]): Array[String] = {
        var argList = new ListBuffer[String]()
        argList += propFileLocation
        argMap.toSeq.sortBy(_._1).map(m => {
          argList += s"--${m._1}"
          argList += m._2
        })
        argList.toList.toArray
      }

      val driver = new SparkKineticaDriver(createDriverArgs(options))

      driver.init(this.m_sparkSession)


      assert(true)




    }
  }
}

/**
 *  Test KineticaJsonSchemaHelper using the DataSource v1 package.
 */
class TestJsonSchemaHelper_V1
  extends SparkConnectorTestBase
    with TestJsonSchemaHelper {

  override val m_package_descr   = m_v1_package_descr;
  override val m_package_to_test = m_v1_package;

  // Run the tests
  testsFor( jsonParsingFeatures( m_package_to_test, m_package_descr ) );

}  // TestJsonSchemaHelper_V1



/**
 *  Test bug fixes using the DataSource v2 package.
 */
class TestJsonSchemaHelper_V2
  extends SparkConnectorTestBase
    with TestJsonSchemaHelper {

  override val m_package_descr   = m_v2_package_descr;
  override val m_package_to_test = m_v2_package;

  // Run the tests
  testsFor( jsonParsingFeatures( m_package_to_test, m_package_descr ) );

}  // end TestJsonSchemaHelper_V2