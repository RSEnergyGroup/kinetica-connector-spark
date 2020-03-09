package com.kinetica.spark.util

import com.typesafe.scalalogging.LazyLogging

private[kinetica] case class TableNameWithSchema(tableName: String, schemaName: Option[String] = None)
private[kinetica] object TableNameWithSchema {
  def apply(tableName: String, schemaName: String): TableNameWithSchema = TableNameWithSchema(tableName, Some(schemaName))
}

/*
Helper object for doing smart extraction of table and schema name
 */
private[kinetica] object RegexHelper extends LazyLogging {

  private val tableSchemaPattern= "^(?:(?:\\[)?([^\\]]+)(?:\\])?)\\.(?:(?:\\[)?([^\\]]+)(?:\\])?)$".r
  //private val tableSchemaPattern= "^(?:(?:\\[)?([^\\]\\s]+)(?:\\])?)\\.(?:(?:\\[)?([^\\]\\s]+)(?:\\])?)$".r
  private val checkBrackets = "\\[+.*\\]+".r

  def extractTableAndSchemaName(tableName: String, tableContainsSchemaName: Boolean): TableNameWithSchema = {

    logger.debug(s"Extracting tablename and schema from '$tableName'")

    // start by trimming and clearing prefix/suffix periods
    val table = Option(tableName).getOrElse("").trim().stripSuffix(".").stripPrefix(".")

    // if table is null or empty, throw an error
    if (table.isEmpty) {
      val ex = new IllegalArgumentException("Unable to extract table or schema name from a null or empty string")
      logger.error(ex.getMessage, ex)
      throw ex
    }

    if(tableContainsSchemaName && (table contains ".")) {

      // if square brackets are present, use regex to grab schema and table names
      if(checkBrackets.findAllMatchIn(table).nonEmpty) {

        val matches = tableSchemaPattern.findAllMatchIn(table).flatMap(m => m.subgroups).toArray

        TableNameWithSchema(matches.tail.mkString("."),matches.head)

      } else {
        // use the old method of just splitting on the dot separator
        val tableParams: Array[String] = table.split("\\.")

        if(tableParams.length >= 1) {

          // everything after the collection name is the table name (which is allowed to have periods)
          TableNameWithSchema(tableParams.tail.mkString("."), tableParams.head)

        } else {
          TableNameWithSchema(table)
        }
      }
    } else {
      TableNameWithSchema(table)
    }
  }

// (?<!\[\s*)
  // ^(?<collectionName>\[.*?\])(?:\.)(?<tableName>\[.*?\])$
}
