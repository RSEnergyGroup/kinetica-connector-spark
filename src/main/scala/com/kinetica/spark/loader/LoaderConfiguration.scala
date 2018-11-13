package com.kinetica.spark.loader

import java.io.Serializable

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions.asScalaBuffer
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util.Constants._
import com.typesafe.scalalogging.LazyLogging
import com.kinetica.spark.LoaderParams
import org.apache.spark.SparkContext

@SerialVersionUID(-2502861044221136156L)
class LoaderConfiguration(sc:SparkContext, params: Map[String, String]) extends LoaderParams(sc, params) with Serializable with LazyLogging {

    @BeanProperty
    val sqlFileName: String = params.get(CONNECTOR_SQLFILE_PARAM).getOrElse(null)

    @BeanProperty
    val dataPath: String = params.get(CONNECTOR_DATAPATH_PARAM).getOrElse(null)

    @BeanProperty
    val dataFormat: String = params.get(CONNECTOR_DATAFORMAT_PARAM).getOrElse(null)

    @BooleanBeanProperty
    val useTemplates: Boolean = params.get(KINETICA_USETEMPLATES_PARAM).getOrElse("false").toBoolean

    @BooleanBeanProperty
    val useJsonTemplate: Boolean = params.get(KINETICA_USE_JSON_SCHEMA).getOrElse("false").toBoolean

    @BeanProperty
    val partitionRows: Int = params.get(KINETICA_PARTROWS_PARAM).getOrElse("-1").toInt

    @BooleanBeanProperty
    val csvHeader: Boolean = params.get(KINETICA_CSV_HEADER).getOrElse("false").toBoolean
}
