package com.kinetica.spark.json

import com.gpudb.protocol.ShowTypesResponse
import com.kinetica.spark.json.KineticaJsonSchema.KineticaSchemaMetadata
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.functional.syntax.{unlift, _}
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map


/**
 * Case class used as the top level of a kinetica schema definition,
 * used to help make kinetica schema compatible for inclusion in other json documents
 * @param kinetica_schema a [[KineticaSchemaMetadata]] container
 */
case class KineticaSchemaRoot(kinetica_schema: Option[KineticaSchemaMetadata] = None)

/**
 * Encapsulates serialization and deserialization of a Kinetica type
 * and table definition using the PLAY framework
 */
object KineticaJsonSchema extends KineticaSchemaRoot with LazyLogging {

  /**
   * Json container object to hold typeProperties, schema, and flag for replicated tables
   * @param typeProperties a map of the type properties retrievable from the kinetica API
   * @param typeSchema the type schema from the kinetica api
   * @param isReplicated indicates if the table is replicated instead of sharded
   */
  case class KineticaSchemaMetadata(typeProperties: Map[String, Seq[String]], typeSchema: Option[KineticaSchemaDefinition], isReplicated: Boolean = false)

  /** Json container object representing the the type schema from the kinetica api */
  case class KineticaSchemaDefinition(recordType: String, name: String, fields: Seq[KineticaFieldDefinition])

  /** Json container object representing the the field definitions from the kinetica api */
  case class KineticaFieldDefinition(name: String, typeProperties: Seq[String])

  /** Decodes a Kinetica schema root definition from Json */
  def apply(jsonString: String): KineticaSchemaRoot = {
    import implicits._

    val result = Json.fromJson[KineticaSchemaRoot](Json.parse(jsonString))
    if(result.isSuccess)
    {
      result.get
    } else {
      logger.error(result.toString)
      throw new Exception(result.toString)
    }
  }

  /** Encodes a Kinetica schema root definition to Json */
  def encodeSchemaToJson(kineticaSchemaRoot: KineticaSchemaRoot): String = {
    import implicits._
    Json.prettyPrint(Json.toJson[KineticaSchemaRoot](kineticaSchemaRoot))
  }

  def encodeSchemaMetadataToJson(kineticaSchemaMetadata: KineticaSchemaMetadata): String = {
    import implicits._
    Json.prettyPrint(Json.toJson[KineticaSchemaMetadata](kineticaSchemaMetadata))
  }

  /** Decodes a Kinetica schema definition from Json */
  def decodeKineticaSchemaDefinition(jsonString: String): Option[KineticaSchemaDefinition] = {
    import implicits._
    Some(Json.fromJson[KineticaSchemaDefinition](Json.parse(jsonString)).get)
  }

  /** Decodes a Kinetica schema definition from Json */
  def decodeKineticaSchemaMetadataDefinition(jsonString: String): Option[KineticaSchemaMetadata] = {
    import implicits._
    Some(Json.fromJson[KineticaSchemaMetadata](Json.parse(jsonString)).get)
  }

  /** Encodes a Kinetica schema definition to Json */
  private[kinetica] def encodeKineticaSchemaDefinition(schema: Option[KineticaSchemaDefinition]): String  = {
    import implicits._
    Json.prettyPrint(Json.toJson[KineticaSchemaDefinition](schema.get))
  }

  /** Converts an API response to the Kinetica json schema definition object */
  private[kinetica] def apply(typeInfo: ShowTypesResponse, isReplicated: Boolean = false): KineticaSchemaMetadata = {
    val props = typeInfo.getProperties.asScala.head.asScala.mapValues(_.toSeq)
    val typeDef = decodeKineticaSchemaDefinition(typeInfo.getTypeSchemas.asScala.head)
    KineticaSchemaMetadata(props.toMap, typeDef, isReplicated)
  }

  /** encapsulates the implict methods required from the PLAY json API.
   */
  object implicits {

    implicit val formatKineticaFieldDefinition: Format[KineticaFieldDefinition] = new Format[KineticaFieldDefinition] {
      override def writes(field: KineticaFieldDefinition): JsValue = Json.obj(
        "name" -> JsString(field.name),
        "type" -> {
          if (field.typeProperties.length == 1) {
            // represent as string in json
            JsString(field.typeProperties.head)
          } else {
            // represent as array in json
            JsArray(field.typeProperties.map(s => JsString(s)))
          }
        }
      )

      override def reads(json: JsValue): JsResult[KineticaFieldDefinition] = {
        try {
          val name = json \ "name" match {
            case JsDefined(JsString(str)) => str
            case _ => throw new Exception("cannot parse name")
          }
          val types = json \ "type" match {
            case JsDefined(JsArray(value)) => value.map(_.as[String])
            case JsDefined(JsString(str)) => Seq(str)
            case _ => throw new Exception("cannot parse type")
          }
          JsSuccess(KineticaFieldDefinition(name, types))
        }
        catch {
          case _: Throwable =>
            logger.error(s"unable to parse json: $json")
            JsError(s"unable to parse json: $json")
        }
      }
    }

    implicit val writesKineticaSchemaDefinition: OWrites[KineticaSchemaDefinition] = (
      (JsPath \ "type").write[String] and
        (JsPath \ "name").write[String] and
        (JsPath \ "fields").write[Seq[KineticaFieldDefinition]]
      ) (unlift(KineticaSchemaDefinition.unapply))

    implicit val readsKineticaSchemaDefinition: Reads[KineticaSchemaDefinition] = (
      (JsPath \ "type").read[String] and
        (JsPath \ "name").read[String] and
        (JsPath \ "fields").read[Seq[KineticaFieldDefinition]]
      ) (KineticaSchemaDefinition.apply _)

    implicit val schemaMapReads: Reads[Map[String, Seq[String]]] = new Reads[Map[String, Seq[String]]] {
      def reads(jv: JsValue): JsResult[Map[String, Seq[String]]] = {
        JsSuccess(jv.as[scala.collection.immutable.Map[String, Array[String]]].map {
          case (k, v) =>
            k -> v.toSeq
        })
      }
    }

    implicit val kineticaModelFormat: OFormat[KineticaSchemaMetadata] = Json.using[Json.WithDefaultValues].format[KineticaSchemaMetadata]

    implicit val kineticaSchemaRootModel: OFormat[KineticaSchemaRoot] = Json.using[Json.WithDefaultValues].format[KineticaSchemaRoot]
  }

}


