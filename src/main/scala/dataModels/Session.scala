package dataModels

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._


object Session {

  val maxSessionGapSec = 60*90    // 90 min, in seconds
  val maxSessionLenSec = 60*60*24 // 24 hours, in seconds

  def apply(instanceId: String,
            entityId: String,
            eventCount: Map[String,Int],
            parentIds: Option[List[String]] = None, // chain of parents, ordered, all the way up
            startTs: Int = -1,
            endTs: Int = -1,
            gap: Option[Double] = None): Session = {
    new Session(instanceId = instanceId,
      entityId = entityId,
      eventCount = eventCount,
      parentIds = parentIds,
      startTs = startTs,
      endTs = endTs,
      gap = gap
    )
  }

  // from mongoDBObject
  def apply(doc: DBObject): Session = {

    val parentIds = {
      val returned = doc.get("parent_ids")
      if (returned == null) {
        None
      } else {
        Some(returned.asInstanceOf[String].split(":").toList)
      }
    }
    val eventCount: Map[String, Int] = doc.get("event_count").asInstanceOf[java.util.Map[String, Int]].asScala.toMap

    val gap: Option[Double] = {
      val foundGap = if (doc.get("gap") == null) None else Some(doc.get("gap").asInstanceOf[Int].toDouble)
      foundGap match {
        case None => None
        case Some(g) => g match {
          case 0 => None
          case _ => Some(g)
        }
      }
    }

    new Session(instanceId = doc.get("instance_id").asInstanceOf[String],
      entityId = doc.get("entity_id").asInstanceOf[String],
      eventCount = eventCount,
      parentIds = parentIds,
      startTs = doc.get("start_ts").asInstanceOf[Int],
      endTs = doc.get("end_ts").asInstanceOf[Int],
      gap = gap
    )
  }

}


class Session(val instanceId: String,
              val entityId: String,
              val eventCount: Map[String,Int],
              val parentIds: Option[List[String]] = None, // chain of parents, ordered, all the way up
              val startTs: Int = -1,
              val endTs: Int = -1,
              val gap: Option[Double] = None) extends Serializable {

  val senjaKey = parentIds match {
    case None => "senja:" + entityId
    case Some(parents) => "senja:" + parentIds.mkString(":") + entityId
  }

  def toJson: JValue = {
    val json = ("instanceId" -> instanceId) ~
      ("entityId" -> entityId) ~
      ("parentIds" -> parentIds) ~
      ("startTs" -> startTs) ~
      ("endTs" -> endTs) ~
      ("gap" -> gap) ~
      ("eventCount" -> eventCount)
    render(json)
  }

  override def toString: String = {
    toJsonString
  }

  def toJsonString: String = compact(this.toJson)

  def toDBObject: DBObject = {
    MongoDBObject("instance_id" -> instanceId,
      "entity_id" -> entityId,
      "parent_ids" -> parentIds,
      "event_count" -> eventCount,
      "start_ts" -> startTs,
      "end_ts" -> endTs,
      "gap" -> gap)
  }

}