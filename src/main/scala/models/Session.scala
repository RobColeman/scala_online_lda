package models





import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._



object Session {

  def apply(instanceId: String,
            entityId: String,
            eventCount: Map[String,Int],
            parentIds: Option[List[String]] = None, // chain of parents, ordered, all the way up
            startTs: Int = -1,
            endTs: Int = -1,
            gap: Option[Double] = None) = new Session(instanceId = instanceId,
                                                      entityId = entityId,
                                                      eventCount = eventCount,
                                                      parentIds = parentIds, // chain of parents, ordered, all the way up
                                                      startTs = startTs,
                                                      endTs = endTs,
                                                      gap = gap)

  def fromDBObject(doc: DBObject): Session = {
    new Session(instanceId = doc.get("instance_id").asInstanceOf[String],
      entityId = doc.get("entity_id").asInstanceOf[String],
      eventCount = doc.get("event_count").asInstanceOf[Map[String, Int]],
      parentIds = {
        val returned = doc.get("parent_ids")
        if (returned == null) {
          None
        } else {
          Some(returned.asInstanceOf[String].split(":").toList)
        }
      },
      startTs = doc.get("start_ts").asInstanceOf[Int],
      endTs = doc.get("end_ts").asInstanceOf[Int],
      gap = {
        doc.get("gap").asInstanceOf[Int] match {
          case 0 => None
          case sessionGap => Some(sessionGap)
        }
      }
    )
  }
}

class Session(instanceId: String,
                              entityId: String,
                              eventCount: Map[String,Int],
                              parentIds: Option[List[String]] = None, // chain of parents, ordered, all the way up
                              startTs: Int = -1,
                              endTs: Int = -1,
                              gap: Option[Double] = None) {

  val senjaKey = parentIds match {
    case None => "senja:" + entityId
    case Some(parents) => "senja:" + parentIds.mkString(":") + entityId
  }

  def toJson: JValue = {
    val json = ("instanceId" -> instanceId) ~
      ("entityId" -> entityId) ~
      ("parentIds" -> parentIds) ~
      ("senjaKey" -> senjaKey) ~
      ("startTs" -> startTs) ~
      ("endTs" -> endTs) ~
      ("gap" -> gap) ~
      ("eventCount" -> eventCount)
    render(json)
  }

  def toJsonString: String = compact(this.toJson)

  def toDBObject: DBObject = {
    MongoDBObject("instance_id" -> instanceId,
      "entity_id" -> entityId,
      "parent_ids" -> parentIds,
      "senjaKey" -> senjaKey,
      "event_count" -> eventCount,
      "start_ts" -> startTs,
      "end_ts" -> endTs,
      "gap" -> gap)
  }

}