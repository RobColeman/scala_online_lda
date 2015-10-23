package mongo

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.types.ObjectId
import dataModels.Session

/**
 * Created by rcoleman on 10/15/15.
 */
object MongoClient {

  val url = "10.0.3.43"
  val port = 27017
  val dbName = "lessneglect2_development"

  val server = new ServerAddress(url, port)
  lazy val db = MongoConnection(url, port)(dbName)

  def getCollectionByName(collectionName: String): MongoCollection = {
    val collection = MongoConnection(url, port)(dbName)(collectionName)
    collection.readPreference = ReadPreference.Secondary
    collection
  }

  lazy val accountsCollection = getCollectionByName("accounts")
  lazy val peopleCollection = getCollectionByName("people")
  lazy val eventTypesCollection = getCollectionByName("event_types")
  lazy val sessionsCollection = getCollectionByName("sessions")

  def byProject(projectId: String): MongoDBObject = MongoDBObject("project_id" -> new ObjectId(projectId))
  def byInstance(instanceId: String): MongoDBObject = MongoDBObject("instance_id" -> new ObjectId(instanceId))
  def byInstanceString(instanceId: String): MongoDBObject = MongoDBObject("instance_id" -> instanceId)
  def byEntityId(entityId: String): MongoDBObject = MongoDBObject("entity_id" -> new ObjectId(entityId))
  def byInstance(instanceId: String, entityId: String): MongoDBObject = MongoDBObject("instance_id" -> new ObjectId(instanceId))
  def byInstanceAndEntityId(instanceId: String, entityId: String): MongoDBObject = MongoDBObject(
    "instance_id" -> new ObjectId(instanceId),
    "entity_id" -> new ObjectId(entityId)
  )

  def getAllEventTypes(projectId: String): MongoCursor = {
    eventTypesCollection.find(byProject(projectId))
  }

  def getAllEventTypesIds(projectId: String): Set[String] = {
    eventTypesCollection.find(byProject(projectId)).map{ _.get("_id").toString}.toSet
  }

  def getAllSessions(instanceId: String): Iterator[Session] = {
    sessionsCollection.find(byInstanceString(instanceId)).map(Session(_))
  }

  def getAllSessionsRDD(sc: SparkContext, instanceId: String): RDD[Session] = {
    sc.parallelize(sessionsCollection.find(byInstanceString(instanceId)).map(Session(_)).toSeq)
  }

  def getSessionBatch(instanceId: String)(offsetLimitPair: (Int, Int)): MongoCursor = {
    val (threadOffset, threadAccountLimit) = offsetLimitPair
    val query = byInstance(instanceId)
    threadAccountLimit > 0 match {
      case true => sessionsCollection.find(query).skip(threadOffset).limit(threadAccountLimit)
      case false => sessionsCollection.find(query).skip(threadOffset)
    }
  }

  def getAllSessionsBatches(instanceId: String, nBatches: Int): Seq[(Int,Int)] = {
    val query = byInstance(instanceId)
    val count = sessionsCollection.find(query).count()
    val sessionsPerBatch = count / (nBatches - 1)
    sessionsPerBatch match {
      case 0 => List((0, -1))
      case _ =>
        val offsetLimitPairs = (0 to count by sessionsPerBatch).map { o => (o, sessionsPerBatch) }.toList
        offsetLimitPairs.dropRight(1) ++ List((offsetLimitPairs.last._1, -1))
    }
  }

}
