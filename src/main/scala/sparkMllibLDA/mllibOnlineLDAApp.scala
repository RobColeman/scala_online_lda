package sparkMllibLDA


import dataModels.Session
import mongo.MongoClient

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{LDA,LDAModel,LDAOptimizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.tools.nsc.io
import java.lang.System.currentTimeMillis


import java.io.StringWriter
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._
import java.io.FileWriter
import java.io.BufferedWriter

object mllibOnlineLDAApp {

  def main(args: Array[String]): Unit = {


    val k = 5
    val startTime = (currentTimeMillis() / 1000.0).toInt

    val appName = "LDA-test"
    val conf = new SparkConf().setAppName(appName).setMaster("local[16]")
    val sc = new SparkContext(conf)

    val instanceId = "517eda23c82561f72a000005"

    val sessions: List[Session] = MongoClient.getAllSessions(instanceId).toList
    val sessionsRDD: RDD[Session] = sc.parallelize(sessions)


    val events: Set[String] = helper.getAllSessionEventTypes(sessionsRDD)
    val dictionary: Map[String,Int] = events.toList.sorted.zipWithIndex.toMap
    val l: Int = events.size
    val trainingData: RDD[(Long,Vector)] = sessionsRDD.map(helper.eventCountToVector(dictionary,l)).zipWithIndex.map{ _.swap}.cache()

    val untrainedLDAModel = new LDA().setK(k)
    val trainedLDAModel: LDAModel = untrainedLDAModel.run(trainingData)

    val model = TrainedTopicModel(model = trainedLDAModel, dictionaryIdxId = dictionary.map{_.swap})

    model.printTopicsMatrix

    val path = s"/tmp/SessionLDAModel_$startTime.csv"
    model.saveTopicMatrixToCSV(path)

  }

}

object helper {
  def eventCountToVector(dictionary: Map[String,Int], l: Int)(session: Session): Vector = {
    val dict = dictionary
    val len = l
    val ec = session.eventCount
    val (indices,values) = ec.toSeq.sortBy( _._1 ).map{ p => (dict(p._1), p._2.toDouble) }.unzip
    val sp = new SparseVector(size = len, indices = indices.toArray, values = values.toArray)
    sp.toDense
  }

  def getAllSessionEventTypes(sessionsRDD: RDD[Session]): Set[String] = {
    sessionsRDD.map{ _.eventCount.keySet}.reduce( _.union(_) )
  }


}

object TrainedTopicModel {
  def apply(model: LDAModel, dictionaryIdxId: Map[Int,String]): TrainedTopicModel = new TrainedTopicModel(model, dictionaryIdxId)
}

class TrainedTopicModel(val model: LDAModel, val dictionary: Map[Int,String]) {

  val topicDistribution = makeTopicModelMap(model.describeTopics())
  val nTopics = topicDistribution.size
  val nEvents = dictionary.size
  val normalizedMatrix = {
    val rows = model.topicsMatrix.numRows
    val cols = model.topicsMatrix.numCols
    val normalizedAsArray = model.topicsMatrix.toArray.grouped(nEvents).flatMap{ l =>
      val s: Double = l.sum
      l.map{ _ / s}
    }.toArray
    Matrices.dense(rows, cols, normalizedAsArray)
  }

  def printTopicModelsLong: Unit = {
    topicDistribution.toSeq.sortBy( _._1 ).foreach{ case (cluster,weightMap) =>
        val weightString: String = weightMap.toSeq.sortBy(_._1).map{ case (etId,prob) =>
          s"${etId} : ${prob} "}.mkString(", ")
        println(s"Cluster $cluster : " + weightString)
    }
  }

  def printTopicsMatrix: Unit = {
    println(normalizedMatrix.transpose)
  }

  def saveTopicMatrixToCSV(path: String): Unit = {
    val rows = model.topicsMatrix.numRows
    val cols = model.topicsMatrix.numCols
    val MatrixRows: List[Array[String]] = normalizedMatrix.transpose.toArray.grouped(nEvents).toList.map{ _.map{ _.toString} }
    val out = new BufferedWriter(new FileWriter(path))
    val writer = new CSVWriter(out)
    writer.writeAll(MatrixRows)
    out.close()
  }

  def makeTopicModelMap(description: Array[(Array[Int], Array[Double])]): Map[Int,Map[Int,Double]] = {
      description.zipWithIndex.map{ _.swap }.map{ case (cluster,etIdEightPairs) =>
      cluster -> etIdEightPairs.zipped.toMap
    }.toMap
  }

}
