package onlinelda

import dataModels.Session

import breeze.linalg.{DenseVector, DenseMatrix, sum, Axis}
import breeze.numerics.{digamma,exp}
import breeze.stats.distributions.Gamma

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object OnlineLDA {

  def psi(x: DenseMatrix[Double]): DenseMatrix[Double] = {
    x.map{ i => digamma(i)}
  }
  def psi(x: DenseVector[Double]): DenseVector[Double] = {
    x.map{ i => digamma(i)}
  }
  def psi(x: Double): Double = digamma(x)
  def gammaln = ???

  /**
   * For each vector theta ~ Dir(alpha) computes E[log(theta)] | alpha
   *
   * @param alpha - Matrix with rows corresponding to topics/clusters
   */
  def dirichletExpectation(alpha: DenseMatrix[Double]): DenseMatrix[Double] = {
    val cols = alpha.cols
    val rows = alpha.rows
    val eV: DenseVector[Double] = psi(sum(alpha, Axis._1))

    new DenseMatrix(rows,cols,
      alpha.toArray.zipWithIndex.map{ case(x, idx) =>
      val r = idx.toInt % cols.toInt
      psi(x) - eV(r)
    })
  }

  /**
   *  For vector theta ~ Dir(alpha) computes E[log(theta)] | alpha
   *
   * @param alpha
   * @return
   */
  def dirichletExpectation(alpha: DenseVector[Double]): DenseVector[Double] = {
    val e: Double = psi(sum(alpha))
    psi(alpha).map{ _ - e }
  }


}

class OnlineLDA(eventSet: Set[String], val K: Int, val D: Long,
                alphaParam: Option[Double] = None, etaParam: Option[Double] = None,
                tau0: Double = 1024, val kappa: Double = 0.7) {

  val alpha: Double = alphaParam match {
    case None => 1.0 / K.toDouble
    case Some(p) => p
  }

  val eta: Double = etaParam match {
    case None => 1.0 / K.toDouble
    case Some(p) => p
  }

  val events: Set[String] = eventSet.map(_.toLowerCase)
  val eventIndexes: Map[String,Int] = eventSet.zipWithIndex.toMap

  val W: Int = events.size  // careful not to overload this Int
  val tau: Double = tau0 + 1
  var updateCount: BigInt = 0

  // Initialize the variational distribution

  var lambda: DenseMatrix[Double] = {
    val g = Gamma(100, 1.0 / 100.0)
    val a = ( 0 until K * W ).map{ i => g.draw() }.toArray
    new DenseMatrix(K, W.toInt, a)
  }
  var ELogBeta: DenseMatrix[Double] = OnlineLDA.dirichletExpectation(lambda)
  var expELogBeta: DenseMatrix[Double] = ELogBeta.map( x => exp(x) )

  var variationalLowerBound: Double = Double.MinValue

  def convertSessionECtoIndexCount(sessions: List[Session]): List[Map[Int, Int]] = {
    sessions.map { _.eventCount.map { case (etId, count) => eventIndexes(etId) -> count } }
  }

  def updateLambda: Unit = ???
  // calls approximateBound
  // calls expectationStep

  def approximateBound: Unit = ???


  def expectationStep: Unit = ???

  def helpOutPerplexityEstimate(sessionIdxCount: List[Map[Int, Int]]): Double = {
    val n: Long = sessionIdxCount.length
    val totalEvents: Long = sessionIdxCount.map{ _.values.sum }.sum
    val perWordBound: Double = ( variationalLowerBound * n ) / (D * totalEvents.toDouble)
    exp(-perWordBound)
  }

  def toJson = ???
  // as a JValue

  def toJsonString = ???
  // as a JSONstring

  def saveModel = ???
  // save model to file-system

}













