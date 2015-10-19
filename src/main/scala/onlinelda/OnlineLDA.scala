package onlinelda

import dataModels.Session

import breeze.linalg._
import breeze.numerics.{log, digamma, exp, pow, lgamma}
import breeze.stats.distributions.Gamma
import scala.collection.mutable


import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object OnlineLDA {

  val meanchangethresh: Double = 0.001

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

/**
 *
 * @param eventSet the dictionary of events
 * @param K number of clusters
 * @param D number of total sessions we are training on, across all mini-batches, default: 1/K
 * @param alphaParam Hyperparameter for prior on weight vectors theta, default: 1/K
 * @param etaParam Hyperparameter for prior on topics beta
 * @param tauParam A (positive) learning parameter that downweights early iterations
 * @param kappa exponential decay learning rate, [0.5, 1.0]
 */
class OnlineLDA(eventSet: Set[String], val K: Int, val D: Long,
                alphaParam: Option[Double] = None,
                etaParam: Option[Double] = None,
                tauParam: Double = 1024,
                val kappa: Double = 0.7,
                val verbose: Boolean = true) extends Serializable {

  val alpha: Double = alphaParam match {
    case None => 1.0 / K.toDouble
    case Some(p) => p
  }

  val eta: Double = etaParam match {
    case None => 1.0 / K.toDouble
    case Some(p) => p
  }

  // the dictionary
  val events: Set[String] = eventSet.map(_.toLowerCase)
  val eventIndexes: Map[String,Int] = eventSet.zipWithIndex.toMap

  // size of the dictionary
  val W: Int = events.size  // careful not to overload this Int
  val tau: Double = tauParam + 1

  // the weight (0,1) we apply to the information from this mini-batch
  var rhoT: Double = 0.0
  var updateCount: Int = 0

  // Initialize the variational distribution
  var lambda: DenseMatrix[Double] = {
    val g = Gamma(100, 1.0 / 100.0)
    val a = ( 0 until K * W ).map{ i => g.draw() }.toArray
    new DenseMatrix(K, W.toInt, a)
  }
  var ELogBeta: DenseMatrix[Double] = OnlineLDA.dirichletExpectation(lambda)
  var expELogBeta: DenseMatrix[Double] = exp(ELogBeta)


  def convertSessionECtoIndexCount(sessions: Seq[Session]): Seq[Map[Int, Int]] = {
    sessions.map { _.eventCount.map { case (etId, count) => eventIndexes(etId) -> count } }
  }

  /**
   *
   * @param sessions
   * @return nested map of session -> cluster -> gamma
   *     where gamma is the parameters to the variational distribution
   *     over the topic weights theta for the documents analyzed in this
   *     update
   */
  def updateWithBatch(sessions: Seq[Session]): (DenseMatrix[Double], Double) = {

    // rhot will be between 0 and 1, and says how much to weight
    // the information we got from this mini-batch.
    this.rhoT = pow( this.tau + this.updateCount, - this.kappa)
    // Do an E step to update gamma, phi | lambda for this
    // mini-batch. This also returns the information about phi that
    // we need to update lambda.
    val (gamma, sstats): (DenseMatrix[Double], DenseMatrix[Double]) = this.expectationStep(sessions)
    // Estimate held-out likelihood for current values of lambda.
    val bound = this.approximateBound(sessions, gamma)
    // Update lambda based on documents.
    val lambdaUpdate: DenseMatrix[Double] = sstats.map{
      x => ((x * this.D )/ sessions.length.toDouble ) + this.eta
    }

    this.lambda = (( 1 - this.rhoT ) * this.lambda ) + (this.rhoT * lambdaUpdate)

    this.ELogBeta = OnlineLDA.dirichletExpectation(this.lambda)
    this.expELogBeta = exp(ELogBeta)
    this.updateCount = this.updateCount + 1

    (gamma, bound)
  }



  def approximateBound(sessions: Seq[Session], gamma: DenseMatrix[Double]): Double = {
    val etCountByToken: Seq[Map[Int, Int]] = convertSessionECtoIndexCount(sessions)
    val batchD: Int = etCountByToken.length

    var score = 0.0
    // may be computing this multiple times
    val batchElogtheta: DenseMatrix[Double] = OnlineLDA.dirichletExpectation(gamma)
    val batchExpElogtheta: DenseMatrix[Double] = exp(batchElogtheta)

    // E[log p(sessions | theta, id)]
    val sessionsScoreContribution = etCountByToken.zipWithIndex.map{ case (etCount, sessionIdx) =>

      val scoreAugForSession: Double = etCount.map{ case (etIdx, count) =>
        val temp: DenseVector[Double] = batchElogtheta(sessionIdx, ::).t + this.ELogBeta(::, etIdx)
        val tmax: Double = max(temp)
        val phiNorm: Double = log( sum( temp.map{ x => exp( x - tmax )} ) ) + tmax
        count * phiNorm
      }.sum

      scoreAugForSession
    }.sum

    // add to score
    score += sessionsScoreContribution

    // E[log p(theta | alpha) - log q(theta | gamma)]
    val ElogTAminuslogTg0: Double = sum(
      gamma.pairs.map{ case (idxPair,v) => batchElogtheta(idxPair) * (this.alpha - v) }
      )
    val ElogTAminuslogTg1: Double = sum(
      gamma.map{ x => lgamma(x) - lgamma(this.alpha) }
      )
    val ElogTAminuslogTg2: Double = sum(
      sum(gamma, Axis._1).map{ x => lgamma(this.alpha * this.K) - lgamma(x) }
    )

    // add to score
    score += (ElogTAminuslogTg0 + ElogTAminuslogTg1 + ElogTAminuslogTg2)

    // normalize by batch size fraction
    score = score * (this.D / batchD.toDouble)


    /*
        # E[log p(beta | eta) - log q (beta | lambda)]
        score = score + n.sum((self._eta-self._lambda)*self._Elogbeta)
        score = score + n.sum(gammaln(self._lambda) - gammaln(self._eta))
        score = score + n.sum(gammaln(self._eta*self._W) -
                              gammaln(n.sum(self._lambda, 1)))
     */

    score
  }


  def expectationStep(sessions: Seq[Session]): (DenseMatrix[Double], DenseMatrix[Double]) = {



    ???
  }


  def helpOutPerplexityEstimate(sessionIdxCount: Seq[Map[Int, Int]], variationalLowerBound: Double): Double = {
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













