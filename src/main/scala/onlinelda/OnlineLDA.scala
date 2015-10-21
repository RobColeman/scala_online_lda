package onlinelda

import dataModels.Session

import breeze.linalg._
import breeze.numerics.{log, digamma, exp, pow, lgamma, abs}
import breeze.stats.mean
import breeze.stats.distributions.Gamma


import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.immutable.IndexedSeq


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
    val eV: DenseVector[Double] = psi(sum(alpha, Axis._1))
    val a = alpha.pairs.iterator.map{ t =>
      val r: Int = t._1._1
      val v: Double = t._2
      psi(v) - eV(r)
    }.toArray

    new DenseMatrix(alpha.rows, alpha.cols, a)
  }

  /**
   *  For vector theta ~ Dir(alpha) computes E[log(theta)] | alpha
   *
   * @param alpha
   * @return
   */
  def dirichletExpectation(alpha: DenseVector[Double]): DenseVector[Double] = {
    val e: Double = psi(sum(alpha))
    psi(alpha).map{ v => v - e }
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
                val verbose: Boolean = true,
                val convergenceThreshold: Double = 0.001,
                val maxIterations: Int = 100) extends Serializable {

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
    new DenseMatrix(K, W.toInt, g.sample(K * W).toArray)
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

  def expectationStep(sessions: Seq[Session]): (DenseMatrix[Double], DenseMatrix[Double]) = {
    val etCountByToken: Seq[Map[Int, Int]] = convertSessionECtoIndexCount(sessions)
    val batchSessN: Int = etCountByToken.length

    // this can be created while iterating through each session
    val gammaDist = Gamma(100, 1.0 / 100.0)
    val gamma: DenseMatrix[Double] = {
      // gamma = 1*n.random.gamma(100., 1./100., (batchSessN, self._K))
      new DenseMatrix(batchSessN, K, gammaDist.sample(batchSessN * K).toArray)
    }

    val bELogTheta = OnlineLDA.dirichletExpectation(gamma)
    val bExpELogTheta = exp(bELogTheta)

    val sstats = DenseMatrix.zeros[Double](this.K,this.W)
    // for each document (d) update that document's gamma and phi

    // slice by columns (sessions)
    // this may be the parallelizable part
    etCountByToken.zipWithIndex.foreach{ case (etCountMap, s) =>

      val (etIndicesOrdered, etCountsOrdered): (IndexedSeq[Int],IndexedSeq[Int]) =
        etCountMap.toIndexedSeq.sortBy( _._1 ).unzip

      val orderedEtCounts: DenseVector[Double] = DenseVector(etCountsOrdered.map{ _.toDouble }toArray)

      val expELogBetaS = this.expELogBeta(::, etIndicesOrdered).toDenseMatrix

      /* loop until convergence
       * Mutables:
       */
      var it = 0
      var meanChange: Double = Double.MaxValue
      var lastGammaS: DenseVector[Double] = gamma(s,::).t
      var thisGammaS: DenseVector[Double] = DenseVector.zeros[Double](this.K)
      var ExpELogThetaS: DenseVector[Double] = bExpELogTheta(s,::).t
      // this could be initialized as zeros, might be a faster constructor

      var phinorm: DenseVector[Double] = {
        val tdv: Transpose[DenseVector[Double]] = ExpELogThetaS.t * expELogBetaS
        tdv.t + 1e-100 // for zeros
      }
      var ctsNormed: DenseVector[Double] = orderedEtCounts :/ phinorm // this is really big for some reason
      // loop //
      do {
        /*check this, scaling is way off*/

        lastGammaS = thisGammaS

        ctsNormed = orderedEtCounts :/ phinorm
        thisGammaS = {
          val prod: DenseVector[Double] = ExpELogThetaS :* (expELogBetaS * ctsNormed)
          prod.map{ _ + this.alpha }
        }

        // saving space
        // val ELogThetaS: DenseVector[Double] = OnlineLDA.dirichletExpectation(thisGammaS)
        // ExpELogThetaS = exp(ELogThetaS)
        ExpELogThetaS = exp(OnlineLDA.dirichletExpectation(thisGammaS))

        phinorm = {
          val tdv: Transpose[DenseVector[Double]] = ExpELogThetaS.t * expELogBetaS
          tdv.t + 1e-100 // for zeros
        }

        it += 1
        meanChange = mean(abs(thisGammaS - lastGammaS))
      } while (hasNotConverged(meanChange) & hasNotRachedMaxIterations(it))

      thisGammaS.toArray.zipWithIndex.foreach{ case (v,idx) => gamma(s,idx) = v}
      val outer = {
        val eeltsM = DMfromVectorReplication(ExpELogThetaS,ctsNormed.length)
        val ctsNM = DMfromVectorReplication(ctsNormed,ExpELogThetaS.length)
        eeltsM.t :* ctsNM
      }
      etIndicesOrdered.zipWithIndex.foreach{ case (etIdx,outerIdx) =>
        val outerP: DenseVector[Double] = outer(::,outerIdx)
        val currentSStats: DenseVector[Double] = sstats(::,etIdx)
        val newCol: DenseVector[Double] = outerP + currentSStats
        newCol.toArray.zipWithIndex.foreach{ case (v,rIdx) => outer(rIdx,etIdx)
        }
      }
    } // per session

    val sstatsOut = sstats :* this.expELogBeta

    (gamma,sstatsOut)
  }

  private def hasNotConverged(meanChange: Double): Boolean = meanChange > convergenceThreshold
  private def hasNotRachedMaxIterations(it: Int): Boolean = it < maxIterations
  private def DMfromVectorReplication(dv: DenseVector[Double], repTimes: Int): DenseMatrix[Double] = {
    var dm: DenseMatrix[Double] = DenseMatrix.vertcat(dv.toDenseMatrix,dv.toDenseMatrix)
    var cnt = 2
    while (cnt < repTimes) {
      dm = DenseMatrix.vertcat(dm,dv.toDenseMatrix)
      cnt += 1
    }
    dm
  }

  /**
   *
   * @param sessions
   * @param gamma
   * @return
   */
  def approximateBound(sessions: Seq[Session], gamma: DenseMatrix[Double]): Double = {
    val etCountByToken: Seq[Map[Int, Int]] = convertSessionECtoIndexCount(sessions)
    val batchSessN: Int = etCountByToken.length

    // may be computing this multiple times
    val batchElogtheta: DenseMatrix[Double] = OnlineLDA.dirichletExpectation(gamma)
    val batchExpElogtheta: DenseMatrix[Double] = exp(batchElogtheta)

    // E[log p(sessions | theta, id)]
    var score = ExpectationLogProbSessionsGivenThetaAndId(etCountByToken, batchElogtheta)

    // E[log p(theta | alpha) - log q(theta | gamma)]
    score += ExpectationLogThetaGivenAlphaMinusLogThetaGivenGamma(gamma,batchElogtheta)

    // compensate for sub-sampling by normalizing by batch size fraction
    score = score * (this.D / batchSessN.toDouble)

    // E[log p(beta | eta) - log q (beta | lambda)]
    score += ExpectationLogBetaGivenEtaMinusLogBetaGivenLambda

    score
  }


  /**
   * E[log p(sessions | theta, id)]
   *
   * @param etCountByToken, Seq of sessions event counts, where event ids are converted to index in dictionary
   * @param batchElogtheta
   * @return
   */
  private def ExpectationLogProbSessionsGivenThetaAndId(etCountByToken: Seq[Map[Int, Int]],
                                                        batchElogtheta: DenseMatrix[Double]): Double = {
    etCountByToken.zipWithIndex.map{ case (etCount, sessionIdx) =>

      val scoreAugForSession: Double = etCount.map{ case (etIdx, count) =>
        // TODO: check this
        val temp: DenseVector[Double] = batchElogtheta(sessionIdx, ::).t + this.ELogBeta(::, etIdx)
        val tmax: Double = max(temp)
        val phiNorm: Double = log( sum( temp.map{ x => exp( x - tmax )} ) ) + tmax
        count * phiNorm

      }.sum

      scoreAugForSession
    }.sum
  }

  /**
   * E[log p(theta | alpha) - log q(theta | gamma)]
   *
   * @param gamma
   * @param batchElogtheta
   * @return
   */
  private def ExpectationLogThetaGivenAlphaMinusLogThetaGivenGamma(gamma: DenseMatrix[Double],
                                                                   batchElogtheta: DenseMatrix[Double]): Double = {
    val part0: Double = sum(
      gamma.pairs.map{ case (idxPair,v) => batchElogtheta(idxPair) * (this.alpha - v) }
    )
    val part1: Double = sum(
      gamma.map{ x => lgamma(x) - lgamma(this.alpha) }
    )
    val part2: Double = sum(
      sum(gamma, Axis._1).map{ x => lgamma(this.alpha * this.K) - lgamma(x) }
    )

    part0 + part1 + part2
  }

  /**
   * E[log p(beta | eta) - log q (beta | lambda)]
   *
   * @return
   */
  private def ExpectationLogBetaGivenEtaMinusLogBetaGivenLambda: Double = {
    val part0: Double = sum(
      this.ELogBeta.pairs.map { case (idxPair, v) => v * (this.eta - this.lambda(idxPair)) }
    )
    val part1: Double = sum(
      this.lambda.map{ x => lgamma(x) - lgamma(this.eta) }
    )
    val part2: Double = sum(
      sum(this.lambda, Axis._1).map{ x => lgamma(this.eta * this.W) - lgamma(x) }
    )
    part0 + part1 + part2
  }

  def helpOutPerplexityEstimate(sessionIdxCount: Seq[Map[Int, Int]], variationalLowerBound: Double): Double = {
    val n: Long = sessionIdxCount.length
    val totalEvents: Long = sessionIdxCount.map{ _.values.sum }.sum
    val perWordBound: Double = ( variationalLowerBound * n ) / (D * totalEvents.toDouble)
    exp(-perWordBound)
  }

  def toJson: JValue = ???
  // as a JValue

  def toJsonString: String = compact(this.toJson)
  // as a JSONstring

  def saveModel = ???
  // save model to file-system

}













