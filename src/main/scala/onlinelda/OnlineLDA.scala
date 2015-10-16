package onlinelda

import breeze.linalg.{DenseVector, DenseMatrix, sum, Axis}
import breeze.numerics.{digamma,exp}
import breeze.stats.distributions.Gamma
import models.Session




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

  def parseSession_list(sessions: List[Session]) = ???
  /*
  def parse_sessions_list(sessions, vocab):
      
      D = len(sessions)
      
      wordids = list()
      wordcts = list()
      for D in range(0, D):
          events = sessions[D]
          ddict = dict()
          for e in events:
              if (e in vocab):
                  eventtoken = vocab[e]
                  if (not eventtoken in ddict):
                      ddict[eventtoken] = 0
                  ddict[eventtoken] += 1
          wordids.append(ddict.keys())
          wordcts.append(ddict.values())

      return((wordids, wordcts))
   */
}

class OnlineLDA(eventSet: Set[String], val K: Int, val D: BigInt,
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

  val events: Map[String, Int] = eventSet.map {
    e => e.toLowerCase
  }.zipWithIndex.toMap

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

}













