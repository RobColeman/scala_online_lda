package onlinelda

import breeze.linalg._
import breeze.numerics.digamma

/**
 * Created by rcoleman on 10/15/15.
 */
object OnlineLDA {

  def psi(x: DenseMatrix[Double]): Double = ??? // digamma
  def gammaln = ???

  /**
   *
   * @param alpha - a vector alpha
   */
  def dirichletExpectation(alpha: DenseMatrix[Double]): Double = {
    alpha.size match {
      case 1 => psi(alpha) - psi( DenseMatrix( sum(alpha) ) )
      case _ => psi(alpha) - psi(sum(alpha, Axis._0))

    }

  }

  def parseSession_list(sessions: List[Sessions]) = ???
}

class OnlineLDA {

}
