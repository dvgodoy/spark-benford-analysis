package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions._
import BigInt._

/**
 * Created by dvgodoy on 17/10/15.
 */

abstract class distBenford {
  val prob: DenseVector[Double] = DenseVector()
  def mult = new Multinomial(prob)
  def sample(n: Int): IndexedSeq[BigInt] = mult.sample(n).map(BigInt(_))
}

private object distBenfordD1 extends distBenford {
  override val prob = DenseVector.vertcat(DenseVector.zeros[Double](1), DenseVector.range(1, 10).map(x => math.log10(1.0 + 1.0 / x)))
}

private object distBenfordD2 extends distBenford {
  override val prob = DenseVector((10 to 19).map(x => (List.range(x, 100, 10).map(x => math.log10(1.0 + 1.0 / x))).sum).toArray)
}

private object distBenfordD1D2 extends distBenford {
  override val prob = DenseVector.vertcat(DenseVector.zeros[Double](10), DenseVector.range(10, 100).map(x => math.log10(1.0 + 1.0 / x)))
}

class Benford(FSD: Boolean, SSD: Boolean) extends distBenford {
  override def mult = (if (FSD && SSD) distBenfordD1D2 else if (FSD && !SSD) distBenfordD1 else distBenfordD2).mult
}

object Benford {
  def apply(): Benford = new Benford(true, false)
  def apply(FSD: Boolean) = new Benford(FSD, true)
}