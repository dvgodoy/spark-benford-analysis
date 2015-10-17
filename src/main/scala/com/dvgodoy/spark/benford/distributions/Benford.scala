package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions._

/**
 * Created by dvgodoy on 17/10/15.
 */

object probabilities {
  val distD1D2: Multinomial[DenseVector[Double],Int] = new Multinomial(DenseVector.vertcat(DenseVector.zeros[Double](10), DenseVector.range(10, 100).map(x => math.log10(1.0 + 1.0 / x))))
  val distD1: Multinomial[DenseVector[Double],Int] = new Multinomial(DenseVector.vertcat(DenseVector.zeros[Double](1), DenseVector.range(1, 10).map(x => math.log10(1.0 + 1.0 / x))))
  val distD2: Multinomial[DenseVector[Double],Int] = new Multinomial(DenseVector((10 to 19).map(x => (List.range(x, 100, 10).map(x => math.log10(1.0 + 1.0 / x))).sum).toArray))
}

class Benford(FSD: Boolean, SSD: Boolean) {
  def sample(n: Int): IndexedSeq[BigInt] = (if (FSD & SSD) probabilities.distD1D2 else if (FSD & !SSD) probabilities.distD1 else probabilities.distD2).sample(n).map(BigInt(_))
}

object Benford {
  def apply(): Benford = new Benford(true, false)
  def apply(FSD: Boolean) = new Benford(FSD, true)
}