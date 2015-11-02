package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.distributions.Bootstrap._
import org.apache.spark.SparkContext

object Benford {
  val probMultinomialD1 = Array.concat(Array(0.0),probabilitiesD1)
  val probMultinomialD2 = Array.concat(Array(0.0),probabilitiesD2)
  val probMultinomialD1D2 = Array.concat(Array.fill(10)(0.0),probabilitiesD1D2)
}

class Benford {
  def statsBenford(sc: SparkContext, sampleSize: Int, numSamples: Int, FSD: Boolean, SSD: Boolean): BootStats = {
    assert (FSD | SSD, "A significant digit must be selected")

    val prob = if (FSD) {
        if (SSD) Benford.probMultinomialD1D2 else Benford.probMultinomialD1
      } else {
      Benford.probMultinomialD2
    }

    val bootRDD = generateBootstrapSamples(sc, prob, sampleSize, numSamples)
    val momentsRDD = calcMomentsSamples(bootRDD)
    //groupStats(calcStatsSamples(momentsRDD))
  }
}