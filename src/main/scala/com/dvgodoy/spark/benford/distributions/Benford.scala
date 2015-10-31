package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.distributions.Bootstrap._
import com.dvgodoy.spark.benford.util.BootStats
import org.apache.spark.SparkContext

object Benford {
  val probMultinomialD1 = Array.concat(Array(0.0),probabilitiesD1)
  val probMultinomialD2 = Array.concat(Array(0.0),probabilitiesD2)
  val probMultinomialD1D2 = Array.concat(Array.fill(10)(0.0),probabilitiesD1D2)
}

class Benford {
  def statsBenford(sc: SparkContext, sampleSize: Int, numSamples: Int): BootStats = {
    val bootRDD = generateBootstrapSamples(sc, Benford.probMultinomialD1D2, sampleSize, numSamples)
    val momentsRDD = calcMomentsSamples(bootRDD)
    groupStats(calcStatsSamples(momentsRDD))
  }
}