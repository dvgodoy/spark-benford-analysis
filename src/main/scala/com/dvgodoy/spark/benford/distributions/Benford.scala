package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.distributions.Bootstrap._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Benford {
  def statsBenford(sc: SparkContext, dataRDD: RDD[((Long, Double, Int), Array[String])], numSamples: Int, conf: Array[Double]): (Array[((String, Int), Long)], RDD[StatsCIByLevel]) = {
    val sampleSize = dataRDD.count().toInt

    val (uniqLevels, levelsRDD) = findLevels(dataRDD)
    val dataStatsRDD = calcDataStats(levelsRDD)
    val (aliasMap, freqRDD) = calcFrequenciesLevels(levelsRDD)

    val bootTableRDD = generateBootstrapTable(sc, BenfordProbabilitiesD1D2, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, levelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    (uniqLevels, calcStatsCIs(dataStatsRDD, groupStatsRDD, conf))

  }
}