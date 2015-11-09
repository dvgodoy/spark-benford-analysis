package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util.StatsCIByLevel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Benford extends Bootstrap {
  def calcBenfordCIs(sc: SparkContext, dataRDD: RDD[((Long, Double, Int), Array[String])], numSamples: Int = 25000): (Array[((String, Int), Long)], Map[Long,Array[Long]], RDD[StatsCIByLevel]) = {
    val (uniqLevels, pointers, levelsRDD) = findLevels(dataRDD)
    val dataStatsRDD = calcDataStats(levelsRDD)
    val (tmp, freqRDD) = calcFrequenciesLevels(levelsRDD)
    val aliasMap = freqRDD.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(BenfordProbabilitiesD1D2)) }.collect().toMap

    val freq = freqRDD.filter{ case FreqByLevel(idxLevel, frequency) => idxLevel == 0}.collect()
    val sampleSize = freq(0).freq.count

    val bootTableRDD = generateBootstrapTable(sc, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, levelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    (uniqLevels, pointers, calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99)))
  }
}

object Benford {
  def apply() = new Benford()
}