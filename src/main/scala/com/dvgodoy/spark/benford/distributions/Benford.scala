package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util.{FreqByLevel, StatsCIByLevel, DataByLevel}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Benford extends Bootstrap {
  def calcBenfordCIs(sc: SparkContext, data: DataByLevel, numSamples: Int = 25000): RDD[StatsCIByLevel] = {
    val dataStatsRDD = calcDataStats(data.dataByLevelsRDD)
    val aliasMap = data.freqByLevel.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(BenfordProbabilitiesD1D2)) }.toMap
    val sampleSize = data.freqByLevel.filter { case FreqByLevel(idxLevel, freq) => idxLevel == 0 }(0).freq.count

    val bootTableRDD = generateBootstrapTable(sc, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, data.dataByLevelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99))
  }
}

object Benford {
  def apply() = new Benford()
}