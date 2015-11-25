package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{Json, JsValue}

class Benford extends Bootstrap {
  /*def calcBenfordCIs(sc: SparkContext, data: DataByLevel, numSamples: Int = 25000): RDD[StatsCIByLevel] = {
    val dataStatsRDD = calcDataStats(data.dataByLevelsRDD)
    val aliasMap = data.freqByLevel.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(BenfordProbabilitiesD1D2)) }.toMap
    val sampleSize = data.freqByLevel.filter { case FreqByLevel(idxLevel, freq) => idxLevel == 0 }(0).freq.count

    val bootTableRDD = generateBootstrapTable(sc, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, data.dataByLevelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    val statsCIRDD = calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99))
    statsCIRDD
  }*/
  def calcBenfordCIs(basicBoot: BasicBoot, dataStatsRDD: RDD[((Long, Int), StatsDigits)], data: DataByLevel, groupId: Int): RDD[StatsCIByLevel] = {
    val bootRDD = generateBootstrapOutcomes(basicBoot.bootTableRDD, data, basicBoot.aliasMapBenf, groupId)
    val momentsRDD = calcMomentsSamples(bootRDD, groupId)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    val statsCIRDD = calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99))
    statsCIRDD
  }

  def getExactBenfordParams: JsValue = {
    Json.toJson(BenfordStatsDigits)
  }

  def getExactBenfordProbs: JsValue = {
    Json.toJson(Frequencies(1000, BenfordProbabilitiesD1D2, BenfordProbabilitiesD1, BenfordProbabilitiesD2))
  }

}

object Benford {
  def apply() = new Benford()
}