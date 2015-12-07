package com.dvgodoy.spark.benford.distributions

import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util._
import play.api.libs.json.{Json, JsValue}
import org.scalactic._
import Accumulation._

class Benford extends Bootstrap {

  def calcBenfordCIs(basicBoot: BasicBootMsg, dataStatsRDD: DataStatsMsg, data: DataByLevelMsg, groupId: Int): StatsCIByLevelMsg = {
    try {
      withGood(basicBoot, dataStatsRDD, data) { (basicBoot, dataStatsRDD, data) =>
        val bootRDD = generateBootstrapOutcomes(basicBoot.bootTableRDD, data, basicBoot.aliasMapBenf, groupId)
        val momentsRDD = calcMomentsSamples(bootRDD, groupId)
        val statsRDD = calcStatsSamples(momentsRDD)
        val groupStatsRDD = groupStats(statsRDD)
        val statsCIRDD = calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99))
        statsCIRDD
      }
    } catch {
      case ex: Exception => Bad(One(s"Error: ${ex.getMessage}"))
    }
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