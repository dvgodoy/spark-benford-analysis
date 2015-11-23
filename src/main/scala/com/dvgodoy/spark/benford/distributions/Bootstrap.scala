package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.RandBasis
import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json._
import scala.collection.mutable
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._

class Bootstrap extends Serializable {
  private def rollToss(nOutcomes: Int, rand: RandBasis): (Int, Double) = {
    (rand.randInt(nOutcomes).get(),rand.uniform.get())
  }

  private def findOutcome(aliasTable: AliasTable, rollToss: (Int, Double)): Int = if (rollToss._2 < aliasTable.modProb(rollToss._1)) rollToss._1 + 10 else aliasTable.aliases(rollToss._1) + 10

  private def calcFrequencies(digitsCounts: List[(Int, Int)]): Frequencies = {
    val digitsTotal = digitsCounts.map { case (d1d2, count) => count }.sum
    val countsD1D2 = digitsCounts ::: (10 to 99).toSet.diff(digitsCounts.map(_._1).toSet).toList.map(n => (n, 0))
    val frequenciesD1D2 = countsD1D2.map { case (d1d2, count) => (d1d2, count/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)
    val frequenciesD1 = countsD1D2.map { case (d1d2, count) => (d1d2/10, count) }
      .groupBy { case (d1, count) => d1 }
      .map { case (d1, arrayCounts) => (d1, arrayCounts.map { case (d1, count) => count }.sum/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)
    val frequenciesD2 = countsD1D2.map { case (d1d2, count) => (d1d2%10, count) }
      .groupBy { case (d2, count) => d2 }
      .map { case (d2, arrayCounts) => (d2, arrayCounts.map { case (d2, count) => count }.sum/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)

    Frequencies(digitsTotal, frequenciesD1D2, frequenciesD1, frequenciesD2)
  }

  private def calcOverlaps(bootStatsCIRDD: RDD[StatsCIByLevel], benfordStatsCIRDD: RDD[StatsCIByLevel]): RDD[OverlapsByLevel] = {
    val overlapRDD = bootStatsCIRDD.map{ case StatsCIByLevel(idxLevel, depth, stats) => ((idxLevel, depth), stats) }
      .join(benfordStatsCIRDD.map{ case StatsCIByLevel(idxLevel, depth, stats) => ((idxLevel, depth), stats) })
      .map{ case ((idxLevel, depth), (boot, benford)) => OverlapsByLevel(idxLevel, depth, boot.overlaps(benford), boot.contains(BenfordStatsDigits)) }
    overlapRDD
  }

  private def calcResultsByLevel(overlapRDD: RDD[OverlapsByLevel]): RDD[ResultsByLevel] = {
    overlapRDD.map { case obl => obl.calcResults }
  }

  protected case class AliasTable(modProb: DenseVector[Double], aliases: DenseVector[Int], nOutcomes: Int)
  protected def buildAliasTable(prob: Array[Double]): AliasTable = {
    val nOutcomes = prob.length
    assert(nOutcomes == 90)
    val aliases = DenseVector.zeros[Int](nOutcomes)
    val sum = breeze.linalg.sum(prob)

    val modProb = DenseVector(prob.map { param => param / sum * nOutcomes })
    val (iSmaller, iLarger) = (0 until nOutcomes).partition(modProb(_) < 1d)
    val smaller = mutable.Stack(iSmaller:_*)
    val larger = mutable.Stack(iLarger:_*)

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.pop()
      val large = larger.pop()
      aliases(small) = large
      modProb(large) -= (1d - modProb(small))
      if (modProb(large) < 1)
        smaller.push(large)
      else
        larger.push(large)
    }

    AliasTable(modProb, aliases, nOutcomes)
  }

  protected def generateBootstrapTable(sc: SparkContext, sampleSize: Int, numSamples: Int): RDD[(Long, (Int, (Int, Double)))] = {
    val nOutcomes = 90
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      val rand = new RandBasis(new MersenneTwister(idx + 42))
      iter.flatMap(sample => Array.fill(sampleSize)(rollToss(nOutcomes, rand)).zipWithIndex
                            .map { case ((roll, toss), idx) => (idx.toLong, (sample, (roll, toss))) })
    }
  }

  protected case class OutcomeByLevel(idx: Long, idxLevel: Long, depth: Int, sample: Int, n: Int)
  protected def generateBootstrapOutcomes(bootstrapTableRDD: RDD[(Long, (Int, (Int, Double)))], levelsRDD: RDD[Level], aliasMap: Map[Long,AliasTable]): RDD[OutcomeByLevel] = {
    val findIdxLevelRDD = levelsRDD.map { case Level(idxLevel, depth, idx, value, d1d2) => (idx, (idxLevel, depth))}
    bootstrapTableRDD.join(findIdxLevelRDD).map { case (idx, ((sample, (roll, toss)), (idxLevel, depth))) => OutcomeByLevel(idx, idxLevel, depth, sample, findOutcome(aliasMap(idxLevel), (roll, toss))) }
  }

  protected case class MomentsByLevel(idxLevel: Long, depth: Int, sample: Int, moments: MomentsDigits)
  protected def calcMomentsSamples(bootRDD: RDD[OutcomeByLevel]): RDD[MomentsByLevel] = {
    bootRDD.map { case OutcomeByLevel(idx, idxLevel, depth, sample, n) => ((idxLevel, depth, sample, n), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample, n), count) => ((idxLevel, depth, sample), calcMoments(n, count))}
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample), moments) => MomentsByLevel(idxLevel, depth, sample, moments) }
  }

  protected case class StatsByLevel(idxLevel: Long, depth: Int, sample: Int, stats: StatsDigits)
  protected def calcStatsSamples(momentsRDD: RDD[MomentsByLevel]): RDD[StatsByLevel] = {
    momentsRDD.map { case MomentsByLevel(idxLevel, depth, sample, moments) => StatsByLevel(idxLevel, depth, sample, calcStatsDigits(moments)) }
  }

  protected def groupStats(statsRDD: RDD[StatsByLevel]): RDD[((Long, Int), StatsDigits)] = {
    statsRDD.map { case StatsByLevel(idxLevel, depth, sample, stats) => ((idxLevel, depth), stats) }.reduceByKey(_+_)
  }

  protected def calcStatsCIs(dataStatsRDD: RDD[((Long, Int), StatsDigits)], groupStatsRDD: RDD[((Long, Int), StatsDigits)], conf: Array[Double]): RDD[StatsCIByLevel] = {
    groupStatsRDD.join(dataStatsRDD)
      .map { case ((idxLevel, depth), (groupStats, dataStats)) => StatsCIByLevel(idxLevel, depth, groupStats.calcBcaCI(conf, dataStats)) }
  }

  protected def calcDataStats(levelsRDD: RDD[Level]): RDD[((Long, Int), StatsDigits)] = {
    val originalRDD = levelsRDD.map { case Level(idxLevel, depth, idx, value, d1d2) => OutcomeByLevel(idx, idxLevel, depth, 1, d1d2) }
    val momentsOriginalRDD = calcMomentsSamples(originalRDD)
    val statsOriginalRDD = calcStatsSamples(momentsOriginalRDD)
    groupStats(statsOriginalRDD)
  }

  protected def findLevels(dataLevelRDD: RDD[((Long, Double, Int), Array[String])])(implicit jobId: JobId): DataByLevel = {
    val sc = dataLevelRDD.context
    sc.setJobDescription(jobId.id + ".findLevels")

    val concatRDD = dataLevelRDD
      .map { case (value, levels) => (value, levels
                                  .zipWithIndex
                                  .map { case (nextLevels, idx) => (levels.slice(0, idx + 1).foldLeft("L")(_ + "." + _), idx) } ) }
    val levelsRDD = concatRDD.flatMap { case (value, levels) => levels.map { case (name, depth) => ((name, depth), value) } }

    val uniqLevelsRDD = levelsRDD.map { case (classif, value) => classif }.distinct().sortBy(identity).zipWithIndex()
    val uniqLevels = uniqLevelsRDD.collect()
    val levels = uniqLevels.map{ case ((classif, depth), idx) => (idx -> (classif, depth))}.toMap

    val hierarchies = concatRDD.map { case (value, levels) => levels.map(_._1).toList }.distinct().collect()
    val idxHierarchies = hierarchies
      .map(levels => levels.flatMap(hierarchy => uniqLevels
                                  .filter{case ((level, depth), idxLevel) => level == hierarchy}.map{case ((level, depth), idxLevel) => idxLevel }))

    val pointers = idxHierarchies.flatMap(levels => levels.zipWithIndex.map{case (top, idx) => (top, if (idx < (levels.length - 1)) levels(idx + 1) else -1)})
      .distinct.groupBy(_._1).map{case (top, below) => (top, below.map(_._2))}

    val dataByLevelsRDD = uniqLevelsRDD.join(levelsRDD).map { case ((name, depth), (idxLevel, (idx, value, d1d2))) => Level(idxLevel, depth, idx, value, d1d2) }

    val freqByLevel = calcFrequenciesLevels(dataByLevelsRDD)
    sc.setJobDescription("")
    DataByLevel(levels, pointers, freqByLevel, dataByLevelsRDD)
  }

  protected def calcFrequenciesLevels(levelsRDD: RDD[Level])(implicit jobId: JobId): Array[FreqByLevel] = {
    val sc = levelsRDD.sparkContext
    sc.setJobDescription(jobId.id + ".calcFrequenciesLevels")
    val levelsCountRDD = levelsRDD
      .map { case Level(idxLevel, depth, idx, value, d1d2) => ((idxLevel, d1d2), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, d1d2), count) => (idxLevel, (d1d2, count)) }

    val freqLevels = levelsCountRDD.groupByKey().map { case (idxLevel, counts) => FreqByLevel(idxLevel, calcFrequencies(counts.toList)) }.collect()
    sc.setJobDescription("")
    freqLevels
  }

  def loadData(sc: SparkContext, filePath: String)(implicit jobId: JobId): DataByLevel = {
    // TO DO - reorder csv
    // TO DO - find levels in decreasing order of distinct values in each one of them
    sc.setJobDescription(jobId.id + ".loadData")
    val dataLevelRDD = sc.textFile(filePath)
      .map(line => line.split(",")
        .map(_.trim.replace("\"","")))
      .map(line => (parseDouble(line(0)), line.slice(1,line.length)))
      .filter { case (value, levels) => value match { case Some(v) if v != 0.0 => true; case Some(v) if v == 0.0 => false; case None => false } }
      .map { case (value, levels) => (value.getOrElse(0.0), levels) }
      .zipWithIndex()
      .map { case ((value, levels), idx) => ((idx, value, findD1D2(value)), levels) }

    val dataByLevel = findLevels(dataLevelRDD)
    sc.setJobDescription("")
    dataByLevel
  }

  def calcSampleCIs(sc: SparkContext, data: DataByLevel, numSamples: Int = 25000): RDD[StatsCIByLevel] = {
    val dataStatsRDD = calcDataStats(data.dataByLevelsRDD)
    val aliasMap = data.freqByLevel.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(freq.freqD1D2)) }.toMap
    val sampleSize = data.freqByLevel.filter { case FreqByLevel(idxLevel, freq) => idxLevel == 0 }(0).freq.count

    val bootTableRDD = generateBootstrapTable(sc, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, data.dataByLevelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    val statsCIRDD = calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99))
    statsCIRDD
  }

  def calcResults(bootSampleRDD: RDD[StatsCIByLevel], bootBenfordRDD: RDD[StatsCIByLevel]): RDD[ResultsByLevel] = {
    val overlapRDD = calcOverlaps(bootSampleRDD, bootBenfordRDD)
    calcResultsByLevel(overlapRDD)
  }

  def getCIsByGroupId(statsCIRDD: RDD[StatsCIByLevel], groupId: Int)(implicit jobId: JobId): JsValue = {
    val sc = statsCIRDD.sparkContext
    sc.setJobDescription(jobId.id + ".getCIsByGroupId")
    val CIsRDD = statsCIRDD.filter { case StatsCIByLevel(idxLevel, depth, stats) => idxLevel == groupId }
    val CIs = CIsRDD.collect()
    sc.setJobDescription("")
    val json = Json.toJson(CIs)
    pruneCIs(json)
  }

  def getCIsByLevel(statsCIRDD: RDD[StatsCIByLevel], level: Int)(implicit jobId: JobId): JsValue = {
    val sc = statsCIRDD.sparkContext
    sc.setJobDescription(jobId.id + ".getCIsByLevel")
    val CIsRDD = statsCIRDD.filter { case StatsCIByLevel(idxLevel, depth, stats) => depth == level }
    val CIs = CIsRDD.collect()
    sc.setJobDescription("")
    val json = Json.toJson(CIs)
    pruneCIs(json)
  }

  def getResultsByGroupId(resultsRDD: RDD[ResultsByLevel], groupId: Int)(implicit jobId: JobId): JsValue = {
    val sc = resultsRDD.sparkContext
    sc.setJobDescription(jobId.id + ".getResultsByGroupId")
    val resRDD = resultsRDD.filter { case ResultsByLevel(idxLevel, depth, results) => idxLevel == groupId }
    val res = resRDD.collect()
    sc.setJobDescription("")
    val json = Json.toJson(res)
    pruneResults(json)
  }

  def getResultsByLevel(resultsRDD: RDD[ResultsByLevel], level: Int)(implicit jobId: JobId): JsValue = {
    val sc = resultsRDD.sparkContext
    sc.setJobDescription(jobId.id + ".getResultsByLevel")
    val resRDD = resultsRDD.filter { case ResultsByLevel(idxLevel, depth, results) => depth == level }
    val res = resRDD.collect()
    sc.setJobDescription("")
    val json = Json.toJson(res)
    pruneResults(json)
  }

  def getFrequenciesByGroupId(data: DataByLevel, groupId: Int): JsValue = {
    val frequencies = data.freqByLevel.filter { case FreqByLevel(idxLevel, freq) => idxLevel == groupId }
                      .map{ case FreqByLevel(idxLevel, freq) => freq }
    Json.toJson(frequencies)
  }

  def getFrequenciesByLevel(data: DataByLevel, level: Int): JsValue = {
    val groupIds = data.levels.filter{case (idxLevel, (name, depth)) => depth == level}.keySet
    val frequencies = data.freqByLevel.filter { case FreqByLevel(idxLevel, freq) => groupIds.contains(idxLevel) }
      .map{ case FreqByLevel(idxLevel, freq) => freq }
    Json.toJson(frequencies)
  }

  def getGroups(data: DataByLevel): JsValue = {
    val groups = (data.levels.toList.sortBy(_._1) zip data.hierarchy.toList.sortBy(_._1))
                .map{case ((idxLevel,(name,depth)),(idx,children)) => Group(idxLevel, depth, name.substring(2), children.sorted)}
    Json.toJson(groups)
  }

  def getExactBenfordParams: JsValue = {
    Json.toJson(BenfordStatsDigits)
  }

  def getExactBenfordProbs: JsValue = {
    Json.toJson(Frequencies(1000, BenfordProbabilitiesD1D2, BenfordProbabilitiesD1, BenfordProbabilitiesD2))
  }

  def getSuspiciousGroups(jsonResults: JsValue): JsValue = {
    val statTransf = ((__ \ 'id).json.pick and (__ \ 'results \ 'statsDiag).json.pick) reduce
    val statsSusp = jsonResults.as[List[JsValue]]
        .map(_.transform(statTransf).get)
        .map(r => (r(0).as[Int], r(1).as[Int]))
        .filter{case (idxLevel, compliant) => compliant == -1}
        .sorted
        .map{case (idxLevel, compliant) => JsNumber(idxLevel)}.toSeq

    val regsFilter = (__ \ 'results \ 'n).json.pick
    val regsTransf = ((__ \ 'id).json.pick and (__ \ 'results \ 'regsDiag).json.pick) reduce
    val regsSusp = jsonResults.as[List[JsValue]]
        .filter(_.transform(regsFilter).get.as[Int] >= 1000)
        .map(_.transform(regsTransf).get)
        .map(r => (r(0).as[Int], r(1).as[Int]))
        .filter{case (idxLevel, compliant) => compliant == -1}
        .sorted
        .map{case (idxLevel, compliant) => JsNumber(idxLevel)}.toSeq

    Json.obj(
      "stats" -> JsArray(statsSusp),
      "regs" -> JsArray(regsSusp)
    )
  }
}

object Bootstrap {
  def apply() = new Bootstrap
}