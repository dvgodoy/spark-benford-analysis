package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.RandBasis
import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

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
    assert(bootStatsCIRDD.count() == benfordStatsCIRDD.count())
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

  protected case class Level(idxLevel: Long, depth: Int, idx: Long, value: Double, d1d2: Int)
  protected def findLevels(dataLevelRDD: RDD[((Long, Double, Int), Array[String])]): (Array[((String, Int), Long)], Map[Long,Array[Long]], RDD[Level]) = {
    val concatRDD = dataLevelRDD
      .map { case (value, levels) => (value, levels
                                  .zipWithIndex
                                  .map { case (nextLevels, idx) => (levels.slice(0, idx + 1).foldLeft("L")(_ + "." + _), idx) } ) }
    val levelsRDD = concatRDD.flatMap { case (value, levels) => levels.map { case (name, depth) => ((name, depth), value) } }

    val uniqLevelsRDD = levelsRDD.map { case (classif, value) => classif }.distinct().sortBy(identity).zipWithIndex()
    val uniqLevels = uniqLevelsRDD.collect()

    val hierarchies = concatRDD.map { case (value, levels) => levels.map(_._1).toList }.distinct().collect()
    val idxHierarchies = hierarchies
      .map(levels => levels.flatMap(hierarchy => uniqLevels
                                  .filter{case ((level, depth), idxLevel) => level == hierarchy}.map{case ((level, depth), idxLevel) => idxLevel }))
    val pointers = idxHierarchies.flatMap(levels => levels.zipWithIndex.map{case (top, idx) => (top, if (idx < (levels.length - 1)) levels(idx + 1) else -1)})
      .distinct.groupBy(_._1).map{case (top, below) => (top, below.map(_._2))}

    (uniqLevels, pointers,
      uniqLevelsRDD.join(levelsRDD).map { case ((name, depth), (idxLevel, (idx, value, d1d2))) => Level(idxLevel, depth, idx, value, d1d2) })
  }

  protected case class FreqByLevel(idxLevel: Long, freq: Frequencies)
  protected def calcFrequenciesLevels(levelsRDD: RDD[Level]): (Map[Long,AliasTable], RDD[FreqByLevel]) = {
    val levelsCountRDD = levelsRDD
      .map { case Level(idxLevel, depth, idx, value, d1d2) => ((idxLevel, d1d2), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, d1d2), count) => (idxLevel, (d1d2, count)) }

    val freqLevelsRDD = levelsCountRDD.groupByKey().map { case (idxLevel, counts) => FreqByLevel(idxLevel, calcFrequencies(counts.toList)) }
    val aliasMap = freqLevelsRDD.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(freq.freqD1D2)) }.collect().toMap

    (aliasMap, freqLevelsRDD)
  }

  def loadData(sc: SparkContext, filePath: String): RDD[((Long, Double, Int), Array[String])] = {
    // TO DO - reorder csv
    val dataLevelRDD = sc.textFile(filePath)
      .map(line => line.split(",")
        .map(_.trim.replace("\"","")))
      .map(line => (parseDouble(line(0)), line.slice(1,line.length)))
      .filter { case (value, levels) => value match { case Some(v) if v != 0.0 => true; case Some(v) if v == 0.0 => false; case None => false } }
      .map { case (value, levels) => (value.getOrElse(0.0), levels) }
      .zipWithIndex()
      .map { case ((value, levels), idx) => ((idx, value, findD1D2(value)), levels) }

    dataLevelRDD
  }

  def calcSampleCIs(sc: SparkContext, dataRDD: RDD[((Long, Double, Int), Array[String])], numSamples: Int = 25000): (Array[((String, Int), Long)], Map[Long,Array[Long]], RDD[StatsCIByLevel]) = {
    val (uniqLevels, pointers, levelsRDD) = findLevels(dataRDD)
    val dataStatsRDD = calcDataStats(levelsRDD)
    val (aliasMap, freqRDD) = calcFrequenciesLevels(levelsRDD)
    val freq = freqRDD.filter{ case FreqByLevel(idxLevel, frequency) => idxLevel == 0}.collect()
    val sampleSize = freq(0).freq.count

    val bootTableRDD = generateBootstrapTable(sc, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, levelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    (uniqLevels, pointers, calcStatsCIs(dataStatsRDD, groupStatsRDD, Array(0.975, 0.99)))
  }

  def showCIsByGroupId(statsCIRDD: RDD[StatsCIByLevel], groupId: Int): String = {
    val CIsRDD = statsCIRDD.filter { case StatsCIByLevel(idxLevel, depth, stats) => idxLevel == groupId }
    val CIs = CIsRDD.collect()
    compact(render(CIs(0).toJson("groupId_" + groupId.toString)))
  }

  def showCIsByLevel(statsCIRDD: RDD[StatsCIByLevel], level: Int): Array[String] = {
    val CIsRDD = statsCIRDD.filter { case StatsCIByLevel(idxLevel, depth, stats) => depth == level }
    val CIs = CIsRDD.collect()
    for (ci <- CIs) yield compact(render(ci.toJson("groupId_" + ci.idxLevel.toString)))
  }

  def calcResults(bootSampleRDD: RDD[StatsCIByLevel], bootBenfordRDD: RDD[StatsCIByLevel]): RDD[ResultsByLevel] = {
    val overlapRDD = calcOverlaps(bootSampleRDD, bootBenfordRDD)
    calcResultsByLevel(overlapRDD)
  }

  def showResultsByGroupId(resultsRDD: RDD[ResultsByLevel], groupId: Int): String = {
    val resRDD = resultsRDD.filter { case ResultsByLevel(idxLevel, depth, results) => idxLevel == groupId }
    val res = resRDD.collect()
    compact(render(res(0).toJson("groupId_" + groupId.toString)))
  }

  def showResultsByLevel(resultsRDD: RDD[ResultsByLevel], level: Int): Array[String] = {
    val resRDD = resultsRDD.filter { case ResultsByLevel(idxLevel, depth, results) => depth == level }
    val res = resRDD.collect()
    for (r <- res) yield compact(render(r.toJson("groupId_" + r.idxLevel.toString)))
  }
}

object Bootstrap {
  def apply() = new Bootstrap
}