package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.RandBasis
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object Bootstrap {

  private def rollToss(nOutcomes: Int, rand: RandBasis): (Int, Double) = {
    (rand.randInt(nOutcomes).get(),rand.uniform.get())
  }

  private case class AliasTable(modProb: DenseVector[Double], aliases: DenseVector[Int], nOutcomes: Int)
  private def buildAliasTable(prob: Array[Double]): AliasTable = {
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

  private def findOutcome(aliasTable: AliasTable, rollToss: (Int, Double)): Int = if (rollToss._2 < aliasTable.modProb(rollToss._1)) rollToss._1 + 10 else aliasTable.aliases(rollToss._1) + 10

  def generateBootstrapTable(sc: SparkContext, prob: Array[Double], sampleSize: Int, numSamples: Int): RDD[(Long, (Int, (Int, Double)))] = {
    val nOutcomes = prob.length
    assert(nOutcomes == 90)
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      val rand = new RandBasis(new MersenneTwister(idx + 42))
      iter.flatMap(sample => Array.fill(sampleSize)(rollToss(nOutcomes, rand)).zipWithIndex
                            .map { case ((roll, toss), idx) => (idx.toLong, (sample, (roll, toss))) })
    }
  }

  case class OutcomeByLevel(idx: Long, idxLevel: Long, depth: Int, sample: Int, n: Int)
  def generateBootstrapOutcomes(bootstrapTableRDD: RDD[(Long, (Int, (Int, Double)))], levelsRDD: RDD[Level], aliasMap: Map[Long,AliasTable]): RDD[OutcomeByLevel] = {
    val findIdxLevelRDD = levelsRDD.map { case Level(idxLevel, depth, idx, value, d1d2) => (idx, (idxLevel, depth))}
    bootstrapTableRDD.join(findIdxLevelRDD).map { case (idx, ((sample, (roll, toss)), (idxLevel, depth))) => OutcomeByLevel(idx, idxLevel, depth, sample, findOutcome(aliasMap(idxLevel), (roll, toss))) }
  }

  case class MomentsByLevel(idxLevel: Long, depth: Int, sample: Int, moments: MomentsDigits)
  def calcMomentsSamples(bootRDD: RDD[OutcomeByLevel]): RDD[MomentsByLevel] = {
    bootRDD.map { case OutcomeByLevel(idx, idxLevel, depth, sample, n) => ((idxLevel, depth, sample, n), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample, n), count) => ((idxLevel, depth, sample), calcMoments(n, count))}
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample), moments) => MomentsByLevel(idxLevel, depth, sample, moments) }
  }

  case class StatsByLevel(idxLevel: Long, depth: Int, sample: Int, stats: StatsDigits)
  def calcStatsSamples(momentsRDD: RDD[MomentsByLevel]): RDD[StatsByLevel] = {
    momentsRDD.map { case MomentsByLevel(idxLevel, depth, sample, moments) => StatsByLevel(idxLevel, depth, sample, calcStatsDigits(moments)) }
  }

  def groupStats(statsRDD: RDD[StatsByLevel]): RDD[((Long, Int), StatsDigits)] = {
    statsRDD.map { case StatsByLevel(idxLevel, depth, sample, stats) => ((idxLevel, depth), stats) }.reduceByKey(_+_)
  }

  case class StatsCIByLevel(idxLevel: Long, depth: Int, CIs: CIDigits)
  def calcStatsCIs(dataStatsRDD: RDD[((Long, Int), StatsDigits)], groupStatsRDD: RDD[((Long, Int), StatsDigits)], conf: Array[Double]): RDD[StatsCIByLevel] = {
    groupStatsRDD.join(dataStatsRDD)
      .map { case ((idxLevel, depth), (groupStats, dataStats)) => StatsCIByLevel(idxLevel, depth, groupStats.calcBcaCI(conf, dataStats)) }
  }

  def calcDataStats(levelsRDD: RDD[Level]): RDD[((Long, Int), StatsDigits)] = {
    val originalRDD = levelsRDD.map { case Level(idxLevel, depth, idx, value, d1d2) => OutcomeByLevel(idx, idxLevel, depth, 1, d1d2) }
    val momentsOriginalRDD = calcMomentsSamples(originalRDD)
    val statsOriginalRDD = calcStatsSamples(momentsOriginalRDD)
    groupStats(statsOriginalRDD)
  }

  def calcBootStats(sc: SparkContext, dataRDD: RDD[((Long, Double, Int), Array[String])], numSamples: Int, conf: Array[Double]): (Array[((String, Int), Long)], RDD[StatsCIByLevel]) = {
    val (uniqLevels, levelsRDD) = findLevels(dataRDD)
    val dataStatsRDD = calcDataStats(levelsRDD)
    val (aliasMap, freqRDD) = calcFrequenciesLevels(levelsRDD)

    val sampleSize = dataRDD.count().toInt
    val level0 = freqRDD.filter { case FreqByLevel(idxLevel, freq) => freq.count == sampleSize }.collect()(0)
    val prob = level0.freq.freqD1D2

    val bootTableRDD = generateBootstrapTable(sc, prob, sampleSize, numSamples)
    val bootRDD = generateBootstrapOutcomes(bootTableRDD, levelsRDD, aliasMap)
    val momentsRDD = calcMomentsSamples(bootRDD)
    val statsRDD = calcStatsSamples(momentsRDD)
    val groupStatsRDD = groupStats(statsRDD)
    (uniqLevels, calcStatsCIs(dataStatsRDD, groupStatsRDD, conf))
  }

  def findStatsByIdx(statsCIRDD: RDD[StatsCIByLevel], idx: Int): Array[StatsCIByLevel] = {
    statsCIRDD.filter { case StatsCIByLevel(idxLevel, depth, stats) => idxLevel == idx }.collect()
  }

  case class Frequencies(count: Int, freqD1D2: Array[Double], freqD1: Array[Double], freqD2: Array[Double])
  def calcFrequencies(digitsCounts: List[(Int, Int)]): Frequencies = {
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

  def loadData(sc: SparkContext, filePath: String): RDD[((Long, Double, Int), Array[String])] = {
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

  case class Level(idxLevel: Long, depth: Int, idx: Long, value: Double, d1d2: Int)
  def findLevels(dataLevelRDD: RDD[((Long, Double, Int), Array[String])]): (Array[((String, Int), Long)], RDD[Level]) = {
    val levelsRDD = dataLevelRDD
      .map { case (value, levels) => (value, levels
                                  .zipWithIndex
                                  .map { case (nextLevels, idx) => (levels.slice(0, idx + 1).foldLeft("L")(_ + "." + _), idx) } ) }
      .flatMap { case (value, levels) => levels.map { case (name, depth) => ((name, depth), value) } }

    val uniqLevelsRDD = levelsRDD.map { case (classif, value) => classif }.distinct().sortBy(identity).zipWithIndex()

    (uniqLevelsRDD.collect(),
      uniqLevelsRDD.join(levelsRDD).map { case ((name, depth), (idxLevel, (idx, value, d1d2))) => Level(idxLevel, depth, idx, value, d1d2) })
  }

  case class FreqByLevel(idxLevel: Long, freq: Frequencies)
  def calcFrequenciesLevels(levelsRDD: RDD[Level]): (Map[Long,AliasTable], RDD[FreqByLevel]) = {
    val levelsCountRDD = levelsRDD
      .map { case Level(idxLevel, depth, idx, value, d1d2) => ((idxLevel, d1d2), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, d1d2), count) => (idxLevel, (d1d2, count)) }

    val freqLevelsRDD = levelsCountRDD.groupByKey().map { case (idxLevel, counts) => FreqByLevel(idxLevel, calcFrequencies(counts.toList)) }
    val aliasMap = freqLevelsRDD.map { case FreqByLevel(idxLevel, freq) => (idxLevel, buildAliasTable(freq.freqD1D2)) }.collect().toMap

    (aliasMap, freqLevelsRDD)
  }
}
