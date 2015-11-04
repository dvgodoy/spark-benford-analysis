package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.{Multinomial, RandBasis}
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable

/**
 * Created by dvgodoy on 31/10/15.
 */
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

  /*def calcFrequenciesSample(sc: SparkContext, filePath: String): (Int, Array[Double], Array[Double], Array[Double]) = {
    val digitsCounts = sc.textFile(filePath)
      .map(value => (findD1D2(value.toDouble), 1))
      .reduceByKey(_ + _)
      .collect().toList

    calcFrequencies(digitsCounts)
  }*/

  private def generateBootstrapTable(sc: SparkContext, prob: Array[Double], sampleSize: Int, numSamples: Int): RDD[(Long, (Int, (Int, Double)))] = {
    val nOutcomes = prob.length
    assert(nOutcomes == 90)
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      val rand = new RandBasis(new MersenneTwister(idx + 42))
      iter.flatMap(sample => Array.fill(sampleSize)(rollToss(nOutcomes, rand)).zipWithIndex
                            .map { case ((roll, toss), idx) => (idx.toLong, (sample, (roll, toss))) })
    }
  }

  def generateBootstrapSamples(sc: SparkContext, prob: Array[Double], sampleSize: Int, numSamples: Int): RDD[((Int, Int), Int)] = {
    assert(prob.length == 90)
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      implicit val rand = new RandBasis(new MersenneTwister(idx + 42))
      val mult = new Multinomial(DenseVector(prob))
      iter.flatMap(sample => mult.sample(sampleSize).map(n => ((sample, n), 1)))
    }
  }

  case class OutcomeByLevel(idx: Long, idxLevel: Long, depth: Int, sample: Int, n: Int)
  def generateBootstrapOutcomes(bootstrapTableRDD: RDD[(Long, (Int, (Int, Double)))], levelsRDD: RDD[Level], aliasMap: Map[Long,AliasTable]): RDD[OutcomeByLevel] = {
    val findIdxLevelRDD = levelsRDD.map { case Level(idxLevel, depth, idx, value, d1d2) => (idx, (idxLevel, depth))}
    bootstrapTableRDD.join(findIdxLevelRDD).map { case (idx, ((sample, (roll, toss)), (idxLevel, depth))) => OutcomeByLevel(idx, idxLevel, depth, sample, findOutcome(aliasMap(idxLevel), (roll, toss))) }
  }

  /*def calcMomentsSamples(bootRDD: RDD[((Int, Int), Int)]): RDD[(Int, Moments)] = {
    /*bootRDD.reduceByKey(_ + _)
      .map(t => (t._1._1, calcMoments(t._1._2, t._2)))
      .reduceByKey(_ + _)*/
    bootRDD.reduceByKey(_ + _)
      .map { case ((sample, n), count) => (sample, calcMoments(n, count))}
      .reduceByKey(_ + _)
  }*/
  case class MomentsByLevel(idxLevel: Long, depth: Int, sample: Int, moments: Moments)
  // TO DO - calculate moments for FSD and SSD only
  def calcMomentsSamples(bootRDD: RDD[OutcomeByLevel]): RDD[MomentsByLevel] = {
    bootRDD.map { case OutcomeByLevel(idx, idxLevel, depth, sample, n) => ((idxLevel, depth, sample, n), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample, n), count) => ((idxLevel, depth, sample), calcMoments(n, count))}
      .reduceByKey(_ + _)
      .map { case ((idxLevel, depth, sample), moments) => MomentsByLevel(idxLevel, depth, sample, moments) }
  }

  case class StatsByLevel(idxLevel: Long, depth: Int, sample: Int, stats: Stats)
  def calcStatsSamples(momentsRDD: RDD[MomentsByLevel]): RDD[StatsByLevel] = {
    momentsRDD.map { case MomentsByLevel(idxLevel, depth, sample, moments) => StatsByLevel(idxLevel, depth, sample, calcStats(moments)) }
  }

  def groupStats(statsRDD: RDD[StatsByLevel]): RDD[((Long, Int), Stats)] = {
    //statsRDD.filter { case StatsByLevel(idxLevel, depth, sample, stats) => depth == 1 }.collect()
    statsRDD.map { case StatsByLevel(idxLevel, depth, sample, stats) => ((idxLevel, depth), stats) }.reduceByKey(_+_)
    // for each stat: sort and toArray
  }

  def calcStatsCIs(groupStatsRDD: RDD[((Long, Int), Stats)], conf: Array[Double], t0: Stats): RDD[((Long, Int), StatsCI)] = {
    groupStatsRDD.map { case ((idxLevel, depth), stats) => ((idxLevel, depth), stats.calcBcaCI(conf, t0)) }
  }

  /*def groupStats(stats: Array[Stats]): BootStats = {
    BootStats(stats.map(_.n).sum, stats.map(_.mean), stats.map(_.variance), stats.map(_.skewness), stats.map(_.kurtosis), stats.map(_.pearson))
  }*/

  def calcFrequencies(digitsCounts: List[(Int, Int)]): (Int, Array[Double], Array[Double], Array[Double]) = {
    //val digitsTotal = digitsCounts.map(t => t._2).sum
    val digitsTotal = digitsCounts.map { case (d1d2, count) => count }.sum
    val countsD1D2 = digitsCounts ::: (10 to 99).toSet.diff(digitsCounts.map(_._1).toSet).toList.map(n => (n, 0))
    /*val frequenciesD1D2 = countsD1D2.map(t => (t._1, t._2/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)*/
    val frequenciesD1D2 = countsD1D2.map { case (d1d2, count) => (d1d2, count/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)
    /*val frequenciesD1 = countsD1D2.map(t => (t._1/10, t._2))
      .groupBy(t => t._1)
      .map(t => (t._1, t._2.map(n => n._2).sum/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)*/
    val frequenciesD1 = countsD1D2.map { case (d1d2, count) => (d1d2/10, count) }
      .groupBy { case (d1, count) => d1 }
      .map { case (d1, arrayCounts) => (d1, arrayCounts.map { case (d1, count) => count }.sum/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)
    /*val frequenciesD2 = countsD1D2.map(t => (t._1%10, t._2))
      .groupBy(t => t._1)
      .map(t => (t._1, t._2.map(n => n._2).sum/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)*/
    val frequenciesD2 = countsD1D2.map { case (d1d2, count) => (d1d2%10, count) }
      .groupBy { case (d2, count) => d2 }
      .map { case (d2, arrayCounts) => (d2, arrayCounts.map { case (d2, count) => count }.sum/digitsTotal.toDouble) }
      .toArray.sorted.map(_._2)

    (digitsTotal, frequenciesD1, frequenciesD2, frequenciesD1D2)
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
    // RDD [(value, Array([level, level depth]))]
    // Assumes values in first column followed by levels from top to bottom
    val levelsRDD = dataLevelRDD
      .map { case (value, levels) => (value, levels
                                  .zipWithIndex
                                  .map { case (nextLevels, idx) => (levels.slice(0, idx + 1).foldLeft("L")(_ + "." + _), idx) } ) }
      .flatMap { case (value, levels) => levels.map { case (name, depth) => ((name, depth), value) } }

    val uniqLevelsRDD = levelsRDD.map { case (classif, value) => classif }.distinct().sortBy(identity).zipWithIndex()

    (uniqLevelsRDD.collect(),
      uniqLevelsRDD.join(levelsRDD).map { case ((name, depth), (idxLevel, (idx, value, d1d2))) => Level(idxLevel, depth, idx, value, d1d2) })
  }
  /*val levelsRDD = dataLevelRDD.map(t => (t(0),t.slice(1,t.length)
                                        .zipWithIndex
                                        .map(l => (t.slice(1, l._2 + 2).foldLeft("L")(_ + "." + _), l._2)))) // RDD [(value, Array([level, level depth]))]*/

  //case class FreqLevel(idxLevel: Long, freqD1: Array[Double], freqD2: Array[Double], freqD1D2: Array[Double])
  def calcFrequenciesLevels(levelsRDD: RDD[Level]): (Map[Long,AliasTable], RDD[(Long, (Int, Array[Double], Array[Double], Array[Double]))]) = {
    /*val levelsCountRDD = levelsRDD
      .flatMap(t => t._2.map(x => ((x._2, x._1, findD1D2(t._1.toDouble)), 1)))                                  // RDD [((level depth, level, d1d2), 1)]
      .reduceByKey(_ + _)                                                                                       // RDD [((level depth, level, d1d2), count)]
      .map(t => ((t._1._1, t._1._2), (t._1._3, t._2)))                                                          // RDD [((level depth, level), (d1d2, count))]*/

    /*val levelsCountRDD = levelsRDD
      .flatMap { case ((idx, value, d1d2), levels) => levels.map { case (name, depth) => ((depth, name, d1d2), 1) } }    // RDD [((level depth, level, d1d2), 1)]
      .reduceByKey(_ + _)                                                                                       // RDD [((level depth, level, d1d2), count)]
      .map { case ((depth, name, d1d2), count) => ((depth, name), (d1d2, count)) }                              // RDD [((level depth, level), (d1d2, count))]*/

    val levelsCountRDD = levelsRDD
      .map { case Level(idxLevel, depth, idx, value, d1d2) => ((idxLevel, d1d2), 1) }
      .reduceByKey(_ + _)
      .map { case ((idxLevel, d1d2), count) => (idxLevel, (d1d2, count)) }

    /*val freqLevelsRDD = levelsCountRDD.groupByKey().map(t => (t._1, calcFrequencies(t._2.toList)))*/

    // RDD [((level depth, level), (# elements, freqD1, freqD2, freqD1D2))]
    //val freqLevelsRDD = levelsCountRDD.groupByKey().map { case ((depth, name), counts) => ((depth, name), calcFrequencies(counts.toList)) }
    val freqLevelsRDD = levelsCountRDD.groupByKey().map { case (idxLevel, counts) => (idxLevel, calcFrequencies(counts.toList)) }
    val aliasMap = freqLevelsRDD.map { case (idxLevel, (count, probd1, probd2, probd1d2)) => (idxLevel, buildAliasTable(probd1d2)) }.collect().toMap

    (aliasMap, freqLevelsRDD)
  }

  /*def calcSplitLevels(freqLevelsRDD: RDD[((Int, String), (Int, Array[Double], Array[Double], Array[Double]))]): RDD[((Long, Int, String), (Int, Int))] = {
    freqLevelsRDD.sortBy(_._1).zipWithIndex()
      .map(t => (t._1._1._1, (t._1._1._2, t._1._2._1, t._2)))
      .groupByKey()
      .map(t => (t._1, t._2.map(_._3), t._2.map(_._1), t._2.map(_._2).map { var s = -1; var sant = -1; d => {sant = s; s += d; (sant + 1, s) }}))
      .flatMap(t => ((t._2 zip t._3) zip t._4).map(s => ((s._1._1, t._1, s._1._2), s._2)))
  }*/

  // RDD[((Int, String), Long)]
  // Associate classification (depth, name) with index of the original sample
  // val idxLevels = levelsRDD.zipWithIndex().flatMap { case ((value, levels), idx) => levels.map { case (name, depth) => ((depth, name), idx) } }

  // RDD[((Int, String), Long)]
  // Generates a unique index for each classification
  // // val uniqLevels = freqLevelsRDD.map(t => t._1).sortBy(identity).zipWithIndex
  // val uniqLevels = levelsRDD.flatMap { case (value, classif) => classif }.distinct().sortBy(identity).zipWithIndex

  // RDD[(Long, (Int, Long))]
  // Associate each index of the original sample with corresponding depths and classifications' unique indexes
  // val idxUniq = idxLevels.join(uniqLevels).map { case ((depth, name), (idx, uniq)) => (idx, (depth, uniq)) }

  // RDD[((Int, String), Array[Double])]
  // Obtains frequencies of D1D2 for each different classification
  // val freqLevelsD1D2 = freqLevelsRDD.map { case ((depth, name), (count, freqD1, freqD2, freqD1D2)) => ((depth, name), freqD1D2) }

  // RDD[(Long, AliasTable)]
  // Generate an alias table for each different classification
  // // val idxAlias = uniqLevels.join(freqLevelsD1D2).map { case ((depth, name), (uniq, prob)) => (uniq, buildAliasTable(prob)) }
  // // val aliasMap = idxAlias.collect.toMap

  // RDD[(Long, ((Int, Double), (Int, Long)))]
  // Associate each rollToss with its corresponding depths and classifications' unique indexes
  // val sampleLevels = bootstrapTable.zipWithIndex.map { case ((sample, (roll, toss)), idx) => (idx, (sample, (roll, toss))) }.join(idxUniq)

  // RDD[(Long, Int, Long, Int)]
  // Generate draws for each index in the original sample and all its corresponding classifications (depth, uniq)
  // val drawLevels = sampleLevels.map { case (idx, ((sample, (roll, toss)), (depth, uniq))) => (idx, depth, uniq, sample, findOutcome(aliasMap(uniq), (roll, toss))) }
}
