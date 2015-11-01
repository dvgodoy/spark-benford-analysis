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

  private def findOutcome(aliasTable: AliasTable, rollToss: (Int, Double)): Int = if (rollToss._2 < aliasTable.modProb(rollToss._1)) rollToss._1 + 1 else aliasTable.aliases(rollToss._1) + 1

  private def generateBootstrapTable(sc: SparkContext, prob: Array[Double], sampleSize: Int, numSamples: Int): RDD[(Int, (Int, Double))] = {
    val nOutcomes = prob.length
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      val rand = new RandBasis(new MersenneTwister(idx + 42))
      iter.flatMap(sample => Array.fill(sampleSize)(rollToss(nOutcomes, rand)).map(n => (sample, (n._1, n._2))))
    }
  }

  def generateBootstrapOutcomes(bootstrapTableRDD: RDD[(Int, (Int, Double))], prob: Array[Double]): RDD[((Int, Int), Int)] = {
    val aliasTable = buildAliasTable(prob)
    bootstrapTableRDD.map(t => ((t._1, findOutcome(aliasTable, t._2)),1))
  }

  def generateBootstrapSamples(sc: SparkContext, prob: Array[Double], sampleSize: Int, numSamples: Int): RDD[((Int, Int), Int)] = {
    sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      implicit val rand = new RandBasis(new MersenneTwister(idx + 42))
      val mult = new Multinomial(DenseVector(prob))
      iter.flatMap(sample => mult.sample(sampleSize).map(n => ((sample, n), 1)))
    }
  }

  def calcMomentsSamples(bootRDD: RDD[((Int, Int), Int)]): RDD[(Int, Moments)] = {
    bootRDD.reduceByKey(_ + _)
      .map(t => (t._1._1, calcMoments(t._1._2, t._2)))
      .reduceByKey(_ + _)
  }

  def calcStatsSamples(momentsRDD: RDD[(Int, Moments)]): Array[Stats] = {
    momentsRDD.map(t => calcStats(t._2)).collect()
  }

  case class BootStats(n: Double, mean: Array[Double], variance: Array[Double], skewness: Array[Double], kurtosis: Array[Double], pearson: Array[Double]) {
    def calcCI = {
      // TO DO
    }
  }

  def groupStats(stats: Array[Stats]): BootStats = {
    BootStats(stats.map(_.n).sum, stats.map(_.mean), stats.map(_.variance), stats.map(_.skewness), stats.map(_.kurtosis), stats.map(_.pearson))
  }

  def calcFrequencies(digitsCounts: List[(Int, Int)]): (Int, Array[Double], Array[Double], Array[Double]) = {
    val digitsTotal = digitsCounts.map(t => t._2).sum
    val countsD1D2 = digitsCounts ::: (10 to 99).toSet.diff(digitsCounts.map(_._1).toSet).toList.map(n => (n, 0))
    val frequenciesD1D2 = countsD1D2.map(t => (t._1, t._2/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)
    val frequenciesD1 = countsD1D2.map(t => (t._1/10, t._2))
      .groupBy(t => t._1)
      .map(t => (t._1, t._2.map(n => n._2).sum/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)
    val frequenciesD2 = countsD1D2.map(t => (t._1%10, t._2))
      .groupBy(t => t._1)
      .map(t => (t._1, t._2.map(n => n._2).sum/digitsTotal.toDouble))
      .toArray.sorted.map(_._2)

    (digitsTotal, frequenciesD1, frequenciesD2, frequenciesD1D2)
  }

  def calcFrequenciesSample(sc: SparkContext, filePath: String): (Int, Array[Double], Array[Double], Array[Double]) = {
    val digitsCounts = sc.textFile(filePath)
      .map(value => (findD1D2(value.toDouble), 1))
      .reduceByKey(_ + _)
      .collect().toList

    calcFrequencies(digitsCounts)
  }

  def findLevels(sc: SparkContext, filePath: String): RDD[(String, Array[(String, Int)])] = {
    val dataLevelRDD = sc.textFile(filePath)
      .map(line => line.split(",")
        .map(_.trim.replace("\"","")))

    /*val levelsRDD = dataLevelRDD.map(t => (t(0),t.slice(1,t.length)
                                          .zipWithIndex
                                          .map(l => (t.slice(1, l._2 + 2).foldLeft("L")(_ + "." + _), l._2)))) // RDD [(value, Array([level, level depth]))]*/

    val levelsRDD = dataLevelRDD
      .map { line => (line(0), line.slice(1, line.length)
                                  .zipWithIndex
                                  .map { case (nextLevels, idx) => (line.slice(1, idx + 2).foldLeft("L")(_ + "." + _), idx) } ) } // RDD [(value, Array([level, level depth]))]

    levelsRDD
  }

  def calcFrequenciesLevels(levelsRDD: RDD[(String, Array[(String, Int)])]): RDD[((Int, String), (Int, Array[Double], Array[Double], Array[Double]))] = {
    /*val levelsCountRDD = levelsRDD
      .flatMap(t => t._2.map(x => ((x._2, x._1, findD1D2(t._1.toDouble)), 1)))                                  // RDD [((level depth, level, d1d2), 1)]
      .reduceByKey(_ + _)                                                                                       // RDD [((level depth, level, d1d2), count)]
      .map(t => ((t._1._1, t._1._2), (t._1._3, t._2)))                                                          // RDD [((level depth, level), (d1d2, count))]*/

    val levelsCountRDD = levelsRDD
      .flatMap { case (value, levels) => levels.map { case (name, depth) => ((depth, name, findD1D2(value.toDouble)), 1) } }    // RDD [((level depth, level, d1d2), 1)]
      .reduceByKey(_ + _)                                                                                       // RDD [((level depth, level, d1d2), count)]
      .map { case ((depth, name, d1d2), count) => ((depth, name), (d1d2, count)) }                              // RDD [((level depth, level), (d1d2, count))]


    /*val freqLevelsRDD = levelsCountRDD.groupByKey().map(t => (t._1, calcFrequencies(t._2.toList)))*/

    // RDD [((level depth, level), (# elements, freqD1, freqD2, freqD1D2))]
    val freqLevelsRDD = levelsCountRDD.groupByKey().map { case ((depth, name), counts) => ((depth, name), calcFrequencies(counts.toList)) }

    freqLevelsRDD
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
  // val idxLevels = levelsRDD.zipWithIndex().flatMap { case ((number, levels), idx) => levels.map { case (name, depth) => ((depth, name), idx) } }

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
  // val idxAlias = uniqLevels.join(freqLevelsD1D2).map { case ((depth, name), (uniq, prob)) => (uniq, buildAliasTable(prob)) }
  // val aliasMap = idxAlias.collect.toMap

  // RDD[(Long, ((Int, Double), (Int, Long)))]
  // Associate each rollToss with its corresponding depths and classifications' unique indexes
  // val sampleLevels = bootstrapTable.zipWithIndex.map { case ((sample, (roll, toss)), idx) => (idx, (sample, (roll, toss))) }.join(idxUniq)

  // RDD[(Long, Int, Long, Int)]
  // Generate draws for each index in the original sample and all its corresponding classifications (depth, uniq)
  // val drawLevels = sampleLevels.map { case (idx, ((sample, (roll, toss)), (depth, uniq))) => (idx, depth, uniq, sample, findOutcome(aliasMap(uniq), (roll, toss))) }
}
