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
  private def buildAliasTable(prob: DenseVector[Double]): AliasTable = {
    val nOutcomes = prob.length
    val aliases = DenseVector.zeros[Int](nOutcomes)
    val sum = breeze.linalg.sum(prob)

    val modProb = DenseVector(prob.map { param => param / sum * nOutcomes }.toArray)
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

  def generateBootstrapOutcomes(sc: SparkContext, bootstrapTableRDD: RDD[(Int, (Int, Double))], aliasTable: AliasTable): RDD[((Int, Int), Int)] = {
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

  def calcFrequenciesSample(sc: SparkContext, filePath: String): (Int, Array[Double], Array[Double], Array[Double]) = {
    val digitsCounts = sc.textFile(filePath)
      .map(value => (findD1D2(value.toDouble), 1))
      .reduceByKey(_+_)
      .collect().toList

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
}
