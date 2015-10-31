package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.{Multinomial, RandBasis}
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by dvgodoy on 31/10/15.
 */
object Bootstrap {
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

  def groupStats(stats: Array[Stats]): BootStats = {
    BootStats(stats.map(_.n).sum, stats.map(_.mean), stats.map(_.variance), stats.map(_.skewness), stats.map(_.kurtosis), stats.map(_.pearson))
  }

  def calcFrequenciesSample(sc: SparkContext, filePath: String): (Int, List[Double]) = {
    val digitsCount = sc.textFile(filePath)
      .map(value => (findD1D2(value.toDouble), 1))
      .reduceByKey(_+_)
      .collect().toList

    val frequenciesD1D2 = (digitsCount ::: (10 to 99).toSet.diff(digitsCount.map(_._1).toSet).toList.map(n => (n, 0)))
      .map(t => (t._1, t._2/digitsCount.length.toDouble))

    (digitsCount.length, frequenciesD1D2.sorted.map(_._2))
  }
}
