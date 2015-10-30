package com.dvgodoy.spark.benford.distributions

import breeze.linalg.DenseVector
import breeze.stats.distributions.{RandBasis, Multinomial}
import com.dvgodoy.spark.benford.constants._
import com.dvgodoy.spark.benford.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext

object Benford {
  val probMultinomialD1 = DenseVector.vertcat(DenseVector.zeros[Double](1), DenseVector(probabilitiesD1))
  val probMultinomialD2 = DenseVector.vertcat(DenseVector.zeros[Double](1), DenseVector(probabilitiesD2))
  val probMultinomialD1D2 = DenseVector.vertcat(DenseVector.zeros[Double](10), DenseVector(probabilitiesD1D2))
}

class Benford {
  def generateBootstrapSamples(sc: SparkContext, sampleSize: Int, numSamples: Int) = {
    val bootRDD = sc.parallelize(1 to numSamples).mapPartitionsWithIndex { (idx, iter) =>
      implicit val rand = new RandBasis(new MersenneTwister(idx + 42))
      val mult = new Multinomial(Benford.probMultinomialD1D2)
      iter.flatMap(sample => mult.sample(sampleSize)).map(n => (n, 1))
    }
    val moments = bootRDD.reduceByKey(_+_)
      .map(num => calcMoments(num._1,num._2))
      .reduce((xMom, yMom) => addMoments(xMom, yMom))
    val bootStats = calcStats(moments)
  }

  def calcFrequenciesSample(sc: SparkContext, filePath: String) = {
    val digitsRDD = sc.textFile(filePath)
      .map(value => (findD1D2(value.toDouble), 1))
      .reduceByKey(_+_)
  }
}