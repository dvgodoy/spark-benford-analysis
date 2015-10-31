package com.dvgodoy.spark.benford

import breeze.linalg.DenseVector
import breeze.stats.distributions.RandBasis

import scala.collection.mutable
import scala.math.{pow, floor}

/**
 * Created by dvgodoy on 30/10/15.
 */
package object util {
  def findD1D2(x: Double): Int = {
    val absx = scala.math.abs(x)
    return if (absx < 10) {
      findD1D2(absx * 10)
    } else if (absx > 99) {
      findD1D2(absx / 10)
    } else {
      floor(absx).toInt
    }
  }

  case class Moments(n: Double, m1: Double, m2: Double, m3: Double, m4: Double, prod: Double) {
    def normalize = new Moments(1.0, m1/n, m2/n, m3/n, m4/n, prod/n)
    def + (that: Moments) = addMoments(this, that)
  }
  def calcMoments(x: Int, n: Double = 1.0) = Moments(n, n*x, n*pow(x,2), n*pow(x,3), n*pow(x,4), n*(if (x<10) x else x/10*x%10))
  def addMoments(x: Moments, y: Moments) = Moments(x.n + y.n,
      x.m1 + y.m1,
      x.m2 + y.m2,
      x.m3 + y.m3,
      x.m4 + y.m4,
      x.prod + y.prod)

  case class BootStats(n: Double, mean: Array[Double], variance: Array[Double], skewness: Array[Double], kurtosis: Array[Double], pearson: Array[Double])

  case class Stats(n: Double, mean: Double, variance: Double, skewness: Double, kurtosis: Double, pearson: Double)
  def calcVariance(m1: Double, m2: Double) = m2 - pow(m1,2)
  def calcSkewness(m1: Double, m2: Double, m3: Double) = (m3 - 3*m2*m1 + 2*pow(m1,3)) / pow(m2 - pow(m1,2), 1.5)
  def calcKurtosis(m1: Double, m2: Double, m3: Double, m4: Double) = (m4 - 4*m3*m1 - 3*pow(m2,2) + 12*m2*pow(m1,2) - 6*pow(m1,4)) / pow(m2 - pow(m1,2), 2)
  def calcStats(x: Moments) = {
    val xn = x.normalize
    Stats(xn.n,
      xn.m1,
      calcVariance(xn.m1, xn.m2),
      calcSkewness(xn.m1, xn.m2, xn.m3),
      calcKurtosis(xn.m1, xn.m2, xn.m3, xn.m4),
      xn.prod)
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

  private def rollToss(aliasTable: AliasTable, rand: RandBasis): (Int, Double) = {
    (rand.randInt(aliasTable.nOutcomes).get(),rand.uniform.get())
  }

  private def time[A](a: => A, n:Int) = {
    var times = List[Long]()
    for (_ <- 1 to n) {
      val now = System.nanoTime
      val res = a
      times :::= List(System.nanoTime - now)
    }
    val result = times.sum / n
    println("%d microseconds".format(result / 1000))
    result
  }

}
