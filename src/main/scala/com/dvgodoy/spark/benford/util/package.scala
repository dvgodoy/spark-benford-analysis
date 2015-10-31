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

  case class Stats(n: Double, mean: Double, variance: Double, skewness: Double, kurtosis: Double, pearson: Double)
  def calcVariance(m1: Double, m2: Double) = m2 - pow(m1,2)
  def calcSkewness(m1: Double, m2: Double, m3: Double) = (m3 - 3*m2*m1 + 2*pow(m1,3)) / pow(m2 - pow(m1,2), 1.5)
  def calcKurtosis(m1: Double, m2: Double, m3: Double, m4: Double) = (m4 - 4*m3*m1 - 3*pow(m2,2) + 12*m2*pow(m1,2) - 6*pow(m1,4)) / pow(m2 - pow(m1,2), 2)
  def calcStats(x: Moments) = {
    val xn = x.normalize
    /*Stats(xn.n,
      xn.m1,
      calcVariance(xn.m1, xn.m2),
      calcSkewness(xn.m1, xn.m2, xn.m3),
      calcKurtosis(xn.m1, xn.m2, xn.m3, xn.m4),
      xn.prod)*/
    val m1p2 = pow(xn.m1,2)
    val m1p3 = pow(xn.m1,3)
    val m1p4 = pow(xn.m1,4)
    val m2p2 = pow(xn.m2,2)
    val variance = xn.m2 - m1p2
    Stats (xn.n,
      xn.m1,
      variance,
      (xn.m3 - 3*xn.m2*xn.m1 + 2*m1p3) / pow(variance, 1.5),
      (xn.m4 - 4*xn.m3*xn.m1 - 3*m2p2 + 12*xn.m2*m1p2 - 6*m1p4) / pow(variance, 2),
      xn.prod
    )
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