package com.dvgodoy.spark.benford

import breeze.stats.distributions.Gaussian
import scala.collection.mutable.ListBuffer
import scala.math.{pow, floor}
import scala.util.Try

import scala.{specialized=>spec}
import scala.reflect.ClassTag
import scala.collection.mutable.ArraySeq

/**
 * Created by dvgodoy on 30/10/15.
 */
package object util {
  def average[T](xs: Iterable[T])(implicit num: Numeric[T]):Double =
    num.toDouble(xs.sum) / xs.size

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def findD1D2(x: Double): Int = {
    assert(x != 0.0, "Zero value has no significant digits!")
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
    def normalize = Moments(1.0, m1/n, m2/n, m3/n, m4/n, prod/n)
    def + (that: Moments) = addMoments(this, that)
  }
  def calcMoments(x: Int, n: Double = 1.0) = Moments(n, n*x, n*pow(x,2), n*pow(x,3), n*pow(x,4), n*(if (x<10) x else x/10*x%10))
  def addMoments(x: Moments, y: Moments) = Moments(x.n + y.n,
      x.m1 + y.m1,
      x.m2 + y.m2,
      x.m3 + y.m3,
      x.m4 + y.m4,
      x.prod + y.prod)

  case class StatsCI(mean: Array[CI], variance: Array[CI], skewness: Array[CI], kurtosis: Array[CI], pearson: Array[CI])
  case class Stats(n: Double, mean: ListBuffer[Double], variance: ListBuffer[Double], skewness: ListBuffer[Double], kurtosis: ListBuffer[Double], pearson: ListBuffer[Double]) {
    def + (that: Stats) = addStats(this, that)
    def calcBcaCI(conf: Array[Double], t0: Stats) = StatsCI(bcaCI(conf, t0.mean.head, this.mean.toArray),
      bcaCI(conf, t0.variance.head, this.variance.toArray),
      bcaCI(conf, t0.skewness.head, this.skewness.toArray),
      bcaCI(conf, t0.kurtosis.head, this.kurtosis.toArray),
      bcaCI(conf, t0.pearson.head, this.pearson.toArray))
  }
  def addStats(x: Stats, y: Stats) = {
    assert(x.n == y.n)
    Stats(x.n, x.mean ++ y.mean, x.variance ++ y.variance, x.skewness ++ y.skewness, x.kurtosis ++ y.kurtosis, x.pearson ++ y.pearson)
  }

  def calcVariance(m1: Double, m2: Double) = m2 - pow(m1,2)
  def calcSkewness(m1: Double, m2: Double, m3: Double) = (m3 - 3*m2*m1 + 2*pow(m1,3)) / pow(m2 - pow(m1,2), 1.5)
  def calcKurtosis(m1: Double, m2: Double, m3: Double, m4: Double) = (m4 - 4*m3*m1 - 3*pow(m2,2) + 12*m2*pow(m1,2) - 6*pow(m1,4)) / pow(m2 - pow(m1,2), 2)
  def calcStats(x: Moments) = {
    val xn = x.normalize
    val m1p2 = pow(xn.m1,2)
    val m1p3 = pow(xn.m1,3)
    val m1p4 = pow(xn.m1,4)
    val m2p2 = pow(xn.m2,2)
    val variance = xn.m2 - m1p2
    Stats (xn.n,
      ListBuffer(xn.m1),
      ListBuffer(variance),
      ListBuffer((xn.m3 - 3*xn.m2*xn.m1 + 2*m1p3) / pow(variance, 1.5)),
      ListBuffer((xn.m4 - 4*xn.m3*xn.m1 - 3*m2p2 + 12*xn.m2*m1p2 - 6*m1p4) / pow(variance, 2)),
      ListBuffer(xn.prod)
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

  case class CI(alpha: Double, li: Double, ui: Double, lower: Double, upper: Double)
  private def bcaCI(conf: Array[Double], t0: Double, tParam: Array[Double]): Array[CI] = {
    val t = tParam.filter(!_.isInfinite)
    val w = Gaussian(0,1).icdf(t.count(_ < t0) / t.length.toDouble)
    assert(!w.isInfinite)
    val alpha = conf.map(v => (1 - v)/2.0) ++ conf.map(v => (1 + v)/2.0)
    val zalpha = alpha.map(v => Gaussian(0,1).icdf(v))
    val tmean = average(t)
    val tdiff = t.map(_ - tmean)
    val a = average(tdiff.map(pow(_, 3))) / (6 * pow(average(tdiff.map(pow(_, 2))), 1.5))
    assert(!a.isInfinite)
    val adjAlpha = zalpha.map(v => (v, 1 - a * (w + v)))
      .map { case (v, c) => w + (w + v)/(if (c < 0) 0 else c) }
      .map(Gaussian(0,1).cdf)
    val qq = normInter(t, adjAlpha)
    conf.zip(qq).map { case (conf, qq) => CI(conf, qq(0), qq(1), qq(2), qq(3)) }
  }

  private def normInter(tParam: Array[Double], alpha: Array[Double]): Array[Array[Double]] = {
    val t = tParam.filter(!_.isInfinite)
    val R = t.length
    val rk = alpha.map(_ * (R+1)).map { v => if (v < 1) 1 else if (v > R) R else v }
    val k = rk.map(_.toInt)
    val inds = (1 to t.length).toArray
    val kvs = k.filter(k => k > 0 & k < R)
    //val partial = (kvs ++ kvs.map(_ + 1) ++ Array(1,R)).toSet.toArray.sorted
    val tstar = t.sorted
    val ints = (k zip rk).map { case (r, rk) => r.toDouble == rk }
    val out = (ints.zipWithIndex.map { case (int, idx) => if (int) tstar(k(inds(idx) - 1) - 1) else inds(idx) } zip (k.map(_ == 0) zip k.map(_ == R)))
      .map { case (out, (k0, kR)) => if (kR) tstar(R - 1) else if (k0) tstar(0) else out }
    val temp = (inds zip (ints.map(_ ^ true) zip k).map { case (ints, k) => ints & (k != 0) & (k != R) }).filter(_._2).map(_._1)
    val temp1 = alpha.zipWithIndex.filter { case (alpha, idx) => temp.contains(idx + 1) }.map { case (alpha, idx) => Gaussian(0, 1).icdf(alpha) }
    val ktemp = k.zipWithIndex.filter { case (k, idx) => temp.contains(idx + 1) }.map(_._1)
    val temp2 = ktemp.map(k => Gaussian(0, 1).icdf(k / (R + 1.0)))
    val temp3 = ktemp.map(k => Gaussian(0, 1).icdf((k + 1)/(R + 1.0)))
    val tk = ktemp.map(v => tstar(v - 1))
    val tk1 = ktemp.map(v => tstar(v))
    val temp4 = (temp1, temp2, temp3).zipped.toArray.map { case (temp1, temp2, temp3) => (temp1 - temp2)/(temp3 - temp2) }
    val temp5 = (tk, tk1, temp4).zipped.toArray.map { case (tk, tk1, temp4) => tk + temp4*(tk1 - tk) }
    (rk.zipWithIndex.map { case (rk, idx) => (idx % (alpha.length / 2), rk) } ++
      out.zipWithIndex.map { case (out, idx) => (idx % (alpha.length / 2), if (temp.contains(idx + 1)) temp5(temp.indexWhere(_ == idx + 1)) else out) })
      .groupBy(_._1).toArray.sortBy(_._1).map { case (m, values) => values.map(_._2) }
  }

  class RVector[@spec(Double, Int, Float, Long) V](val data: ArraySeq[V], val length: Int) extends scala.AnyRef with Serializable {
    def this(data: ArraySeq[V]) = this(data, data.length)
    def this(data: List[V]) = this(ArraySeq(data: _*), data.length)
    def this(data: Vector[V]) = this(ArraySeq(data: _*), data.length)
    def this(data: Array[V]) = this(ArraySeq(data: _*), data.length)
    def this(length: Int)(implicit man: ClassTag[V]) = this(new ArraySeq[V](length), length)

    override def toString = {
      data.iterator.mkString("RVector(",", ", ")")
    }

    def toArray(implicit man: ClassTag[V]): Array[V] = Array(data: _*)
    def apply(indexes: Int*) = {
      new RVector(indexes.toArray.map(index => data(index - 1)))
    }
    def +(that: RVector[V])(implicit man: ClassTag[V]): Array[(V,V)] = {
      assert(this.length == that.length, "Length of both vectors must match!")
      this.toArray zip that.toArray
    }
  }

  object RVector {
    def apply[@spec(Double, Int, Float,Long) V](data: ArraySeq[V]) = new RVector(data)
    def apply[@spec(Double, Int, Float,Long) V](data: List[V]) = new RVector(data)
    def apply[@spec(Double, Int, Float,Long) V](data: Vector[V]) = new RVector(data)
    def apply[@spec(Double, Int, Float,Long) V](data: Array[V]) = new RVector(data)
    def apply[@spec(Double, Int, Float,Long) V](data: V*)(implicit man: ClassTag[V]) = new RVector(Array(data: _*))
    def apply(data: Range) = new RVector(data.toArray)
  }
}
