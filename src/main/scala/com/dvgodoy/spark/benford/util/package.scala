package com.dvgodoy.spark.benford

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.{Gaussian, ChiSquared}
import com.dvgodoy.spark.benford.constants._
import org.apache.commons.math3.special.Gamma
import org.apache.spark.rdd.RDD
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.functional.syntax._
import scala.collection.immutable.Range
import scala.collection.mutable.ListBuffer
import scala.util.Try
import org.apache.commons.math3.util.FastMath

package object util {

  def erfinv(x: Double): Double = {
    var w: Double = -FastMath.log((1.0 - x) * (1.0 + x))
    var p: Double = 0.0
    if (w < 6.25) {
      w = w - 3.125
      p = -3.6444120640178196996e-21
      p = -1.685059138182016589e-19 + p * w
      p = 1.2858480715256400167e-18 + p * w
      p = 1.115787767802518096e-17 + p * w
      p = -1.333171662854620906e-16 + p * w
      p = 2.0972767875968561637e-17 + p * w
      p = 6.6376381343583238325e-15 + p * w
      p = -4.0545662729752068639e-14 + p * w
      p = -8.1519341976054721522e-14 + p * w
      p = 2.6335093153082322977e-12 + p * w
      p = -1.2975133253453532498e-11 + p * w
      p = -5.4154120542946279317e-11 + p * w
      p = 1.051212273321532285e-09 + p * w
      p = -4.1126339803469836976e-09 + p * w
      p = -2.9070369957882005086e-08 + p * w
      p = 4.2347877827932403518e-07 + p * w
      p = -1.3654692000834678645e-06 + p * w
      p = -1.3882523362786468719e-05 + p * w
      p = 0.0001867342080340571352 + p * w
      p = -0.00074070253416626697512 + p * w
      p = -0.0060336708714301490533 + p * w
      p = 0.24015818242558961693 + p * w
      p = 1.6536545626831027356 + p * w
    } else if (w < 16.0) {
      w = FastMath.sqrt(w) - 3.25
      p = 2.2137376921775787049e-09
      p = 9.0756561938885390979e-08 + p * w
      p = -2.7517406297064545428e-07 + p * w
      p = 1.8239629214389227755e-08 + p * w
      p = 1.5027403968909827627e-06 + p * w
      p = -4.013867526981545969e-06 + p * w
      p = 2.9234449089955446044e-06 + p * w
      p = 1.2475304481671778723e-05 + p * w
      p = -4.7318229009055733981e-05 + p * w
      p = 6.8284851459573175448e-05 + p * w
      p = 2.4031110387097893999e-05 + p * w
      p = -0.0003550375203628474796 + p * w
      p = 0.00095328937973738049703 + p * w
      p = -0.0016882755560235047313 + p * w
      p = 0.0024914420961078508066 + p * w
      p = -0.0037512085075692412107 + p * w
      p = 0.005370914553590063617 + p * w
      p = 1.0052589676941592334 + p * w
      p = 3.0838856104922207635 + p * w
    } else if (!w.isInfinite) {
      w = FastMath.sqrt(w) - 5.0
      p = -2.7109920616438573243e-11
      p = -2.5556418169965252055e-10 + p * w
      p = 1.5076572693500548083e-09 + p * w
      p = -3.7894654401267369937e-09 + p * w
      p = 7.6157012080783393804e-09 + p * w
      p = -1.4960026627149240478e-08 + p * w
      p = 2.9147953450901080826e-08 + p * w
      p = -6.7711997758452339498e-08 + p * w
      p = 2.2900482228026654717e-07 + p * w
      p = -9.9298272942317002539e-07 + p * w
      p = 4.5260625972231537039e-06 + p * w
      p = -1.9681778105531670567e-05 + p * w
      p = 7.5995277030017761139e-05 + p * w
      p = -0.00021503011930044477347 + p * w
      p = -0.00013871931833623122026 + p * w
      p = 1.0103004648645343977 + p * w
      p = 4.8499064014085844221 + p * w
    } else {
      p = Inf
    }
    p * x
  }

  def icdf(mu: Double, sigma: Double, p: Double): Double = {
    mu + sigma * math.sqrt(2.0) * erfinv(2 * p - 1)
  }

  def erf(x: Double): Double = {
    val res = if (FastMath.abs(x) > 40) {
      if (x > 0) 1 else - 1
    } else {
      val ret: Double = Gamma.regularizedGammaP(0.5, x * x, 1.0e-15, 10000)
      if (x < 0) -ret else ret
    }
    res
  }

  def cdf(mu: Double, sigma: Double, x: Double) = .5 * (1 + erf( (x - mu)/ (math.sqrt(2.0) * sigma)))
  /*
  #
  # Function ported and adapted from R package "boot: Bootstrap Functions (Originally by Angelo Canty for S)"
  #
  #  Interpolation on the normal quantile scale.  For a non-integer
  #  order statistic this function interpolates between the surrounding
  #  order statistics using the normal quantile scale.  See equation
  #  5.8 of Davison and Hinkley (1997)
  #
  */
  private def normInter(tParam: Array[Double], alphaParam: Array[Double]): DenseMatrix[Double] = {
    val t = DenseVector(tParam.filter(!_.isInfinite))
    val alpha = DenseVector(alphaParam)
    val R = t.length.toDouble
    val rk = alpha :* (R+1)
    rk(rk :< 1.0) := 1.0
    rk(rk :> R) := R
    val k = floor(rk)
    val inds = DenseVector((1 to alpha.length): _*)
    val out = DenseVector(Range.Double(1.0, alpha.length + 1.0, 1.0): _*)
    val kvs = k((k :> 0.0) :& (k :< R)).toDenseVector
    val tstar = t(argsort(t)).toDenseVector
    val ints = k :== rk
    if (any(ints)) out(inds(ints).toArray.map(_ - 1).toSeq) := tstar(k(inds(ints).toArray.map(_ - 1).toSeq).toArray.map(_.toInt - 1).toSeq)
    out(k :== 0.0) := tstar(0)
    out(k :== R) := tstar(R.toInt - 1)
    val temp = inds((ints :^^ BitVector.ones(ints.length)) :& (k :!= 0.0) :& (k :!= R)).toArray.map(_ - 1).toSeq
    //val temp1 = DenseVector(alpha(temp).toArray.map(Gaussian(0,1).icdf(_)))
    val temp1 = DenseVector(alpha(temp).toArray.map(icdf(0,1,_)))
    //val temp2 = DenseVector((k(temp) :/ (R + 1.0)).toArray.map(Gaussian(0,1).icdf(_)))
    val temp2 = DenseVector((k(temp) :/ (R + 1.0)).toArray.map(icdf(0,1,_)))
    //val temp3 = DenseVector(((k(temp) :+ 1.0) :/ (R + 1.0)).toArray.map(Gaussian(0,1).icdf(_)))
    val temp3 = DenseVector(((k(temp) :+ 1.0) :/ (R + 1.0)).toArray.map(icdf(0,1,_)))
    val tk = tstar(k(temp).toArray.map(_.toInt - 1).toSeq)
    val tk1 = tstar(k(temp).toArray.map(_.toInt).toSeq)
    out(temp) := tk :+ ((temp1 :- temp2) :/ (temp3 :- temp2) :* (tk1 :- tk))
    DenseMatrix.horzcat(rk.toDenseMatrix.reshape(alphaParam.length/2,2), out.toDenseMatrix.reshape(alphaParam.length/2,2))
  }

  /*
  #
  # Function ported and adapted from R package "boot: Bootstrap Functions (Originally by Angelo Canty for S)"
  #
  #  Adjusted Percentile (BCa) Confidence interval method.  This method
  #  uses quantities calculated from the empirical influence values to
  #  improve on the precentile interval.  Usually the required order
  #  statistics for this method will not be integers and so norm.inter
  #  is used to find them.
  #
  */
  private def bcaCI(conf: Array[Double], t0: Double, tParam: Array[Double]): Array[CI] = {
    return if (tParam.length > 0) {
      val t = tParam.filter(!_.isInfinite)
      //val w = Gaussian(0, 1).icdf(t.count(_ < t0) / t.length.toDouble)
      val w = icdf(0, 1, t.count(_ < t0) / t.length.toDouble)
      assert(!w.isInfinite)
      val alpha = conf.map(v => (1 - v) / 2.0) ++ conf.map(v => (1 + v) / 2.0)
      //val zalpha = alpha.map(v => Gaussian(0, 1).icdf(v))
      val zalpha = alpha.map(v => icdf(0, 1, v))
      val tmean = average(t)
      val tdiff = t.map(_ - tmean)
      val a = average(tdiff.map(pow(_, 3))) / (6 * pow(average(tdiff.map(pow(_, 2))), 1.5))
      assert(!a.isInfinite)
      val adjAlpha = zalpha.map(v => (v, 1 - a * (w + v)))
        //.map { case (v, c) => Gaussian(0, 1).cdf(w + (w + v) / c) }
        .map { case (v, c) => cdf(0, 1, w + (w + v) / c) }
      val qq = normInter(t, adjAlpha)
      val resultMatrix = DenseMatrix.horzcat(DenseMatrix(conf).reshape(conf.length, 1), qq, DenseMatrix(Array.fill(conf.length)(t0)).t)
      val result = for (i <- (0 until conf.length)) yield resultMatrix(i, ::).t.toArray
      result.map { case Array(alpha, li, ui, lower, upper, t0) => CI(alpha, li, ui, lower, upper, t0) }.toArray
    } else {
      val result = for (i <- (0 until conf.length)) yield CI(conf(i), Inf, -Inf, Inf, -Inf, t0)
      result.toArray
    }
  }

  private def matchAlphaCIs(ci1: Array[CI], ci2: Array[CI]): Array[(CI, Array[CI])] = {
    for (ci <- ci1) yield (ci, ci2.filter { case CI(alpha, li, ui, lower, upper, t0) => alpha == ci.alpha })
  }

  private def findOverlap(stat: Array[Overlap], alphaParam: Double): Boolean = stat.filter { case Overlap(alpha, ov) => alpha == alphaParam }.map { case Overlap(alpha, ov) => ov }.head
  private def findContain(stat: Array[Contain], alphaParam: Double): Boolean = stat.filter { case Contain(alpha, co) => alpha == alphaParam }.map { case Contain(alpha, co) => co }.head

  private def findStatsResults(statsOv: StatsOverlap, statsCo: StatsContain, alphaParam: Double): StatsOC = {
    val mean = OverlapContain(findOverlap(statsOv.mean, alphaParam), findContain(statsCo.mean, alphaParam))
    val variance = OverlapContain(findOverlap(statsOv.variance, alphaParam), findContain(statsCo.variance, alphaParam))
    val skewness = OverlapContain(findOverlap(statsOv.skewness, alphaParam), findContain(statsCo.skewness, alphaParam))
    val kurtosis = OverlapContain(findOverlap(statsOv.kurtosis, alphaParam), findContain(statsCo.kurtosis, alphaParam))
    StatsOC(mean, variance, skewness, kurtosis)
  }

  private def findRegsResults(regsOv: RegsOverlap, regsCo: RegsContain, alphaParam: Double): RegsOC = {
    val pearson = OverlapContain(findOverlap(regsOv.pearson, alphaParam), findContain(regsCo.pearson, alphaParam))
    val alpha0 = OverlapContain(findOverlap(regsOv.alpha0, alphaParam), findContain(regsCo.alpha0, alphaParam))
    val alpha1 = OverlapContain(findOverlap(regsOv.alpha1, alphaParam), findContain(regsCo.alpha1, alphaParam))
    val beta0 = OverlapContain(findOverlap(regsOv.beta0, alphaParam), findContain(regsCo.beta0, alphaParam))
    val beta1 = OverlapContain(findOverlap(regsOv.beta1, alphaParam), findContain(regsCo.beta1, alphaParam))
    RegsOC(pearson, alpha0, alpha1, beta0, beta1)
  }

  protected case class Overlap(alpha: Double, overlaps: Boolean)
  protected case class Contain(alpha: Double, contains: Boolean)
  protected case class StatsOverlap(n: Double, mean: Array[Overlap], variance: Array[Overlap], skewness: Array[Overlap], kurtosis: Array[Overlap])
  protected case class StatsContain(n: Double, mean: Array[Contain], variance: Array[Contain], skewness: Array[Contain], kurtosis: Array[Contain])
  protected case class RegsOverlap(n: Double, pearson: Array[Overlap], alpha0: Array[Overlap], alpha1: Array[Overlap], beta0: Array[Overlap], beta1: Array[Overlap])
  protected case class RegsContain(n: Double, pearson: Array[Contain], alpha0: Array[Contain], alpha1: Array[Contain], beta0: Array[Contain], beta1: Array[Contain])
  protected case class OverlapDigits(n: Double, d1d2: StatsOverlap, d1: StatsOverlap, d2: StatsOverlap, r: RegsOverlap)
  protected case class ContainDigits(n: Double, d1d2: StatsContain, d1: StatsContain, d2: StatsContain, r: RegsContain)

  protected case class Results(n: Double, stats: Int, regs: Int, d1d2: StatsOC, d1: StatsOC, d2: StatsOC, r: RegsOC)

  protected case class RegsOC(pearsonOC: OverlapContain, alpha0: OverlapContain, alpha1: OverlapContain, beta0: OverlapContain, beta1: OverlapContain) {
    def count: Int = Array(alpha0.result, alpha1.result, beta0.result, beta1.result).map(if (_) 1 else 0).sum
    def pearson: Boolean = pearsonOC.result
  }

  protected case class OverlapContain(overlaps: Boolean, contains: Boolean) {
    def result: Boolean = overlaps || contains
  }

  protected case class StatsOC(mean: OverlapContain, variance: OverlapContain, skewness: OverlapContain, kurtosis: OverlapContain) {
    def count: Int = Array(mean.result, variance.result, skewness.result, kurtosis.result).map(if (_) 1 else 0).sum
  }

  protected case class StatsCI(n: Double,
                               mean: Array[CI],
                               variance: Array[CI],
                               skewness: Array[CI],
                               kurtosis: Array[CI]) {
    def overlaps(that: StatsCI) = StatsOverlap(this.n,
      matchAlphaCIs(this.mean, that.mean)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.variance, that.variance)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.skewness, that.skewness)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.kurtosis, that.kurtosis)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) }
    )
    def contains(exact: Stats) = StatsContain(this.n,
      this.mean.map { case ci => Contain(ci.alpha, ci.contains(exact.mean.head)) },
      this.variance.map { case ci => Contain(ci.alpha, ci.contains(exact.variance.head)) },
      this.skewness.map { case ci => Contain(ci.alpha, ci.contains(exact.skewness.head)) },
      this.kurtosis.map { case ci => Contain(ci.alpha, ci.contains(exact.kurtosis.head)) }
    )
  }

  protected case class RegsCI(n: Double,
                              pearson: Array[CI],
                              alpha0: Array[CI],
                              alpha1: Array[CI],
                              beta0: Array[CI],
                              beta1: Array[CI]) {
    def overlaps(that: RegsCI) = RegsOverlap(this.n,
      matchAlphaCIs(this.pearson, that.pearson)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.alpha0, that.alpha0)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.alpha1, that.alpha1)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.beta0, that.beta0)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) },
      matchAlphaCIs(this.beta1, that.beta1)
        .map { case (thisStat, thatArray) => assert(thatArray.length == 1); Overlap(thisStat.alpha, thisStat.overlaps(thatArray.head)) }
    )
    def contains(exact: Regs) = RegsContain(this.n,
      this.pearson.map { case ci => Contain(ci.alpha, ci.contains(exact.pearson.head)) },
      this.alpha0.map { case ci => Contain(ci.alpha, ci.contains(if (exact.alpha0.nonEmpty) exact.alpha0.head else Inf)) },
      this.alpha1.map { case ci => Contain(ci.alpha, ci.contains(if (exact.alpha1.nonEmpty) exact.alpha1.head else Inf)) },
      this.beta0.map { case ci => Contain(ci.alpha, ci.contains(if (exact.beta0.nonEmpty) exact.beta0.head else Inf)) },
      this.beta1.map { case ci => Contain(ci.alpha, ci.contains(if (exact.beta1.nonEmpty) exact.beta1.head else Inf)) }
    )
  }

  protected case class CI(alpha: Double, li: Double, ui: Double, lower: Double, upper: Double, t0: Double) {
    def overlaps(that: CI) = (this.lower <= that.upper) && (this.upper >= that.lower)
    def contains(exact: Double) = (exact >= this.lower) && (exact <= this.upper)
  }

  protected[benford] case class AliasTable(modProb: DenseVector[Double], aliases: DenseVector[Int], nOutcomes: Int)

  protected[benford] def findD1D2(x: Double): Int = {
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

  protected[benford] case class Moments(n: Double, m1: Double, m2: Double, m3: Double, m4: Double, prod: Double) {
    def normalize = Moments(1.0, m1/n, m2/n, m3/n, m4/n, prod/n)
    def + (that: Moments) = addMoments(this, that)
  }
  protected[benford] def calcMoments(x: Int, n: Double = 1.0): MomentsDigits = {
    val d1 = x/10
    val d2 = x%10
    MomentsDigits(Moments(n, n*x, n*pow(x, 2), n*pow(x, 3), n*pow(x, 4), n*(x/10)*(x%10)),
      Moments(n, n*d1, n*pow(d1, 2), n*pow(d1, 3), n*pow(d1, 4), 0),
      Moments(n, n*d2, n*pow(d2, 2), n*pow(d2, 3), n*pow(d2, 4), 0))
  }
  protected[benford] def addMoments(x: Moments, y: Moments) = Moments(x.n + y.n,
    x.m1 + y.m1,
    x.m2 + y.m2,
    x.m3 + y.m3,
    x.m4 + y.m4,
    x.prod + y.prod)

  protected[benford] case class MomentsDigits(d1d2: Moments, d1: Moments, d2: Moments) {
    def + (that: MomentsDigits) = addMomentsDigits(this, that)
  }
  protected[benford] def addMomentsDigits(x: MomentsDigits, y: MomentsDigits) = MomentsDigits(x.d1d2 + y.d1d2, x.d1 + y.d1, x.d2 + y.d2)

  protected[benford] case class Stats(n: Double,
                   mean: ListBuffer[Double],
                   variance: ListBuffer[Double],
                   skewness: ListBuffer[Double],
                   kurtosis: ListBuffer[Double]) {
    def + (that: Stats) = addStats(this, that)
    def calcBcaCI(conf: Array[Double], t0: Stats) = StatsCI(this.n,
      bcaCI(conf, t0.mean.head, this.mean.toArray),
      bcaCI(conf, t0.variance.head, this.variance.toArray),
      bcaCI(conf, t0.skewness.head, this.skewness.toArray),
      bcaCI(conf, t0.kurtosis.head, this.kurtosis.toArray))
  }
  protected[benford] def calcStats(x: Moments): Stats = {
    val xn = x.normalize
    val m1p2 = pow(xn.m1,2)
    val m1p3 = pow(xn.m1,3)
    val m1p4 = pow(xn.m1,4)
    val m2p2 = pow(xn.m2,2)
    val variance = xn.m2 - m1p2
    Stats (x.n,
      ListBuffer(xn.m1),
      ListBuffer(variance),
      ListBuffer((xn.m3 - 3*xn.m2*xn.m1 + 2*m1p3) / pow(variance, 1.5)),
      ListBuffer((xn.m4 - 4*xn.m3*xn.m1 - 3*m2p2 + 12*xn.m2*m1p2 - 6*m1p4) / pow(variance, 2))
    )
  }
  protected[benford] def addStats(x: Stats, y: Stats) = {
    assert(x.n == y.n)
    Stats(x.n, x.mean ++ y.mean,
      x.variance ++ y.variance,
      x.skewness ++ y.skewness,
      x.kurtosis ++ y.kurtosis)
  }

  protected[benford] case class Regs(n: Double,
                  pearson: ListBuffer[Double],
                  alpha0: ListBuffer[Double],
                  alpha1: ListBuffer[Double],
                  beta0: ListBuffer[Double],
                  beta1: ListBuffer[Double]) {
    def + (that: Regs) = addRegs(this, that)
    def calcBcaCI(conf: Array[Double], t0: Regs) = RegsCI(this.n,
      bcaCI(conf, if (t0.pearson.isEmpty) 0.0 else t0.pearson.head, this.pearson.toArray),
      bcaCI(conf, if (t0.alpha0.isEmpty) 0.0 else t0.alpha0.head, this.alpha0.toArray),
      bcaCI(conf, if (t0.alpha1.isEmpty) 0.0 else t0.alpha1.head, this.alpha1.toArray),
      bcaCI(conf, if (t0.beta0.isEmpty) 0.0 else t0.beta0.head, this.beta0.toArray),
      bcaCI(conf, if (t0.beta1.isEmpty) 0.0 else t0.beta1.head, this.beta1.toArray))
  }
  protected[benford] def calcRegs(d1d2: Moments, d1: Moments, d2: Moments): Regs = {
    assert(d1d2.n == d1.n)
    assert(d1d2.n == d2.n)
    val d1d2n = d1d2.normalize
    val d1n = d1.normalize
    val d2n = d2.normalize
    val pearson = (d1d2n.prod - d1n.m1 * d2n.m1) / (sqrt(d1n.m2 - pow(d1n.m1, 2)) * sqrt(d2n.m2 - pow(d2n.m1, 2)))
    return if (d1d2.n >= 1000 ) {
      val varianced2 = d2n.m2 - pow(d2n.m1, 2)
      val alpha1 = if (varianced2 > 0) (d1d2n.prod - d1n.m1 * d2n.m1) / varianced2 else Inf
      val alpha0 = if (varianced2 > 0) d1n.m1 - alpha1 * d2n.m1 else Inf
      val varianced1 = d1n.m2 - pow(d1n.m1, 2)
      val beta1 = if (varianced1 > 0) (d1d2n.prod - d1n.m1 * d2n.m1) / varianced1 else Inf
      val beta0 = if (varianced1 > 0) d2n.m1 - beta1 * d1n.m1 else Inf
      Regs(d1d2.n, ListBuffer(pearson), ListBuffer(alpha0), ListBuffer(alpha1), ListBuffer(beta0), ListBuffer(beta1))
    } else {
      Regs(d1d2.n, ListBuffer(pearson), ListBuffer(), ListBuffer(), ListBuffer(), ListBuffer())
    }
  }
  protected[benford] def addRegs(x: Regs, y: Regs) = {
    assert(x.n == y.n)
    Regs(x.n,
      x.pearson ++ y.pearson,
      x.alpha0 ++ y.alpha0,
      x.alpha1 ++ y.alpha1,
      x.beta0 ++ y.beta0,
      x.beta1 ++ y.beta1)
  }

  case class StatsDigits(d1d2: Stats, d1: Stats, d2: Stats, r: Regs) {
    def + (that: StatsDigits) = addStatsDigits(this, that)
    def calcBcaCI(conf: Array[Double], t0: StatsDigits) =
      CIDigits(this.d1d2.calcBcaCI(conf, t0.d1d2), this.d1.calcBcaCI(conf, t0.d1), this.d2.calcBcaCI(conf, t0.d2), this.r.calcBcaCI(conf, t0.r))
  }
  protected[benford] def calcStatsDigits(x: MomentsDigits) = {
    val statsD1D2 = calcStats(x.d1d2)
    val statsD1 = calcStats(x.d1)
    val statsD2 = calcStats(x.d2)
    val statsR = calcRegs(x.d1d2, x.d1, x.d2)
    StatsDigits(statsD1D2, statsD1, statsD2, statsR)
  }
  protected[benford] def addStatsDigits(x: StatsDigits, y: StatsDigits) = StatsDigits(x.d1d2 + y.d1d2, x.d1 + y.d1, x.d2 + y.d2, x.r + y.r)

  protected[benford] case class OverlapsByLevel(idxLevel: Long, depth: Int, overlap: OverlapDigits, contain: ContainDigits) {
    def calcResults: ResultsByLevel = {
      assert(overlap.n == contain.n)
      val statsOCD1 = findStatsResults(overlap.d1, contain.d1, 0.99)
      val statsOCD2 = findStatsResults(overlap.d2, contain.d2, 0.99)
      val statsOCD1D2 = findStatsResults(overlap.d1d2, contain.d1d2, 0.99)
      val regsOC = findRegsResults(overlap.r, contain.r, 0.975)

      val regCount = regsOC.count
      val doubleDigitsCount = statsOCD1D2.count
      val singleDigitsCount = statsOCD1.count + statsOCD2.count

      val paramFinal = if (overlap.n >= 1000 && doubleDigitsCount == 4) 1 else if (singleDigitsCount == 8) 1 else if (regsOC.pearson) 1 else 0
      val regFinal = if (overlap.n < 1000 || (regCount > 0 && regCount < 4)) 0 else if (regCount == 4) 1 else -1
      ResultsByLevel(idxLevel, depth, Results(overlap.n, paramFinal, regFinal, statsOCD1D2, statsOCD1, statsOCD2, regsOC))
    }
  }

  protected[benford] case class CIDigits(d1d2: StatsCI, d1: StatsCI, d2: StatsCI, r: RegsCI) {
    assert(d1d2.n == d1.n)
    assert(d1d2.n == d2.n)
    assert(d1d2.n == r.n)
    def overlaps(that: CIDigits) =
      OverlapDigits(d1d2.n, this.d1d2.overlaps(that.d1d2), this.d1.overlaps(that.d1), this.d2.overlaps(that.d2), this.r.overlaps(that.r))
    def contains(exact: StatsDigits) =
      ContainDigits(d1d2.n, this.d1d2.contains(exact.d1d2), this.d1.contains(exact.d1), this.d2.contains(exact.d2), this.r.contains(exact.r))
  }

  case class TestResults(elements: Array[Int], stats: Array[Double], pvalues: Array[Double], rejected: Array[Int])
  case class FreqTests(count: Int, testD1D2: TestResults, testD1: TestResults, testD2: TestResults)

  private def calcZTest(elements: Array[Int], observed: Array[Double], expected: Array[Double], count: Int, alpha: Double): TestResults = {
    val freqBenf = DenseVector(expected)
    val freqSample = DenseVector(observed)
    val correction = 1.0 / (2 * count)
    val zalfa = (alpha / 2) / count
    val diff = abs(freqBenf - freqSample).map(v => v - (if ((v - correction) > 0) correction else 0))
    val zstat = diff :/ sqrt((freqBenf :* (1.0 :- freqBenf)) :/ count.toDouble)
    val pval = zstat.map(v => 1 - Gaussian(0, 1).cdf(v))
    val rejected = (DenseVector(elements)(pval :< zalfa)).toArray
    TestResults(elements, zstat.toArray, pval.toArray, rejected)
  }

  private def calcChiSquareTest(observed: Array[Double], expected: Array[Double], count: Int, alpha: Double, degrees: Int): TestResults = {
    val chistat = count.toDouble * sum(pow(DenseVector(observed) - DenseVector(expected),2) :/ DenseVector(expected))
    val pval = ChiSquared(degrees).cdf(chistat)
    TestResults(Array(0), Array(chistat), Array(pval), if (pval < alpha) Array(0) else Array())
  }

  protected[benford] case class Frequencies(count: Int, freqD1D2: Array[Double], freqD1: Array[Double], freqD2: Array[Double]) {
    def zTest = FreqTests(
      count,
      calcZTest((10 to 99).toArray, freqD1D2, BenfordProbabilitiesD1D2, count, 0.05),
      calcZTest((1 to 9).toArray, freqD1, BenfordProbabilitiesD1, count, 0.05),
      calcZTest((0 to 9).toArray, freqD2, BenfordProbabilitiesD2, count, 0.05)
    )
    def chiTest = FreqTests(
      count,
      calcChiSquareTest(freqD1D2, BenfordProbabilitiesD1D2, count, 0.05, 89),
      calcChiSquareTest(freqD1, BenfordProbabilitiesD1, count, 0.05, 8),
      calcChiSquareTest(freqD2, BenfordProbabilitiesD2, count, 0.05, 9)
    )
  }

  protected[benford] case class Level(idxLevel: Long, depth: Int, idx: Long, value: Double, d1d2: Int)

  protected[benford] def average[T](xs: Iterable[T])(implicit num: Numeric[T]):Double =
    num.toDouble(xs.sum) / xs.size

  protected[benford] def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  protected[benford] def pruneResults(json: JsValue): JsValue = {
    val regTransf = (__ \ 'results \ 'regsDiag).json.prune
    val alpha0Transf = (__ \ 'results \ 'reg \ 'alpha0).json.prune
    val alpha1Transf = (__ \ 'results \ 'reg \ 'alpha1).json.prune
    val beta0Transf = (__ \ 'results \ 'reg \ 'beta0).json.prune
    val beta1Transf = (__ \ 'results \ 'reg \ 'beta1).json.prune
    JsArray(json.as[List[JsObject]]
      .map(x => if (x.transform((__ \ 'results \ 'n).json.pick).get.as[Int] < 1000)
        x.transform(regTransf).get
          .transform(alpha0Transf).get
          .transform(alpha1Transf).get
          .transform(beta0Transf).get
          .transform(beta1Transf).get
      else x))
  }

  protected[benford] def pruneCIs(json: JsValue): JsValue = {
    val alpha0Transf = (__ \ 'CIs \ 'r \ 'alpha0).json.prune
    val alpha1Transf = (__ \ 'CIs \ 'r \ 'alpha1).json.prune
    val beta0Transf = (__ \ 'CIs \ 'r \ 'beta0).json.prune
    val beta1Transf = (__ \ 'CIs \ 'r \ 'beta1).json.prune
    JsArray(json.as[List[JsObject]]
      .map(x => if (x.transform((__ \ 'CIs \ 'r \ 'n).json.pick).get.as[Int] < 1000)
        x.transform(alpha0Transf).get
          .transform(alpha1Transf).get
          .transform(beta0Transf).get
          .transform(beta1Transf).get
      else x))
  }

  case class BasicBoot(aliasMap: Map[Long, AliasTable], aliasMapBenf: Map[Long, AliasTable], bootTableRDD: RDD[(Long, (Int, (Int, Double)))])
  case class DataByLevel(levels: Map[Long, (String, Int)], hierarchy: Map[Long, Array[Long]], freqByLevel: Array[FreqByLevel] ,dataByLevelsRDD: RDD[Level])
  case class ResultsByLevel(idxLevel: Long, depth: Int, results: Results)
  case class FreqByLevel(idxLevel: Long, freq: Frequencies)
  case class StatsCIByLevel(idxLevel: Long, depth: Int, CIs: CIDigits) {
    protected[benford] def overlaps(that: StatsCIByLevel) = {
      assert(this.idxLevel == that.idxLevel && this.depth == that.depth)
      this.CIs.overlaps(that.CIs)
    }
    protected[benford] def contains(exact: StatsDigits) = {
      this.CIs.contains(exact)
    }
  }
  case class Group(idxLevel: Long, depth: Int, name: String, children: Array[Long])
  case class JobId(id: String)

  implicit val FrequenciesWrites = new Writes[Frequencies] {
    def writes(frequency: Frequencies) = Json.obj(
      "count" -> frequency.count,
      "d1d2" -> frequency.freqD1D2.map{ case freq => "%.10f".format(freq).toDouble },
      "d1" -> frequency.freqD1.map{ case freq => "%.10f".format(freq).toDouble },
      "d2" -> frequency.freqD2.map{ case freq => "%.10f".format(freq).toDouble }
    )
  }

  implicit val FrequenciesReads: Reads[Frequencies] = (
    (JsPath \ "count").read[Int] and
    (JsPath \ "d1d2").read[Array[Double]] and
    (JsPath \ "d1").read[Array[Double]] and
    (JsPath \ "d2").read[Array[Double]]
  )(Frequencies.apply _)

  implicit val CIWrites = new Writes[CI] {
    def writes(ci: CI) = Json.obj(
      "alpha" -> ci.alpha,
      "li" -> (if (!ci.li.isInfinite) "%.10f".format(ci.li).toDouble else 0.0),
      "ui" -> (if (!ci.ui.isInfinite) "%.10f".format(ci.ui).toDouble else 0.0),
      "lower" -> (if (!ci.lower.isInfinite) "%.10f".format(ci.lower).toDouble else 0.0),
      "upper" -> (if (!ci.upper.isInfinite) "%.10f".format(ci.upper).toDouble else 0.0),
      "t0" -> (if (!ci.t0.isInfinite) "%.10f".format(ci.t0).toDouble else 0.0)
    )
  }

  implicit val CIReads: Reads[CI] = (
    (JsPath \ "alpha").read[Double] and
    (JsPath \ "li").read[Double] and
    (JsPath \ "ui").read[Double] and
    (JsPath \ "lower").read[Double] and
    (JsPath \ "upper").read[Double] and
    (JsPath \ "t0").read[Double]
  )(CI.apply _)

  implicit val StatsCIWrites: Writes[StatsCI] = (
    (JsPath \ "n").write[Double] and
    (JsPath \ "mean").write[Array[CI]] and
    (JsPath \ "variance").write[Array[CI]] and
    (JsPath \ "skewness").write[Array[CI]] and
    (JsPath \ "kurtosis").write[Array[CI]]
  )(unlift(StatsCI.unapply))

  implicit val StatsCIReads: Reads[StatsCI] = (
    (JsPath \ "n").read[Double] and
    (JsPath \ "mean").read[Array[CI]]  and
    (JsPath \ "variance").read[Array[CI]]  and
    (JsPath \ "skewness").read[Array[CI]]  and
    (JsPath \ "kurtosis").read[Array[CI]]
  )(StatsCI.apply _)

  implicit val RegsCIWrites: Writes[RegsCI] = (
    (JsPath \ "n").write[Double] and
    (JsPath \ "pearson").write[Array[CI]] and
    (JsPath \ "alpha0").write[Array[CI]] and
    (JsPath \ "alpha1").write[Array[CI]] and
    (JsPath \ "beta0").write[Array[CI]] and
    (JsPath \ "beta1").write[Array[CI]]
  )(unlift(RegsCI.unapply))

  implicit val RegsCIReads: Reads[RegsCI] = (
    (JsPath \ "n").read[Double] and
    (JsPath \ "pearson").read[Array[CI]]  and
    (JsPath \ "alpha0").read[Array[CI]]  and
    (JsPath \ "alpha1").read[Array[CI]]  and
    (JsPath \ "beta0").read[Array[CI]]  and
    (JsPath \ "beta1").read[Array[CI]]
  )(RegsCI.apply _)

  implicit val CIDigitsWrites: Writes[CIDigits] = (
    (JsPath \ "d1d2").write[StatsCI] and
    (JsPath \ "d1").write[StatsCI] and
    (JsPath \ "d2").write[StatsCI] and
    (JsPath \ "r").write[RegsCI]
  )(unlift(CIDigits.unapply))

  implicit val CIDigitsReads: Reads[CIDigits] = (
    (JsPath \ "d1d2").read[StatsCI] and
    (JsPath \ "d1").read[StatsCI]  and
    (JsPath \ "d2").read[StatsCI]  and
    (JsPath \ "r").read[RegsCI]
  )(CIDigits.apply _)

  implicit val StatsCIByLevelWrites: Writes[StatsCIByLevel] = (
    (JsPath \ "id").write[Long] and
    (JsPath \ "level").write[Int] and
    (JsPath \ "CIs").write[CIDigits]
  )(unlift(StatsCIByLevel.unapply))

  implicit val StatsCIByLevelReads: Reads[StatsCIByLevel] = (
    (JsPath \ "id").read[Long] and
    (JsPath \ "level").read[Int]  and
    (JsPath \ "CIs").read[CIDigits]
  )(StatsCIByLevel.apply _)

  implicit val OverlapContainWrites: Writes[OverlapContain] = (
    (JsPath \ "overlaps").write[Boolean] and
    (JsPath \ "contains").write[Boolean]
  )(unlift(OverlapContain.unapply))

  implicit val OverlapContainReads: Reads[OverlapContain] = (
    (JsPath \ "overlaps").read[Boolean] and
    (JsPath \ "contains").read[Boolean]
  )(OverlapContain.apply _)

  implicit val StatsOCWrites: Writes[StatsOC] = (
    (JsPath \ "mean").write[OverlapContain] and
    (JsPath \ "variance").write[OverlapContain] and
    (JsPath \ "skewness").write[OverlapContain] and
    (JsPath \ "kurtosis").write[OverlapContain]
  )(unlift(StatsOC.unapply))

  implicit val StatsOCReads: Reads[StatsOC] = (
    (JsPath \ "mean").read[OverlapContain] and
    (JsPath \ "variance").read[OverlapContain] and
    (JsPath \ "skewness").read[OverlapContain] and
    (JsPath \ "kurtosis").read[OverlapContain]
  )(StatsOC.apply _)

  implicit val RegsOCWrites: Writes[RegsOC] = (
    (JsPath \ "pearson").write[OverlapContain] and
    (JsPath \ "alpha0").write[OverlapContain] and
    (JsPath \ "alpha1").write[OverlapContain] and
    (JsPath \ "beta0").write[OverlapContain] and
    (JsPath \ "beta1").write[OverlapContain]
  )(unlift(RegsOC.unapply))

  implicit val RegsOCReads: Reads[RegsOC] = (
    (JsPath \ "pearson").read[OverlapContain] and
    (JsPath \ "alpha0").read[OverlapContain] and
    (JsPath \ "alpha1").read[OverlapContain] and
    (JsPath \ "beta0").read[OverlapContain] and
    (JsPath \ "beta1").read[OverlapContain]
  )(RegsOC.apply _)

  implicit val ResultsWrites: Writes[Results] = (
    (JsPath \ "n").write[Double] and
    (JsPath \ "statsDiag").write[Int] and
    (JsPath \ "regsDiag").write[Int] and
    (JsPath \ "d1d2").write[StatsOC] and
    (JsPath \ "d1").write[StatsOC] and
    (JsPath \ "d2").write[StatsOC] and
    (JsPath \ "reg").write[RegsOC]
  )(unlift(Results.unapply))

  implicit val ResultsReads: Reads[Results] = (
    (JsPath \ "n").read[Double] and
    (JsPath \ "statsDiag").read[Int] and
    (JsPath \ "regsDiag").read[Int] and
    (JsPath \ "d1d2").read[StatsOC] and
    (JsPath \ "d1").read[StatsOC] and
    (JsPath \ "d2").read[StatsOC] and
    (JsPath \ "reg").read[RegsOC]
  )(Results.apply _)

  implicit val ResultsByLevelWrites: Writes[ResultsByLevel] = (
    (JsPath \ "id").write[Long] and
    (JsPath \ "level").write[Int] and
    (JsPath \ "results").write[Results]
  )(unlift(ResultsByLevel.unapply))

  implicit val ResultsByLevelReads: Reads[ResultsByLevel] = (
    (JsPath \ "id").read[Long] and
    (JsPath \ "level").read[Int]  and
    (JsPath \ "results").read[Results]
  )(ResultsByLevel.apply _)

  implicit val GroupWrites: Writes[Group] = (
    (JsPath \ "id").write[Long] and
    (JsPath \ "level").write[Int] and
    (JsPath \ "name").write[String] and
    (JsPath \ "children").write[Array[Long]]
  )(unlift(Group.unapply))

  implicit val GroupReads: Reads[Group] = (
    (JsPath \ "id").read[Long] and
    (JsPath \ "level").read[Int] and
    (JsPath \ "name").read[String] and
    (JsPath \ "children").read[Array[Long]]
  )(Group.apply _)

  implicit val StatsWrites: Writes[Stats] = (
    (JsPath \ "n").write[Double] and
    (JsPath \ "mean").write[ListBuffer[Double]] and
    (JsPath \ "variance").write[ListBuffer[Double]] and
    (JsPath \ "skewness").write[ListBuffer[Double]] and
    (JsPath \ "kurtosis").write[ListBuffer[Double]]
  )(unlift(Stats.unapply))

  implicit val StatsReads: Reads[Stats] = (
    (JsPath \ "n").read[Double] and
    (JsPath \ "mean").read[ListBuffer[Double]] and
    (JsPath \ "variance").read[ListBuffer[Double]] and
    (JsPath \ "skewness").read[ListBuffer[Double]] and
    (JsPath \ "kurtosis").read[ListBuffer[Double]]
  )(Stats.apply _)

  implicit val RegsWrites: Writes[Regs] = (
    (JsPath \ "n").write[Double] and
    (JsPath \ "pearson").write[ListBuffer[Double]] and
    (JsPath \ "alpha0").write[ListBuffer[Double]] and
    (JsPath \ "alpha1").write[ListBuffer[Double]] and
    (JsPath \ "beta0").write[ListBuffer[Double]] and
    (JsPath \ "beta1").write[ListBuffer[Double]]
  )(unlift(Regs.unapply))

  implicit val RegsReads: Reads[Regs] = (
    (JsPath \ "n").read[Double] and
    (JsPath \ "pearson").read[ListBuffer[Double]] and
    (JsPath \ "alpha0").read[ListBuffer[Double]] and
    (JsPath \ "alpha1").read[ListBuffer[Double]] and
    (JsPath \ "beta0").read[ListBuffer[Double]] and
    (JsPath \ "beta1").read[ListBuffer[Double]]
  )(Regs.apply _)

  implicit val StatsDigitsWrites: Writes[StatsDigits] = (
    (JsPath \ "d1d2").write[Stats] and
    (JsPath \ "d1").write[Stats] and
    (JsPath \ "d2").write[Stats] and
    (JsPath \ "r").write[Regs]
  )(unlift(StatsDigits.unapply))

  implicit val StatsDigitsReads: Reads[StatsDigits] = (
    (JsPath \ "d1d2").read[Stats] and
    (JsPath \ "d1").read[Stats] and
    (JsPath \ "d2").read[Stats] and
    (JsPath \ "r").read[Regs]
  )(StatsDigits.apply _)

  implicit val TestResultsWrites: Writes[TestResults] = (
    (JsPath \ "elements").write[Array[Int]] and
    (JsPath \ "stats").write[Array[Double]] and
    (JsPath \ "pvalues").write[Array[Double]] and
    (JsPath \ "rejected").write[Array[Int]]
  )(unlift(TestResults.unapply))

  implicit val testResultsReads: Reads[TestResults] = (
    (JsPath \ "elements").read[Array[Int]] and
    (JsPath \ "stats").read[Array[Double]] and
    (JsPath \ "pvalues").read[Array[Double]] and
    (JsPath \ "rejected").read[Array[Int]]
  )(TestResults.apply _)

  implicit val FreqTestsWrites: Writes[FreqTests] = (
    (JsPath \ "count").write[Int] and
    (JsPath \ "testD1D2").write[TestResults] and
    (JsPath \ "testD1").write[TestResults] and
    (JsPath \ "testD2").write[TestResults]
  )(unlift(FreqTests.unapply))

  implicit val FreqTestsReads: Reads[FreqTests] = (
    (JsPath \ "count").read[Int] and
    (JsPath \ "testD1D2").read[TestResults] and
    (JsPath \ "testD1").read[TestResults] and
    (JsPath \ "testD2").read[TestResults]
  )(FreqTests.apply _)

}
