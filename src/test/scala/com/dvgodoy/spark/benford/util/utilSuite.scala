package com.dvgodoy.spark.benford.util

import org.scalatest.{BeforeAndAfterAll, FunSuite, MustMatchers}

class utilSuite extends FunSuite with MustMatchers with BeforeAndAfterAll {
  val tolerance = 1e-10

  test("Finding first and seconds significant digits works") {
    findD1D2(0.032) must be(32)
    findD1D2(0.03) must be(30)
    findD1D2(1.2e-6) must be(12)
    findD1D2(7.35) must be(73)
    findD1D2(7) must be(70)
    findD1D2(345) must be(34)
    findD1D2(4.8e9) must be(48)
  }

  test("Moments calculation works") {
    val moment = calcMoments(23,0.1).d1d2
    moment.n must be(0.1 +- tolerance)
    moment.m1 must be(2.3 +- tolerance)
    moment.m2 must be(52.9 +- tolerance)
    moment.m3 must be(1216.7 +- tolerance)
    moment.m4 must be(27984.1 +- tolerance)
    moment.prod must be(0.6 +- tolerance)
  }

  test("Moments normalization works") {
    val moment = calcMoments(23,0.1).d1d2.normalize
    moment.n must be(1.0 +- tolerance)
    moment.m1 must be(23.0 +- tolerance)
    moment.m2 must be(529.0 +- tolerance)
    moment.m3 must be(12167.0 +- tolerance)
    moment.m4 must be(279841.0 +- tolerance)
    moment.prod must be(6.0 +- tolerance)
  }

  test("Adding moments works") {
    val moment1 = calcMoments(23,0.1)
    val moment2 = calcMoments(12,0.7)
    val sum = (moment1 + moment2).d1d2
    sum.n must be(0.8 +- tolerance)
    sum.m1 must be(10.7 +- tolerance)
    sum.m2 must be(153.7 +- tolerance)
    sum.m3 must be(2426.3 +- tolerance)
    sum.m4 must be(42499.3 +- tolerance)
    sum.prod must be(2.0 +- tolerance)
  }

  test("Stats calculation works") {
    val moment1 = calcMoments(23,0.1)
    val moment2 = calcMoments(12,0.7)
    val stat = calcStats((moment1 + moment2).d1d2)
    stat.n must be(0.8 +- tolerance)
    stat.mean.head must be(13.375 +- tolerance)
    stat.variance.head must be(13.234375 +- tolerance)
    stat.skewness.head must be(2.2677868380 +- tolerance)
    stat.kurtosis.head must be(3.1428571428 +- tolerance)
  }

}