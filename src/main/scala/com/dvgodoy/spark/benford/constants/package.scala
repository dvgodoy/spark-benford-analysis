package com.dvgodoy.spark.benford

import com.dvgodoy.spark.benford.util._

package object constants {
  val BenfordProbabilitiesD1 = (1 to 9).map(x => math.log10(1.0 + 1.0 / x)).toArray
  val BenfordProbabilitiesD2 = (10 to 19).map(x => List.range(x, 100, 10).map(x => math.log10(1.0 + 1.0 / x)).sum).toArray
  val BenfordProbabilitiesD1D2 = (10 to 99).map(x => math.log10(1.0 + 1.0 / x)).toArray

  private val BenfordMoments = (10 to 99, BenfordProbabilitiesD1D2.map(_*1000)).zipped.map((n, p) => calcMoments(n, p)).reduce(_ + _)
  //val BenfordMomentsD1D2 = Moments(1000, 1000*BenfordMoments.d1d2.m1, 1000*BenfordMoments.d1d2.m2, 1000*BenfordMoments.d1d2.m3, 1000*BenfordMoments.d1d2.m4, 1000*BenfordMoments.d1d2.prod)
  //val BenfordMomentsD1 = Moments(1000, 1000*BenfordMoments.d1.m1, 1000*BenfordMoments.d1.m2, 1000*BenfordMoments.d1.m3, 1000*BenfordMoments.d1.m4, 1000*BenfordMoments.d1.prod)
  //val BenfordMomentsD2 = Moments(1000, 1000*BenfordMoments.d2.m1, 1000*BenfordMoments.d2.m2, 1000*BenfordMoments.d2.m3, 1000*BenfordMoments.d2.m4, 1000*BenfordMoments.d2.prod)
  val BenfordMomentsD1D2 = BenfordMoments.d1d2
  val BenfordMomentsD1 = BenfordMoments.d1
  val BenfordMomentsD2 = BenfordMoments.d2

  private val BenfordStats = calcStatsDigits(BenfordMoments)
  val BenfordStatsD1D2 = BenfordStats.d1d2
  val BenfordStatsD1 = BenfordStats.d1
  val BenfordStatsD2 = BenfordStats.d2
  val BenfordStatsR = calcRegs(BenfordMomentsD1D2, BenfordMomentsD1, BenfordMomentsD2)
  val BenfordStatsDigits = StatsDigits(BenfordStatsD1D2, BenfordStatsD1, BenfordStatsD2, BenfordStatsR)
}

