package com.dvgodoy.spark.benford

import com.dvgodoy.spark.benford.util._

/**
 * Created by dvgodoy on 30/10/15.
 */
package object constants {
  val probabilitiesD1 = (1 to 9).map(x => math.log10(1.0 + 1.0 / x))
  val probabilitiesD2 = (10 to 19).map(x => List.range(x, 100, 10).map(x => math.log10(1.0 + 1.0 / x)).sum)
  val probabilitiesD1D2 = (10 to 99).map(x => math.log10(1.0 + 1.0 / x))

  val momentsD1 = (1 to 9, probabilitiesD1).zipped.map((n, p) => calcMoments(n, p)).reduce(addMoments)
  val momentsD2 = (0 to 9, probabilitiesD2).zipped.map((n, p) => calcMoments(n, p)).reduce(addMoments)
  val momentsD1D2 = (10 to 99, probabilitiesD1D2).zipped.map((n, p) => calcMoments(n, p)).reduce(addMoments)

  val statsD1 =calcStats(momentsD1)
  val statsD2 =calcStats(momentsD2)
  val statsD1D2 =calcStats(momentsD1D2)
}
