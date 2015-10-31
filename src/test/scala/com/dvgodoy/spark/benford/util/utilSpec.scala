package com.dvgodoy.spark.benford.util

import org.scalatestplus.play.PlaySpec
import play.api.Play

/**
 * Created by dvgodoy on 31/10/15.
 */
class utilSpec extends PlaySpec {
  "findD1D2" must {
    "work with numbers below 1.0" in {
      findD1D2(0.032) must be(32)
      findD1D2(0.03) must be(30)
      findD1D2(1.2e-6) must be(12)
    }
    "work with numbers between 1.0 and 10.0" in {
      findD1D2(7.35) must be(73)
      findD1D2(7) must be(70)
    }
    "wortk numbers above 10.0" in {
      findD1D2(345) must be(34)
      findD1D2(4.8e9) must be(48)
    }
  }

  "calcMoments" must {
    val tolerance = 1e-10
    val moment = calcMoments(23,0.1)
    "return n" in {
      moment.n must be(0.1 +- tolerance)
    }
    "calculate first moment" in {
      moment.m1 must be(2.3 +- tolerance)
    }
    "calculate second moment" in {
      moment.m2 must be(52.9 +- tolerance)
    }
    "calculate third moment" in {
      moment.m3 must be(1216.7 +- tolerance)
    }
    "calculate fourth moment" in {
      moment.m4 must be(27984.1 +- tolerance)
    }
    "calculate product" in {
      moment.prod must be(0.6 +- tolerance)
    }
  }

  "Moments.normalize" must {
    val tolerance = 1e-10
    val moment = calcMoments(23,0.1).normalize
    "return n" in {
      moment.n must be(1.0 +- tolerance)
    }
    "calculate first moment" in {
      moment.m1 must be(23.0 +- tolerance)
    }
    "calculate second moment" in {
      moment.m2 must be(529.0 +- tolerance)
    }
    "calculate third moment" in {
      moment.m3 must be(12167.0 +- tolerance)
    }
    "calculate fourth moment" in {
      moment.m4 must be(279841.0 +- tolerance)
    }
    "calculate product" in {
      moment.prod must be(6.0 +- tolerance)
    }
  }

  "addMoments" must {
    val tolerance = 1e-10
    val moment1 = calcMoments(23,0.1)
    val moment2 = calcMoments(12,0.7)
    val sum = moment1 + moment2
    "return n" in {
      sum.n must be(0.8 +- tolerance)
    }
    "first moment" in {
      sum.m1 must be(10.7 +- tolerance)
    }
    "second moment" in {
      sum.m2 must be(153.7 +- tolerance)
    }
    "third moment" in {
      sum.m3 must be(2426.3 +- tolerance)
    }
    "fourth moment" in {
      sum.m4 must be(42499.3 +- tolerance)
    }
    "product" in {
      sum.prod must be(2.0 +- tolerance)
    }
  }

  "calcStats" must {
    val tolerance = 1e-10
    val moment1 = calcMoments(23,0.1)
    val moment2 = calcMoments(12,0.7)
    val stat = calcStats(moment1 + moment2)
    "return n" in {
      stat.n must be(1.0 +- tolerance)
    }
    "calculate mean" in {
      stat.mean must be(13.375 +- tolerance)
    }
    "calculate variance" in {
      stat.variance must be(13.234375 +- tolerance)
    }
    "calculate skewness" in {
      stat.skewness must be(2.2677868380 +- tolerance)
    }
    "calculate kurtosis" in {
      stat.kurtosis must be(3.1428571428 +- tolerance)
    }
    "calculate pearson" in {
      stat.pearson must be(2.5 +- tolerance)
    }
  }

}
