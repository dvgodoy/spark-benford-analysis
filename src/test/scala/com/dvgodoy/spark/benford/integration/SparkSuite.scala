package com.dvgodoy.spark.benford.integration

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, MustMatchers}

abstract class SparkSuite extends FunSuite with MustMatchers with BeforeAndAfterAll {
  protected var sc: SparkContext = _

  protected def loadAndProcessData(): Unit

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    lazy val conf = new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
    sc = SparkContext.getOrCreate(conf)
    loadAndProcessData()
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
    } finally {
      super.afterAll()
    }
  }

}
