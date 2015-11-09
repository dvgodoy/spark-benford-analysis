package com.dvgodoy.spark.benford.distributions

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, FunSuite}

abstract class SparkSuite extends FunSuite with MustMatchers with BeforeAndAfterAll {
  protected var sc: SparkContext = _
  protected var conf: SparkConf = _

  protected def loadAndProcessData(): Unit

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    conf = new SparkConf(false)
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
