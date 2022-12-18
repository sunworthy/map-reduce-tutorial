package com.josh.mr.wc

import org.apache.spark.{SparkConf, SparkContext}

class BaseSparkTest {
  def getSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("junitTest")
    sparkConf.set("spark.default.parallelism", "5")
    new SparkContext(sparkConf)
  }
}
