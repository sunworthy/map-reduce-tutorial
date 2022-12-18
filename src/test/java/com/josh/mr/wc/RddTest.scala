package com.josh.mr.wc

import org.apache.spark.rdd.RDD
import org.junit.Test

import scala.reflect.io.File

class RddTest extends BaseSparkTest {

  @Test
  def test1(): Unit = {
    val sc = getSparkContext
    val seq = Seq[Int](1, 2, 3, 4,5)
    // memory rdd
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    val dir = "out"
    File.apply(dir).deleteRecursively()
    rdd.saveAsTextFile(dir)
  }

  @Test
  def test2(): Unit = {
    // rdd from file system(or hdfs)
    val rdd: RDD[String] = getSparkContext.textFile("data/txt")
    rdd.collect().foreach(println)

    val dir = "out"
    File.apply(dir).deleteRecursively()
    rdd.saveAsTextFile(dir)
  }


  @Test
  def test3(): Unit = {
    // with file path
    val rdd: RDD[(String, String)] = getSparkContext.wholeTextFiles("data/txt")
    rdd.collect().foreach(println)
  }
}
