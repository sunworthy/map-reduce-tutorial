package com.josh.mr.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class WordCountTest {

  private def getSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    new SparkContext(sparkConf)
  }

  @Test
  def wordCount1(): Unit = {
    val sc = getSparkContext

    // 1. read lines
    val lines: RDD[String] = sc.textFile("data/txt/")

    // 2. split each line
    // x -> x.split
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(e => e)

    // 3. group by
    val wordMap = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    val array: Array[(String, Int)] = wordMap.collect()

    array.foreach(println)

    // SparkUI: http://s1.local:4040
    Thread.sleep(3000)
    sc.stop()
  }

  @Test
  def wordCount2(): Unit = {
    val sc = getSparkContext

    // 1. read lines
    val lines: RDD[String] = sc.textFile("data/txt/")

    // 2. split each line
    // x -> x.split
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. group by
    val wordMap = words.map {
      word => (word, 1)
    }
    val wordGroup = wordMap.groupBy(t => t._1)

    val wordCount = wordGroup.map {
      case (_, list) =>
        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
    }
    wordCount.foreach(println)

    sc.stop()
  }

  @Test
  def wordCount3(): Unit = {
    val sc = getSparkContext

    // 1. read lines
    val lines: RDD[String] = sc.textFile("data/txt/")
    // 2. split each line
    // x -> x.split
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. group by
    val wordToOne = words.map {
      word => (word, 1)
    }

    // group by same key
    //    wordToOne.reduceByKey((x, y) => {x + y})
    //    wordToOne.reduceByKey((x, y) => x + y)
    val wordCount = wordToOne.reduceByKey(_ + _).collect()

    wordCount.foreach(println)

    sc.stop()
  }

  @Test
  def wordCount4(): Unit = {
    val sc = getSparkContext
    sc.textFile("data/txt/").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
  }
}
