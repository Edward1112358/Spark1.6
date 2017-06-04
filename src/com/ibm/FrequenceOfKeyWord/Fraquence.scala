package com.ibm.FrequenceOfKeyWord

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2017-4-8.
  */
object Fraquence {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Fraquence")
    val sc = new SparkContext(conf)
    val fileRdd = sc.textFile("/home/aisinobi/tmpEdward/KeyWordFile.txt", 5)

    val fraquence = fileRdd.map(line => (line.toLowerCase(), 1)).reduceByKey(_ + _).sortBy(x => x._2, false)
    fraquence.foreach(println)
  }
}
