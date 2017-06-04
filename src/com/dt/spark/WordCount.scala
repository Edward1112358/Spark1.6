package com.dt.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2016-10-26.
  */
// 运行会报错  Failed to locate the winutils binary in the hadoop binary path
// 因为版本是Spark prebuild with Hadoop
// 但不影响

object WordCount {
  def main(args: Array[String]): Unit = {
    //Spark程序运行时的配置信息
    val conf = new SparkConf()
    conf.setAppName("My First Spark")
    conf.setMaster("local")
    // 集群方式：conf.setMaster("spark://Master:7077") // 建议在spark-submit中再设置
    // lambda表达式方式：val conf = new SparkConf().setAppName("xxx").setMaster("xxx")

    //Spark程序所有功能的唯一入口 无论是Scala、Java、Python、R都必须有一个SparkContext
    //核心作用：初始化Spark应用程序 包括DAGScheduler、TaskScheduler、SchedulerBackend
    //同时还会负责Spark程序向Master注册程序
    val sc = new SparkContext(conf)
    // Java中是 val sc = new JavaSparkContext(conf)

    //根据具体的数据来源 例如 HDFS、Hbase、Local FS、DB、S3等 通过SparkContext创建RDD
    //RDD的创建基本有三种方式：
    //根据外部数据来源 例如HDFS
    //根据Scala集合
    //其他RDD操作
    //数据会被RDD划分为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
    val lines = sc.textFile("F://1.txt", 1)
    //并行度为1
    //val lines = sc.textFile("hdfs://Master:9000/...") //集群
    //建议直接写/... 否则放在不同集群，前边的Master:9000还要改
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _) //对相同的Key 进行Value的累加 等同于 reduceByKey((x,y)=> x + y)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + ":" + wordNumberPair._2))
    // 集群 需要加上collect
    // wordCounts.collect.foreach(wordNumberPair => println(wordNumberPair._1 + ":" + wordNumberPair._2))

    // 广告点击排名
    // val wordCounts = pairs.reduceByKey(_+_).map(pair=>(pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2, pair._1))

    sc.stop()
  }
}
