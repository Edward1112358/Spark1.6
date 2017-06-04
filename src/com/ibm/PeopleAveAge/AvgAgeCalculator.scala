package com.ibm.PeopleAveAge

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2017-3-31.
  */
object AvgAgeCalculator {

  case class People(peopleId: String, age: Int)

  def main(args: Array[String]): Unit = {
    /*if (args.length < 1) {
      println("Usage:AvgAgeCalculator dataFile")
      System.exit(1)
    }*/

    val conf = new SparkConf().setAppName("AveAgeCalculator")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val dataFile = sc.textFile("/user/hive/warehouse/tmpEdward/sampleAgeData.txt")
    val peopleRDD = dataFile.map(line => line.split(" ")).map(line => People(line(0), line(1).toInt))
    //val peopleRDD = sc.textFile("file:///home/aisinobi/tmpEdward/sampleAgeData.txt").map(line => line.split(" ")).map(line => People(line(0), line(1).toInt))
    val peopleDF = peopleRDD.toDF("peopleId", "age")
    peopleDF.registerTempTable("people")

    // 统计一个 1000 万人口的所有人的平均年龄
    // 方法一
    sqlContext.sql("select avg(age) from people").show
    // 方法二
    peopleDF.agg(avg("age")).show

    // 方法三
    val count = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1)) // 不用再.map(x=>x(1))
    val totalAge = ageData.map(age => age.toLong).reduce((a, b) => a + b) // 转换为Double后a+b才是数值相加
    // 或 val totalAge = ageData.map(age => age.toLong).sum()
    val avgAge: Double = totalAge.toDouble / count.toDouble // 注意要toDouble 否则1/3=0
  }
}
