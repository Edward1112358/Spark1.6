package com.ibm.PeopleInfo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2017-4-3.
  */
object PeopleInfoCalculator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PeopleInfoCalculator")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    val peopleRDD = sc.textFile("file:///home/aisinobi/tmpEdward/samplePeopleInfo.txt").map(line => line.split(" "))
    val people = peopleRDD.persist(StorageLevel.MEMORY_ONLY) // people.cache()

    val mPeople = people.filter(x => x(1).equals("M"))
    val fPeople = people.filter(x => x(1).equals("F"))

    val mCount = mPeople.count()
    val fCount = fPeople.count()
    System.out.println("################" + mCount)

    // 注意 要将身高转换为数值类型，否则排序是按字符顺序进行排序
    // val mHeighest = mPeople.sortBy(x => x(2).toDouble, false).take(1)(0)(2)
    val mHeighest = mPeople.map(x => x(2).toDouble).sortBy(x => x, false).take(1)(0)
  }
}
