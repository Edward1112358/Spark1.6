package com.DataFrame

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Edward on 2016-12-22.
  */
object DataFrameTest {
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  // For implicit conversions like converting RDDs to DataFrames
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {
    val dataList = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4))
    val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    data.foreach(println)
    data.foreachPartition(println)

    val a = sc.parallelize(Array("a","b","c","a"))
    val b = a.map(x=>(x,1))
    val c = b.reduceByKey((t, tt)=>t + 3)
    c.foreach(x=>println(x._2))
  }
}
