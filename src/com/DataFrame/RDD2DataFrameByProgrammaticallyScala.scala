package com.DataFrame

/**
  * Created by Edward on 2017-5-30.
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

class RDD2DataFrameByProgrammaticallyScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDD2DataFrameByProgrammaticallyScala") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("C://Users//DS01//Desktop//persons.txt")
    val schemaString = "name age"
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType, StructField, StringType};
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.registerTempTable("people")
    val results = sqlContext.sql("select name from people")
    results.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}