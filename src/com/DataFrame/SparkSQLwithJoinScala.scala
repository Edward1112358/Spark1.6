package com.DataFrame

/**
  * Created by Edward on 2017-6-2.
  */

import java.util

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLwithJoinScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQLwithJoin")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val peoplesDF = sqlContext.read.json("F:\\peopleScores.json")
    /*
      {"name":"Michael"}
      {"name":"Lily", "score":"a"}
      {"name":"Andy", "score":100}
      {"name":"Justin", "score":87}
     */

    // 方法零
    peoplesDF.filter(peoplesDF("score") > 90).select(peoplesDF("name")).show

    // 方法一
    peoplesDF.registerTempTable("peopleScores")
    val excellentScoresDF = sqlContext.sql("select name,score from peopleScores where score > 90")
    val execellentScoresNameList = excellentScoresDF.map(x => x.getAs("name").toString)
    execellentScoresNameList.foreach(println)

    // 方法二
    //如果数据不规整，需要处理一些特殊情况，否则会报错
    // 过滤掉分数为空的
    // 过滤掉分数为非数字的
    // val t = peoplesDF.rdd.filter(x => Integer.valueOf(x.getAs("score").toString) > 90).map(x => (x.getAs("name").toString))
    val t = peoplesDF.rdd.filter(x => (x.getAs("score") != null)).filter(x => {
      val str = x.getAs("score").toString
      var flagDigit = true
      for (i <- str) {
        if (i.isDigit == false) flagDigit = false
      }
      flagDigit
    }).filter(x => Integer.valueOf(x.getAs("score").toString) > 90).map(x => (x.getAs("name").toString))
    t.foreach(println)

    val excellentScoresNameList = t.collect()

    var peopleInformations = Seq[String]()
    peopleInformations = peopleInformations :+ "{\"name\":\"Michael\", \"age\":20}"
    peopleInformations = peopleInformations :+ "{\"name\":\"Andy\", \"age\":17}"
    peopleInformations = peopleInformations :+ "{\"name\":\"Justin\", \"age\":19}"

    val peopleInformationsRDD = sc.parallelize(peopleInformations)
    val peopleInfomationDF = sqlContext.read.json(peopleInformationsRDD)
    peopleInfomationDF.registerTempTable("peopleInformations")

    var sqlText = "select name, age from peopleInformations where name in (";
    for (i <- excellentScoresNameList) {
      sqlText += "'" + i + "',"
    }
    sqlText = sqlText.substring(0, sqlText.length - 1)
    sqlText += ");"

    val excellentNameAgeDF = sqlContext.sql(sqlText)
    val resultRDD = excellentNameAgeDF.rdd.map(
      x => (x.getAs("name").toString, Integer.valueOf(x.getAs("age").toString))
    ).join(excellentNameAgeDF.rdd.map(
      x => (x.getAs("name").toString, Integer.valueOf(x.getAs("score").toString))
    ))
    val resultRowRdd = resultRDD.map(x=>Row(x._1, x._2._1, x._2._2))

    val structFields = new util.ArrayList[StructField]()
    structFields.add(StructField("name",DataTypes.StringType, true))
    structFields.add(StructField("age",DataTypes.IntegerType, true))
    structFields.add(StructField("score",DataTypes.IntegerType, true))

    val structType = DataTypes.createStructType(structFields)
    val personDF = sqlContext.createDataFrame(resultRowRdd, structType)

    personDF.show()
    personDF.write.format("json").save("F:\\peopleResult")
  }
}
