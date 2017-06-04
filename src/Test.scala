import scala.collection.mutable.HashSet
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.{HashMap, Map}
import com._

/**
  * Created by Edward on 2017-2-3.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.parallelize(List((0, 2. ), (0, 4. ), (1, 10. ), (1, 20. )))
    val df = data.toDF("col1", "col2")

    //val tttt:Map[String,Array[Int]] = Map("b"->Array(3,4))
    //val x = tt.+(ttt) //有没有办法把a合并成Map("a"->Array(3,5))
    //val y = tt.++(tttt)

    /*
      // reduceByKey/aggregateByKey
      val pairs = sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
      val resReduce = pairs.reduceByKey(_ + _)
      resReduce.collect.foreach(println)
      val resAgg = pairs.aggregateByKey(0)(_+_,_+_)
      resAgg.collect.foreach(println)

      val sets = pairs.aggregateByKey(new HashSet[Int])(_+_, _++_)
      sets.collect.foreach(println)*/

    /*
    // 写日志
    val logger = Logger.getLogger("TestSpark");
    logger.info("============This is info message.============");
    logger.warn("============This is warn message.============");
    logger.error("============This is error message.============");*/

    /*
    // 求平局
    val data = sc.parallelize(List((0, 2. ), (0, 4. ), (1, 10. ), (1, 20. )))
    // 方法一
    val sum = data.reduceByKey((x, y) => x + y)
    val count = data.map(x => (x._1, 1)).reduceByKey(_ + _)
    val ave = sum.join(count).map(x => (x._1, x._2._1 / x._2._2))
    ave.foreach(println)
    // 方法二
    val ave1 = data.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, (x._2._1 / x._2._2)))
    // 方法三
    val ave2 = data.combineByKey(value => (value, 1),
      (x: (Double, Int), value: Double) => (x._1 + value, x._2 + 1),
      (x: (Double, Int), y: (Double, Int)) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2)))
    ave2.foreach(println)*/
  }
}
