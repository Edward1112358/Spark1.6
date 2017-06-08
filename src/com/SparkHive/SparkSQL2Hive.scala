package com.SparkHive

/**
  * Created by Edward on 2017-6-5.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

// 使用Java的方式开发实战对DataFrame的操作
object SparkSQL2Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("SparkSQL2Hive") //设置应用程序名
    conf.setMaster("spark://slq1:7077")
    //设置集群的Master
    val sc = new SparkContext //创建SparkContext对象，

    //在目前企业级大数据Spark开发的时候，绝大多数情况下是采用Hive作为数据仓库
    //Spark提供了HIve的支持功能，Spark通过HiveContext可以直接操作Hive中的数据
    //基于HiveContext我们可以使用sql/hql两种方式才编写SQL语句对Hive进行操作，
    //包括创建表、删除表、往表里导入数据 以及用SQL语法构造 各种SQL语句对表中的数据进行CRUD操作
    //第二：也可以直接通过saveAsTable的方式把DaraFrame中的数据保存到Hive数据仓库中
    //第三：可以直接通过HiveContext.table方法来直接加载Hive中的表而生成DataFrame
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use hive")
    hiveContext.sql("DROP TABLE IF EXISTS people")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING,age INT)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/richard/slq/spark/people.txt' INTO TABLE people")
    //把本地数据加载到Hive中（背后实际上发生了数据的拷贝）
    //当然也可以通过LOAD DATA INPATH去获得HDFS等上面的数据 到Hive（此时发生了数据的移动）
    hiveContext.sql("DROP TABLE IF EXISTS peoplescores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS peoplescores(name STRING,score INT)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/richard/slq/spark/peoplescores.txt' INTO TABLE peoplescores")

    //通过HiveContext使用join直接基于Hive中的两张表进行操作获得大于90分的人的name,age,score
    val resultDF = hiveContext.sql("SELECT pi.name,pi.age,ps.score"
      + "FROM people pi JOIN peoplescores ps ON pi.name=ps.name WHERE ps.score > 90")

    //通过saveAsTable创建一张Hive Managed Table，数据放在什么地方、元数据都是Hive管理的
    //当删除该表时，数据也会一起被删除（磁盘上的数据不再存在）
    hiveContext.sql("DROP TABLE IF EXISTS peopleinformationresult")
    resultDF.write.saveAsTable("peopleinformationresult")

    //使用HivewContext的Table方法可以直接去读Hive中的Table并生成DaraFrame
    //读取的数据就可以进行机器学习、图计算、各种复杂ETL等操作
    val dataFrameHive = hiveContext.table("peopleinformationresult")
    dataFrameHive.show()
  }
}

