package com.DataFrame;

/**
 * Created by Edward on 2017-5-31.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQLLoadSaveOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLLoadSaveOps");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        /**
         * read()是DataFrameReader类型，load可以将数据读取出来
         */
        DataFrame peopleDF = sqlContext.read().load("E:\\SourceCode\\spark-1.6.0\\examples\\target\\scala-2.10\\classes\\users.parquet");

        /**
         * 直接对DataFrame进行操作
         * Json: 是一种自解释的格式，读取Json的时候怎么判断其是什么格式？
         * 通过扫描整个Json。扫描之后才会知道元数据
         */
        //通过mode来指定输出文件的是append。创建新文件来追加文件
        peopleDF.select("name").write().save("F:\\personNames");
    }
}