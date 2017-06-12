package com.SparkLogAnalyze;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Edward on 2017/6/10.
 */
public class SparkSQLUserLogsOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLUserLogsOps");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        String twodaysago = SparkSQLDataManually.yesterday();
        pvStatistic(hiveContext, twodaysago);
    }

    private static void pvStatistic(HiveContext hiveContext, String twodaysago) {
        hiveContext.sql("use hive;");
        String sqlText = "Select ...";
        hiveContext.sql(sqlText);
    }
}
