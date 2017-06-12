package com.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Edward on 2017-6-12.
  */
object WordCountOnlineScala {
  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("WordCountOnlineScala")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.socketTextStream("Master", 9999, StorageLevel.MEMORY_AND_DISK_SER_2)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map((_, 1)).reduceByKey(_+_)
    wordCount.print

    ssc.start()
    ssc.awaitTermination()

  }
}
