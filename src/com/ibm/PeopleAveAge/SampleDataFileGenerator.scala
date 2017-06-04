package com.ibm.PeopleAveAge

import java.io.{File, FileWriter}

import scala.util.Random

/**
  * Created by Edward on 2017-3-31.
  */

object SampleDataFileGenerator {

  def main(args: Array[String]) {
    val writer = new FileWriter(new File("F:\\sampleAgeData.txt"), false)
    val rand = new Random()
    for (i <- 1 to 1000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
