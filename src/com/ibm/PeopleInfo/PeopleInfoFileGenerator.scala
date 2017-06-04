package com.ibm.PeopleInfo

import java.io.{File, FileWriter}
import java.util.Random

/**
  * Created by Edward on 2017-3-29.
  */
object PeopleInfoFileGenerator {
  def main(args: Array[String]): Unit = {
    val writer = new FileWriter(new File("F:\\samplePeopleInfo.txt"), false)
    val rand = new Random()

    for (i <- 1 to 1000) {
      var height = rand.nextInt(220)
      val gender = if ((rand.nextInt(2) + 1) % 2 == 0) "M" else "F"

      if (height < 50)
        height = height + 50
      if (height < 100 && gender == "M")
        height = height + 100
      if (height < 100 && gender == "F")
        height = height + 50

      writer.write(i + " " + gender + " " + height)
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    println("People Information File Generate Successfully.")
    System.currentTimeMillis()
  }
}
