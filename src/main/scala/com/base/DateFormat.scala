package com.base

import org.joda.time.DateTimeZone
import java.time.LocalDateTime

object DateFormat {

  def main(args: Array[String]): Unit = {
    val date = "1558032152539"
    println(parseEpoch(date))
    println(LocalDateTime.now())
  }

  def parseEpoch(time:String) = {
//    (new SimpleDateFormat("MM/dd/yyyy")).format(time.toLong)
//    (new SimpleDateFormat("MM/dd/yyyy")).parse(time)
    new org.joda.time.DateTime(time.toLong, DateTimeZone.forID("America/Los_Angeles")).toDate
  }

}
