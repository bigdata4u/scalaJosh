package com.base

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
object readConfig {

  def main(args:Array[String]) = {
    readConfig
  }

  def readConfig = {
        val config: Config = ConfigFactory.load()
//    println(config.getConfig("kafka.consumer"))
    val props = new Properties()

    val map = config.getConfig("kafka.consumer").entrySet().asScala.map{ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    }.toMap

    props.putAll(map.asJava)
    println(props)
  }
}
