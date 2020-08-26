package com.base

object DiffLists {

  def main(args: Array[String]): Unit = {

    // Base List
    val list1 = List("One", "Two", "Three", "Four")
    val set1 = Set("One", "Two", "Three", "Four")
    // compare two
    val list2= List("One", "Two")
    val set2 = Set("One", "Two")

    val common = set1.companion(set2).flatten
    common.foreach(println(_))
    println("--------------------------------------")
    (set1--(common)).foreach(println)
//    (set1 - common.toList).foreach(println(_))

  }
}
