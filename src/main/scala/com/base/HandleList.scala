package com.base

import scala.util.{Failure, Success}

object HandleList {

  def main(args:Array[String]) = {
//    populateList
    println(findFalse)
    println(FindFirstFailed)
  }

//  def populateList = {
//    val x = List(1,2,3,4,5,1,2,3,4,5,1,2,3,4,5,1,2,3,4,5,1,2,3,4,5)
//    val z: Seq[Int] = x.flatMap(multiple(_).head)
//    println(z)
//  }
//  def multiple(y:Int) = Option[Seq[Int]] {
//    val x = Seq(1,2,3,4,5,1,2,3,4,5,1,2,3,4,5,1,2,3,4,5,1,2,3,4,5)
//    if (System.currentTimeMillis()%2 == 0)
//      x.map(_*y)
//    else
//      Seq()
//  }

  def findFalse() = {
    val x = Seq(true,true,true )
    println(x.contains(false))
    val y = Seq(true,false,true )
    println(y.contains(false))
    val z = Seq(1,2,3 )
    println(z.contains(10))
    val a = Seq(Failure,Failure,Success )
    println(a.contains(Success))
    a.collectFirst({case x:Success.type => x})
  }

  def FindFirstFailed(): Option[Throwable] = {
    val xyz = Seq(Success, Failure(new Throwable("34")), Failure(new Throwable("2")))
    xyz.collectFirst({case Failure(x) => x})
  }
}
