package com.base

import scala.util.{Either, Failure, Left, Right, Success, Try}
import com.base._

object HandlePackage {

  def main(args: Array[String]): Unit = {
    println(s"yei aata he kya: $fees")
    val t:Try[String] = Success("foo")
    println(t.tryToEither)
    val t2:Try[String] = Failure(new RuntimeException("bar"))
    println(t2.toEitherA)
//
//    val e:Either[Throwable,String] = Right("foo")
//    println(e.toTryA)
//    val e2:Either[Throwable,String] = Left(new RuntimeException("bar"))
//    println(e2.toTryA)
  }
}
