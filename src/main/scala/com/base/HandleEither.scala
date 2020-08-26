package com.base

import scala.util.Try

object HandleEither {

  def main(args: Array[String]): Unit = {
    val result = Try(thorwExe() match {
      case Left(e) => throw e
      case Right(value) => value
    }).toOption
    println(s"result: $result")
  }

  def thorwExe(): Either[Throwable, String] = {
    throw new ClassNotFoundException("Not found")
  }
}
