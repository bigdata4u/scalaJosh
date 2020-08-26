package com
import scala.util.{ Either, Failure, Left, Right, Success, Try }

package object base {

  val fees = "dekho ab kya hota he"

  implicit class RichEither[L <: Throwable,R](e:Either[L,R]){
    def toTryA:Try[R] = e.fold(Failure(_), Success(_))
    def eitherToTry[L <: Exception, B]: Try[R] = {
      e match {
        case Right(obj) => Success(obj)
        case Left(err) => Failure(err)

      }
    }
  }

  implicit class RichTry[T](t:Try[T]){
    def toEitherA:Either[Throwable,T] = t.transform(s => Success(Right(s)), f => Success(Left(f))).get
    def tryToEither: Either[Throwable, T] = {
      t match {
        case Success(something) => Right(something)
        case Failure(err) => Left(err)
      }
    }
  }




}
