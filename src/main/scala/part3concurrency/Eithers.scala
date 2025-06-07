package part3concurrency

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import utils._

object Eithers {

  def simpleEither[A, B](isLeft: Boolean, a: A, b: B): Either[A, B] =
    if (isLeft) Left(a)
    else Right(b)

  val testSimpleEither: Either[Int, String] = simpleEither(false, 42, "Scala") // Right(Scala)

  def simpleIOEither[A, B](isLeft: Boolean, a: A, b: B): IO[Either[A, B]] =
    if (isLeft) IO(Left(a))
    else IO(Right(b))

  val testSimpleIOEither: IO[Either[Int, String]] = simpleIOEither(false, 43, "Haskell")  // Right(Haskell)
  

  def main(args: Array[String]): Unit = {
    println(testSimpleEither)
    testSimpleIOEither.myDebug.unsafeRunSync()
  }
}
