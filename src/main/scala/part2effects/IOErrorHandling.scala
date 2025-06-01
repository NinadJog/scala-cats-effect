package part2effects

import cats.effect.IO
import scala.util.{Try, Success, Failure}

object IOErrorHandling {

  // IO: can be created with pure, delay, defer
  // create failed effects

  // 1. With IO.delay or IO.defer and throw an exception. The exception will be
  //    thrown only when we call unsafeRunSync().
  val aFailedCompute: IO[Int] = IO.delay(throw new RuntimeException("A failure!"))

  // 2. By calling raiseError. raiseError stores the exception. It is thrown
  //    only when unsafeRun is called. This is the more idiomatic way.
  val aFailure: IO[Int] = IO.raiseError(new RuntimeException("A proper failure"))

  // Since exceptions can be suspended or embodied in IO, we can handle those
  // exceptions using methods provided by IO.
  // The handleErrorWith method converts an exception to an effect.
  val dealWithIt: IO[Int | Unit] = aFailure.handleErrorWith {
    case _: RuntimeException => IO.delay(println("I'm still here. Converted exception to an effect"))
  }

  // If we want to deal with both the successful and failed cases at the same
  // time, use the following APIs.

  // 1. Turn the potentially failed effect into an Either.
  val effectAsEither: IO[Either[Throwable, Int]] = aFailure.attempt

  // 2. redeem transforms the failure and the success in one go
  val resultAsString: IO[String] = aFailure.redeem(
    ex    => s"FAIL: $ex",        // failure case
    value => s"SUCCESS: $value"   // success
  )

  // 3. redeemWith is like flatMap for redeem, where the returned values are
  //    not simple values but are wrapper values. So we return effects rather
  //    than plain values.
  val resultAsEffect: IO[Unit] = aFailure.redeemWith(
    ex    => IO(println(s"FAIL: $ex")),
    value => IO(println(s"SUCCESS: $value"))
  )

  //---------------------------------------------------------------------------
  // Exercises

  // 1. Construct potentially failed IOs from standard library data types
  //    Option, Try, Either. (Convert Scala data types to effects)

  // Can also use IO.pure(x) for the Some case if you don't want to suspend
  // the value in IO, if you want to evaluate it eagerly, because the value
  // has already been evaluated inside the option, so there's no significant
  // difference between the two.
  def option2IO[A](option: Option[A])(ifEmpty: Throwable): IO[A] =
    option match
      case Some(value)  => IO(value)  // or IO.pure(x)
      case None         => IO.raiseError(ifEmpty)

  def try2IO[A](aTry: Try[A]): IO[A] =
    aTry match
      case Success(value) => IO(value)  // Or IO.pure(value)
      case Failure(ex)    => IO.raiseError(ex)

  def either2IO[A](anEither: Either[Throwable, A]): IO[A] =
    anEither match
      case Left(ex)     => IO.raiseError(ex)
      case Right(value) => IO(value)

  // Cats-Effect provides library functions for the above conversions:
  // fromOption, fromTry, fromEither, fromFuture, fromCompletableFuture

  //---------------------------------------------------------------------------
  // 2. handleError and handleErrorWith in terms of the other APIs seen so far

  // If the IO is successful, the final result will also be successful but
  // if the IO fails with some sort of exception then this handler will be
  // performed and we return a successful IO with whatever value the handler
  // returns on that particular exception.
  def handleIOError[A](io: IO[A])(handler: Throwable => A): IO[A] =
    io.redeem(handler, identity)

  // My solution. TBD if it's the same as IO.pure for the second argument
  def handleIOErrorWith[A](io: IO[A])(handler: Throwable => IO[A]): IO[A] =
    io.redeemWith(handler, _ => io) // or value => IO.pure(value) for the second argument

  // Instructor's solution:
  def handleIOErrorWith_v2[A](io: IO[A])(handler: Throwable => IO[A]): IO[A] =
    io.redeemWith(handler, IO.pure) // same as value => IO.pure(value) for the second argument

  // Cats-effect already has a function for this called handleFailureWith
  // aFailure.handleErrorWith(f: Throwable => IO[B])

  //---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {

    import cats.effect.unsafe.implicits.global
    // aFailure.unsafeRunSync()
    // dealWithIt.unsafeRunSync()
    // println(resultAsString.unsafeRunSync())
    resultAsEffect.unsafeRunSync()
  }
}
