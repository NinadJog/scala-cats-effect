package part2effects

import cats.effect.IO

import scala.io.StdIn

object IOIntroduction {

  // Different ways of creating an IO
  // 1. With the pure method
  // The argument to pure should not have side effects because side effects
  // should be suspended until the point where we evaluate it at the end.
  // The pure method evaluates its argument eagerly.
  val ourFirstIO: IO[Int] = IO.pure(42)

  // 2. With the delay method
  val aDelayedIO: IO[Int] = IO.delay({
    println("I'm producing an integer.")
    54
  })

  // Since the pure method evaluates its argument eagerly, we should NOT
  // pass impure code as the argument. It's the programmer's responsibility
  // as the compiler can't tell whether the code has side-effects or not.
  // Running the main program now prints out the line from this function.
  val shouldNotDoThis: IO[Int] = IO.pure({
    println("I'm producing an integer. Should not do this; I should not cause side effects from pure")
    54
  })

  // If you're unsure whether the expression you're passing produces
  // side effects or not, just use delay instead of pure.

  // 3. With IO's apply method. It's exactly the same as IO.delay
  val aDelayedIO_v2: IO[Int] = IO { // apply == delay
    println("I'm producing an integer.")
    54
  }

  //---------------------------------------------------------------------------
  // map, flatMap, etc.
  val improvedMeaningOfLife: IO[Int] = ourFirstIO map (_*2)
  val printedMeaningOfLife: IO[Unit] = ourFirstIO flatMap (mol => IO.delay(println(mol)))

  def smallProgram: IO[Unit] =
    for
      _     <- IO.delay(println("Please enter text on two lines"))
      line1 <- IO(StdIn.readLine())
      line2 <- IO(StdIn.readLine())
      _     <- IO.delay(println(line1 + line2))
    yield ()

  // mapN - combine IO effects as tuples.
  // The mapN transformation from the Apply Cats type class is used to
  // compose effects (i.e. to compose IO data types)
  // The following example combines two effects to produce a single one:
  import cats.syntax.apply._
  val combinedMeaningOfLife: IO[Int] = (ourFirstIO, improvedMeaningOfLife) mapN (_ + _)

  // We can use mapN to rewrite the smallProgram without for comprehensions
  def smallProgram_v2: IO[Unit] = {
    (IO(StdIn.readLine()), IO(StdIn.readLine())) mapN (_+_) map println
  }

  //---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {

    // Performing the IO effects

    // Import the Cats Effect IO Runtime. This is the "platform" needed to
    // perform the effects described by the IO.
    import cats.effect.unsafe.implicits.global

    // Perform the effect at the "end of the world" to make the effects concrete.
    // The unsafeRunSync() or unsafeRunAsync() is called exactly once.
    println(aDelayedIO.unsafeRunSync())

    // The goal of Cats Effect is for programmers to be able to compose
    // computations with the IO data types

    // smallProgram.unsafeRunSync()

    println(combinedMeaningOfLife.unsafeRunSync()) // 126

    smallProgram_v2.unsafeRunSync()

  }
}
