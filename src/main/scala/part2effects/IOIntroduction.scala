package part2effects

import cats.effect.IO
import scala.annotation.tailrec
import scala.io.StdIn

/*
  Key takeaway: IOs are useful not only to compose effects but also to avoid
  stack recursion because the Cats-Effect runtime processes chains of
  flatMaps in a tail-recursive manner behind the scenes using trampolining.
*/

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
  // Exercises

  // 1. Sequence two IOs and return the result of the LAST one.
  def sequenceTakeLast[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    for
      _ <- ioa
      b <- iob
    yield b

  // Using flatMap and map. My attempt
  def sequenceTakeLast_v2[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa flatMap (_ =>   // The use of flatMap guarantees sequencing of a followed by b
    iob map     (identity(_)))   // (b => b)

  // But this reduces to the following, because iob map (identity(_))
  // is the same as iob.
  def sequenceTakeLast_v3[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa flatMap (_ => iob)

  // This pattern is so common that it has an operator *> called andThen:
  def sequenceTakeLast_v4[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa *> iob  // "andThen"

  // There's a related operator >> where iob is passed by name, so the
  // evaluation is delayed (lazy)
  def sequenceTakeLast_v5[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa >> iob // lazy "andThen" (call-by-name), similar to Haskell's >> bind operator

  //----------------------------
  // 2. Sequence two IOs and return the result of the FIRST one.
  def sequenceTakeFirst[A, B](ioa: IO[A], iob: IO[B]): IO[A] =
    for
      a <- ioa
      _ <- iob
    yield a

  // using flatMap and map
  def sequenceTakeFirst_v2[A, B](ioa: IO[A], iob: IO[B]): IO[A] =
    ioa flatMap (a =>
    iob map     (_ => a))

  // This occurrence is also common enough that there's a special operator for it:
  def sequenceTakeFirst_v3[A, B](ioa: IO[A], iob: IO[B]): IO[A] =
    ioa <* iob  // Execute ioa then iob, but take ioa's result

  //----------------------------
  // 3. Repeat an IO effect forever

  // Using for comprehension. In practice the result of the recursive call will
  // never be returned since it's infinite recursion.
  def forever[A](ioa: IO[A]): IO[A] =
    for
      _       <- ioa
      result  <- forever(ioa)
    yield result

  // Instructor's solution
  def forever_v2[A](ioa: IO[A]): IO[A] =
    ioa flatMap (_ => forever_v2(ioa))

  // Since the same pattern occurs as before, we can use the lazy andThen operator.
  // The lazy andThen operator is recommended because the eager one cause a stack
  // overflow. The cats-effect runtime executes this function in a tail-recursive
  // manner behind the scenes, which is why it doesn't cause a stack overflow.
  def forever_v3[A](ioa: IO[A]): IO[A] =
    ioa >> forever_v3(ioa)  // uses lazy andThen

  // Using the eager andThen operator. Causes a stack overflow because it
  // evaluates ioa repeatedly before the final ioa has had a chance to run
  // its unsafeRunSync method.
  // So it causes a stack overflow even without running unsafeRunSync!
  def forever_v4[A](ioa: IO[A]): IO[A] =
    ioa *> forever_v4(ioa)  // eager andThen

  // Implement using foreverM
  def forever_v5[A](ioa: IO[A]): IO[A] =
    ioa.foreverM  // same as v3 with the lazy andThen

  //----------------------------
  // 4. Convert an IO to a different type
  def convert[A, B](ioa: IO[A], value: B): IO[B] =
    ioa map (_ => value)

  // This pattern is so common that there's a dedicated method named 'as' to
  // implement it:
  def convert_v2[A, B](ioa: IO[A], value: B): IO[B] =
    ioa as value

  //----------------------------
  // 5. Discard value inside an IO, just return Unit
  def asUnit[A](ioa: IO[A]): IO[Unit] =
    ioa map (_ => ())

  // Implementation with 'as' method
  // heavily discouraged as it confuses programmers
  def asUnit_v2[A](ioa: IO[A]): IO[Unit] =
    ioa as ()

  // Same thing; this implementation is encouraged.
  def asUnit_v3[A](ioa: IO[A]): IO[Unit] =
    ioa.void

  //----------------------------
  // 6. Fix stack recursion
  def sum(n: Int): Int =
    if (n <= 0) 0
    else n + sum(n-1)

  // My attempt at a tail-recursive version that returns the result wrapped in IO
  // This is NOT what the instructor had asked for
  def sum_v2(n: Int): IO[Int] = {
    @tailrec
    def sumTailRec(m: Int, acc: Int): Int =
      if (m <= 0) acc
      else sumTailRec(m - 1, acc + m)

    val result = sumTailRec(n, 0)
    IO(result)
  }

  // Fix the same problem when it happens under IO.
  // But my attempt wasn't the correct solution; here's the correct version
  // Key takeaway: IOs are useful not only to compose effects but also to avoid
  // stack recursion because the Cats-Effect runtime processes chains of
  // flatMaps in a tail-recursive manner behind the scenes.
  def sumIO(n: Int): IO[Int] =
    if (n <= 0) IO(0)
    else for
      lastNum <- IO(n)
      prevSum <- sumIO(n-1)
    yield
      lastNum + prevSum

  //----------------------------
  /*
    7. Write a Fibonacci IO that does NOT crash on recursion.
    hints: use recursion, ignore exponential complexity, use flatMaps heavily.
    In the following code, IO(fibobacci(n-1)) has type IO[IO[BigInt]], so we
    flatMap it with the identity function.

    If we don't wrap the calls to fibonacci(n-1) and fibonacci(n-2) in IO, the
    recursion happens immediately (eagerly), leading to stack overflow for
    large n, since each recursive call adds a stack frame. The IO monad doesn't
    get a chance to manage the recursion in a stack-safe way.

    By wrapping fibonacci(n - 1) in IO, we defer the evaluation of the recursive call.
    flatMap(x => x) ensures that the IO effect is properly chained in a way that
    Cats Effect can execute it trampolined (i.e., without consuming stack space).
   */
  def fibonacci(n: Int): IO[BigInt] =
    if (n <= 0)       IO(0)
    else if (n == 1)  IO(1)
    else for
      last <- IO(fibonacci(n - 1)).flatMap(x => x)
      prev <- IO(fibonacci(n - 2)).flatMap(identity)  // Same as (x => x)
    yield
      last + prev

  // Using flatten instead of flatMap(identity)
  def fibonacci_v2(n: Int): IO[BigInt] =
    if (n <= 0)       IO(0)
    else if (n == 1)  IO(1)
    else for
      last <- IO(fibonacci_v2(n - 1)).flatten
      prev <- IO(fibonacci_v2(n - 2)).flatten  // Same as flatMap(x => x) or flatMap(identity)
    yield
      last + prev

   // A more idiomatic way is to use IO.defer, which delays the evaluation of
   // the recursive call.
   // IO.defer is a combination of IO.delay and flatten:
   // IO.defer(fibonacci_v3(n - 1)) == IO.delay(fibonacci_v3(n - 1))
   // IO.defer is the third way of constructing IO data types based on
   // EXISTING IO data types. (In this case the existing IO data type is
   // fibonacci(n-1). So we are suspending an effect inside another effect
   // and returning an IO of that same type.

   def fibonacci_v3(n: Int): IO[BigInt] =
     if (n <= 0)       IO(0)
     else if (n == 1)  IO(1)
     else for
       last <- IO.defer(fibonacci_v3(n - 1))  // Same as IO.delay(...).flatten
       prev <- IO.defer(fibonacci_v3(n - 2))
     yield
       last + prev

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

    // smallProgram_v2.unsafeRunSync()

    // Test the forever function
    // forever_v3(IO(println("Print this line forever!"))).unsafeRunSync()

    /*
    // Causes a stack overflow
    forever_v4(IO{
      println("forever!")
      Thread.sleep(100)
    }).unsafeRunSync()
    */

    // println(sumIO(20000).unsafeRunSync())

    // Print the first 30 Fibonacci numbers
    (1 to 30).foreach(i => println(fibonacci_v3(i).unsafeRunSync()))

  }
}
