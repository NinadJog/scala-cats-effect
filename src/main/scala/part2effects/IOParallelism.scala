package part2effects

import cats.Parallel
import cats.effect.{IO, IOApp}

object IOParallelism extends IOApp.Simple {

  // 1. Compose the two IOs sequentially with a for comprehension
  // IOs are usually sequential

  val aliceIO:  IO[String] = IO(s"[${Thread.currentThread.getName}] Alice")
  val bobIO:    IO[String] = IO(s"[${Thread.currentThread.getName}] Bob")

  val composedIO: IO[String] =
    for
      alice <- aliceIO
      bob   <- bobIO
    yield
      s"$alice and $bob love Cats Effects IOs"

  //-------------------------------------------------
  // 2. Compose the two IOs sequentially using mapN.
  // Use a custom-defined extension method on IO called myDebug to see which
  // thread each IO is running on.

  import utils._              // Import the myDebug IO extension method
  import cats.syntax.apply._  // Import mapN

  // Both the following IOs run on the same thread, which means they are
  // sequential.
  val meaningOfLife:  IO[Int]     = IO.delay(42)
  val favLang:        IO[String]  = IO.delay("Scala")
  val goalInLife:     IO[String]  =
    (meaningOfLife.myDebug, favLang.myDebug) mapN ((num, string) =>
                          s"My goal in life is $num and $string")

  //-------------------------------------------------
  // 3. Parallelism in IOs

  // 1. Convert a sequential IO to parallel
  //    Parallel is a Cats type class
  val parIO1: IO.Par[Int]     = Parallel[IO] parallel meaningOfLife.myDebug
  val parIO2: IO.Par[String]  = Parallel[IO] parallel favLang.myDebug

  import cats.effect.implicits._
  val goalInLifeParallel: IO.Par[String] =
    (parIO1, parIO2) mapN ((num, string) => s"My goal in life is $num and $string")
  // turn it back to sequential
  val goalInLife_v2: IO[String] = Parallel[IO] sequential goalInLifeParallel

  //-------------------------------------------------
  // 4. Simplify above pattern with parallel version of mapN
  // parMapN does the conversion to parallel and back to sequential for us

  import cats.syntax.parallel._ // import parMapN

  val goalInLife_v3: IO[String]  =
    (meaningOfLife.myDebug, favLang.myDebug) parMapN ((num, string) =>
      s"My goal in life is $num and $string")

  //-------------------------------------------------
  // 5. Dealing with Failure
  // compose success + failure in parallel

  val aFailure: IO[String] = IO.raiseError(new RuntimeException("I can't do this!"))
  val parallelWithFailure:  IO[String] =
    (meaningOfLife.myDebug, aFailure.myDebug) parMapN (_+_)

  // We don't get a final result. The composition fails if any of its components fails.
  // We can handle the error using handleErrorWith or redeem or attempt or any
  // of the error handling techniques that we saw earlier.

  // Output:
  // [io-compute-0] 42
  // java.lang.RuntimeException: I can't do this!

  //-------------------------------------------------
  // 6. Composing two failures
  val anotherFailure:       IO[String] = IO.raiseError(new RuntimeException("Second failure"))
  val twoFailuresParallel:  IO[String] =
    (aFailure.myDebug, anotherFailure.myDebug) parMapN (_+_)

  // The first one to fail in this case is the second one to be defined, which
  // shows that order does not matter
  // Output: java.lang.RuntimeException: Second failure

  //-------------------------------------------------
  // 7. What if one of the failures is delayed?

  val twoFailuresDelayed: IO[String] =
    (aFailure.myDebug, IO(Thread.sleep(1000)) >> anotherFailure.myDebug) parMapN (_+_)

  // aFailure happens first; the composition stops whenever the first effect
  // fails.
  // Output: java.lang.RuntimeException: I can't do this!

  //===========================================================================
  // various implementations of the run method for the above use cases

  // 1.
  // override def run: IO[Unit] = composedIO map println

  // 2.
  /* Executes each IO effect on the same thread
      [io-compute-0] 42
      [io-compute-0] Scala
      My goal in life is 42 and Scala
   */
  // override def run: IO[Unit] = goalInLife map println

  //-----------
  // 4.
  /* Executes each IO effect and their composition on a different thread.
     It's a BIG deal that the effect composition also happens on a different
     thread and the synchronization of the three threads is happening
     automatically. (We wrote this parallel program in just one line of code,
     on the line where the mapN function is called.)

     Note that the two effects are printed out of order: 'Scala'
     is printed before '42', another hint that they are being executed in
     parallel.

    [io-compute-2] Scala
    [io-compute-0] 42
    [io-compute-2] My goal in life is 42 and Scala
   */
  // override def run: IO[Unit] = goalInLife_v3.myDebug.void
  // or = goalInLife_v2 map println

  override def run: IO[Unit] = twoFailuresDelayed.myDebug.void
}

