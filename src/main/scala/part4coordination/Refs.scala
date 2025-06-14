package part4coordination

import cats.effect.{IO, IOApp, Ref}
import utils._
import scala.concurrent.duration._

object Refs extends IOApp.Simple {

  // A Ref is a purely functional atomic reference. Modifications are always thread-safe.
  // The Ref concurrency primitive facilitates purely functional, thread-safe
  // concurrency management.
  // Interacting with a Ref is also an effect!

  // We have to type a ref with the effect type, in this case IO. Ref is very
  // generic and can take any effect type.
  // Allocating a Ref is itself an effect, so the Ref is itself wrapped in an
  // IO when we create it using of.
  val atomicMol: IO[Ref[IO, Int]] = Ref[IO] of 42

  // Alternative way of creating a Ref
  val atomicMol_v2: IO[Ref[IO, Int]] = IO ref 42

  // A Ref is a wrapper over a value that we can modify in a purely functional
  // way and returns an effect.

  // 1. modifying is an effect
  val increasedMol:     IO[IO[Unit]] = atomicMol map (ref => ref set 43)
  val increasedMol_v2:  IO[IO[Unit]] = atomicMol map (_ set 43) // same as _.set(43)

  // 2. Since this is an effect of an effect, we usually use flatMap so that
  //    the return type is IO[Unit] instead of IO[IO[Unit]]
  val increasedMol_v3: IO[Unit] = atomicMol flatMap { _ set 43 } // always thread-safe

  // 3. Using a for comprehension
  val increasedMol_v4: IO[Unit] =
    for {
      ref <- atomicMol
      _   <- ref set 43
    } yield ()

  // 4. obtain a value
  val mol: IO[Int] = atomicMol flatMap { _.get }

  // 5. get and set in a single operation. Returns the OLD value 42; sets the new one
  val getSetMol: IO[Int] = atomicMol flatMap { _ getAndSet 43 }

  // 6. Updating with a function
  val fMol: IO[Unit] = atomicMol flatMap { _ update (_ * 10) }

  // same update in more elaborate notation:
  val fMol_v2: IO[Unit] =
    atomicMol flatMap { ref   =>
    ref       update  { value => value * 10 }}

  // 7. Update with a function and get the NEW value
  val updatedMol: IO[Int] = atomicMol flatMap { _ updateAndGet (_ * 10) } // will return IO(420)

  // 8. Update with a function and get the OLD value
  val updatedMolOld: IO[Int] = atomicMol flatMap { _ getAndUpdate (_ * 10) } // will return IO(42)

  // 9. Modify the value inside and return a different effect type using a function.
  // modify's f is has type Int => (Int, B) where B is the different type that's surfaced out
  // This is a very powerful feature.
  val modifiedMol: IO[String] =
    atomicMol flatMap { ref   =>
    ref       modify  { value => (value * 10, s"my current value is $value") }}
                      // Tuple has type (Int, String). The String type is being surfaced out

  //---------------------------------------------------------------------------
  /**
   * Why do we need the atomic Ref? Main use is for concurrent + thread-safe
   * reads and writes over shared values in a purely functional way.
   */

  // Example: a program for distributing work. Does NOT use Refs.
  // It contains a mixture of good and bad practices!
  def demoConcurrentWorkImpure(): IO[Unit] = {
    import cats.syntax.parallel._
    var count = 0 // bad practice to use a var, as it's not thread-safe

    def task(workload: String): IO[Unit] = {
      val wordCount = workload.split(" ").length
      for {
        _ <- IO(s"Counting words for workload '$workload': $wordCount").myDebug
        newCount = count + wordCount
        _ <- IO(s"New total: $newCount").myDebug
        _ = count = newCount
      } yield ()
    }

    List("I love Scala", "This ref thing is useful", "Ninad writes a lot of code")
      .map(task)
      .parSequence
      .void
  }

  /**
   * Run:
   * override def run: IO[Unit] = demoConcurrentWorkImpure()
   *
   * Output:
   * [io-compute-3] Counting words for workload 'I love Scala': 3
   * [io-compute-2] Counting words for workload 'This ref thing is useful': 5
   * [io-compute-0] Counting words for workload 'Ninad writes a lot of code': 6
   * [io-compute-0] New total: 6
   * [io-compute-2] New total: 5
   * [io-compute-3] New total: 3
   *
   * Explanation: The individual totals for each workload are correct but the
   * grand totals are wrong because 'count' is getting overwritten by three
   * threads, as it's not thread safe.
   */

  import cats.syntax.parallel._

  // Modify the above code. Wrap the word counts update in effects.
  // We don't have variable allocations except in the effect constructors.
  // Still does NOT use Refs.
  def demoConcurrentWorkImpure_v2(): IO[Unit] = {
    var count = 0 // bad practice

    def task(workload: String): IO[Unit] = {
      val wordCount = workload.split(" ").length
      for {
        _         <- IO(s"Counting words for workload '$workload': $wordCount").myDebug
        newCount  <- IO(count + wordCount) // execution is delayed by wrapping it in IO
        _         <- IO(s"New total: $newCount").myDebug
        _         <- IO(count += wordCount) // execution is delayed by wrapping it in IO
      } yield ()
    }

    List("I love Scala", "This ref thing is useful", "Ninad writes a lot of code")
      .map(task)
      .parSequence
      .void
  }

  /**
   * But the new total remains unchanged; it does not reach 14. That's because
   * the threads intermingle and overlap each other's writing.
   *
   * [io-compute-3] Counting words for workload 'Ninad writes a lot of code': 6
   * [io-compute-0] Counting words for workload 'This ref thing is useful': 5
   * [io-compute-2] Counting words for workload 'I love Scala': 3
   * [io-compute-0] New total: 5
   * [io-compute-3] New total: 6
   * [io-compute-2] New total: 3
   */

  /*
    Drawbacks (in increasing order of severity):
    - hard to read/debug
    - mixture of pure & impure code
    - NOT THREAD SAFE
  */

  // Here's a pure version using Refs that fixes the above problems
  def demoConcurrentWorkPure(): IO[Unit] = {

    def task(workload: String, total: Ref[IO, Int]): IO[Unit] = {
      val wordCount = workload.split(" ").length

      for {
        _         <- IO(s"Counting words for workload '$workload': $wordCount").myDebug
        newCount  <- total updateAndGet (_ + wordCount)
        _         <- IO(s"New total: $newCount").myDebug
      } yield ()
    }

    // The following computation is concurrent AND parallel. Parallel because
    // we are using parSequence, concurrent because threads are sharing the
    // count Ref.
    for {
      initialCount  <- Ref[IO] of 0
      _             <- List("I love Scala", "This ref thing is useful", "Ninad writes a lot of code")
                        .map(string => task(string, initialCount))
                        .parSequence
                        .void
    } yield ()
  }

  /**
   * OMG this program worked correctly on the instructor's machine but not on
   * mine, unless the printing of the new totals is happening out of order!
   * The total should be 14 at the end. He had older versions of Scala and
   * Cats Effect.
   *
   * The correct total 14 ALWAYS appears in the output, which means the
   * printing to the console must be happening out of order.
   *
   * Run:
   * override def run: IO[Unit] = demoConcurrentWorkPure()
   *
   * Output:
   * [io-compute-1] Counting words for workload 'This ref thing is useful': 5
   * [io-compute-3] Counting words for workload 'Ninad writes a lot of code': 6
   * [io-compute-2] Counting words for workload 'I love Scala': 3
   * [io-compute-3] New total: 9
   * [io-compute-1] New total: 14
   * [io-compute-2] New total: 3
   */

  //===========================================================================
  // Exercises

  // 1. Convert the following impure program to pure using Refs and thread safety

  /**
   * Clock ticks every second. Print the current time every second on each
   * tick. Also count the number of ticks and print the number every 5 seconds
   */
  def tickingClockImpure(): IO[Unit] = {
    var ticks: Long = 0L

    // Ticks every second. Prints the current time at each tick.
    def tickingClock: IO[Unit] = for {
      _ <- IO.sleep(1.second)
      _ <- IO(System.currentTimeMillis()).myDebug // print the current time
      _ <- IO(ticks += 1) // increment the number of ticks. Not thread safe
      _ <- tickingClock
    } yield ()

    // Prints the number of ticks every 5 seconds
    def printTicks: IO[Unit] = for {
      _ <- IO.sleep(5.seconds)
      _ <- IO(s"TICKS = $ticks").myDebug
      _ <- printTicks
    } yield ()

    // Start the ticking clock and print ticks concurrently on two different fibers
    for {
      _ <- (tickingClock, printTicks).parTupled
    } yield ()
  }
  /**
   * Sample output of impure version: (The 2 fibers executed on different threads.)
   *
   * [io-compute-1] 1749560516107
   * [io-compute-1] 1749560517113
   * [io-compute-1] 1749560518114
   * [io-compute-1] 1749560519115
   * [io-compute-2] TICKS = 4
   */

  //---------------------------------------------------------------------------
  // Pure version using Refs and thread safety. This is a correct version
  def tickingClockPure(): IO[Unit] = {

    // Ticks every second. Prints the current time at each tick.
    def tickingClock(ticks: Ref[IO, Long]): IO[Unit] = for {
      _ <- IO.sleep(1.second)
      _ <- IO(System.currentTimeMillis()).myDebug // print the current time
      _ <- ticks update (_ + 1) // increment the number of ticks. thread-safe effect
      _ <- tickingClock(ticks)
    } yield ()

    // Prints the number of ticks every 5 seconds
    def printTicks(ticks: Ref[IO, Long]): IO[Unit] = for {
      _         <- IO.sleep(5.seconds)
      tickCount <- ticks.get
      _         <- IO(s"TICKS = $tickCount").myDebug
      _         <- printTicks(ticks)
    } yield ()

    // Start the ticking clock and print ticks concurrently on two different fibers
    for {
      initialTicks <- IO ref 0L // Same as 'Ref[IO] of 0L' initialTicks has type Ref[IO, Long]
      _ <- (tickingClock(initialTicks), printTicks(initialTicks)).parTupled
    } yield ()
  }
  /**
   * Sample output of pure version: (Both fibers executed on the same thread)
   * [io-compute-3] 1749564079874
   * [io-compute-3] 1749564080880
   * [io-compute-3] 1749564081881
   * [io-compute-3] 1749564082883
   * [io-compute-3] TICKS = 4
   */

  //---------------------------------------------------------------------------
  /**
   * This implementation is WRONG even though it uses a Ref for the ticks
   * because there's a race condition for the ticks resource, as it is not
   * passed explicitly between the two methods that use it.
   *
   * So the tick count NEVER get updated; it stays at 0. That's because the
   * code 't <- ticks' provides a NEW reference every time this IO is evaluated,
   * a reference initialized to 0. So the tickCount is always 0.
   *
   * Moral of the story: When we pass ticks as a parameter to the two methods,
   * ticks should be of type Ref[...], NOT of IO[Ref[...]]. If we use
   * IO[Ref[...]], it will create a NEW ticks reference every time the IO
   * effect is evaluated. So we should make sure that we UNPACK IOs that wrap
   * concurrency primitives such as ref, deferred, etc.
   *
   * The above method tickingClockPure() has the correct implementation.
   */
  def tickingClockWeird(): IO[Unit] = {
    val ticks: IO[Ref[IO, Long]] = IO ref 0L  // initialize ticks to 0.

    // Ticks every second. Prints the current time at each tick.
    def tickingClock: IO[Unit] = for {
      t <- ticks  // Ticks provides a NEW reference every time this IO is evaluated
      _ <- IO.sleep(1.second)
      _ <- IO(System.currentTimeMillis()).myDebug // print the current time
      _ <- t update (_ + 1) // increment the number of ticks
      _ <- tickingClock
    } yield ()

    // Prints the number of ticks every 5 seconds
    def printTicks: IO[Unit] = for {
      t         <- ticks // Ticks provides a NEW reference every time this IO is evaluated
      _         <- IO.sleep(5.seconds)
      tickCount <- t.get
      _         <- IO(s"TICKS = $tickCount").myDebug
      _         <- printTicks
    } yield ()

    // Print time and tick count in parallel on two different fibers
    for {
      _ <- (tickingClock, printTicks).parTupled
    } yield ()
  }
  /**
   * Sample output (ticks is ALWAYS 0):
   *
   * [io-compute-1] 1749565404369
   * [io-compute-1] 1749565405374
   * [io-compute-1] 1749565406375
   * [io-compute-1] 1749565407376
   * [io-compute-0] TICKS = 0
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = tickingClockWeird()

}
