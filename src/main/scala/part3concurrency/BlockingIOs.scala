package part3concurrency

import cats.effect.{IO, IOApp}
import scala.concurrent.duration.*
import utils.*
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext

object BlockingIOs extends IOApp.Simple {

  /**
   * Regular IOs, especially those built with the pure constructor, run synchronously
   * and sequentially on the same thread. To do blocking computations, we can
   * run some sleeps.
   *
   * Since Cats-Effect implements semantic blocking, no thread is actually blocked
   * when the following sleeps are executed. Instead, a timer is set and when the
   * sleep duration is over, the computation continues on a different thread.
   *
   * In semantic blocking, it looks like the thread has slept for 1 second, but
   * no thread is blocked for that amount of time.
   */
  val someSleeps: IO[Unit] = for {
    _ <- IO.sleep(1.second).myDebug // Implement Semantic Blocking
    _ <- IO.sleep(1.second).myDebug
  } yield ()

  /**
   * These sleeps supposedly run on different threads (although that didn't
   * happen on my machine) because two different fibers are executing the
   * IO sleep. So they yield the thread when they sleep.
   *
   * Output:
   * [io-compute-2] ()
   * [io-compute-2] ()
   */

  // Truly blocking IOs
  // IO.blocking takes a thunk (thunk is a tongue-in-cheek past tense of think)
  // blocking shifts the execution of the blocking operation (the thunk) to a
  // different thread pool to avoid blocking on the main execution context.
  // The created effect is uncancelable.
  val aBlockingIO: IO[Int] = IO.blocking {
    Thread sleep 1000
    println(s"[${Thread.currentThread().getName}] computed a blocking code")
    42
  } // evaluates on a thread from ANOTHER thread pool specific for blocking calls

  /**
   * Output:
   * [io-compute-blocker-1] computed a blocking code
   */

  /**
   * To implement semantic blocking, a thread that's supposed to sleep has to
   * yield. We can control how we yield a thread.
   */
  val iosOnManyThreads: IO[Unit] = for {
    _ <- IO("first").myDebug
    _ <- IO.cede // a signal to yield control over the thread. Equivalent to IO.shift in Cats-Effect 2.
    _ <- IO("second").myDebug
    _ <- IO.cede
    _ <- IO("third").myDebug
  } yield ()

  // A call to IO.cede is a HINT to the cats-effect runtime to start running
  // the rest of the for comprehension on a different thread. IO.cede might
  // not yield the thread, but it's a hint to do the yielding.
  // If we don't do the IO.cede, all the effects will run on the same thread
  // sequentially.

  // Output: (In this case the IO.cede signal was ignored; they all ran on the
  // same thread because the cats-effect runtime did some further optimizations)
  //  [io - compute - 0] first
  //  [io - compute - 0] second
  //  [io - compute - 0] third

  // We can demonstrate yielding if we run many, many more effects.
  val thousandCedes: IO[Int] =
    (1 to 1000)
      .map(IO.pure)
      .reduce(_.myDebug >> IO.cede >> _.myDebug)

  /**
   * All these thousand effects also ran on a single thread because Cats Effect
   * batched them to run on a single thread for performance reasons. But if CE
   * tries to evaluate these effects on some other thread pool that it doesn't
   * have control over, then the thread switching *might* occur.
   *
   * Let's evaluate this sequence of thousand effects on a different thread pool
   * that CE has no control over.
   *
   * We place the following code in a method to ensure that we spawn the
   * execution context only when we need it. If we call it through main then
   * daemon threads survive even after the computation finishes.
   */
  val threadPool: ExecutorService = Executors newFixedThreadPool 8

  def testThousandEffectsSwitch(): IO[Int] = {
    val ec = ExecutionContext fromExecutorService threadPool
    thousandCedes evalOn ec
  }

  /**
   * In this case the thread switching did keep taking place! Here's a sample
   * output. The batches are smaller this time.
   *
   * [pool-1-thread-3] 500
   * [pool-1-thread-6] 566
   * [pool-1-thread-7] 568
   * [pool-1-thread-5] 616
   * [pool-1-thread-1] 687
   * [pool-1-thread-4] 736
   * [pool-1-thread-2] 795
   * [pool-1-thread-8] 798
   */

  /*
    Blocking calls and IO.sleep implement semantic blocking and yield control
    of the calling thread automatically.
  */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = testThousandEffectsSwitch().void >> IO(threadPool.shutdown())
}
