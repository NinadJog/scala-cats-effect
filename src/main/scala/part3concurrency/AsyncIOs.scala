package part3concurrency

import cats.effect.{IO, IOApp}
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try
import utils.*

/**
 * Unlike uncancelable, the techniques introduced here are rarely used and are
 * quite complicated. The Async API is quite cumbersome.
 *
 * Goal: Run IOs (effects) asynchronously on fibers without having to manually
 * manage the fiber lifecycle.
 *
 * Usually, asynchronous implies a program based on callbacks. We run something
 * asynchronously and when the computation ends, the callback function is run.
 * The callback can be run either on a thread pool managed by Cats Effect or on
 * some other thread.
 */
object AsyncIOs extends IOApp.Simple {

  val threadPool: ExecutorService   = Executors newFixedThreadPool 8
  given ec:       ExecutionContext  = ExecutionContext fromExecutorService threadPool

  // Define a type alias for the callback function. async_ takes a callback from A to Unit.
  type Callback[A] = Either[Throwable, A] => Unit

  def computeMeaningOfLife(): Int = {
    Thread sleep 1000 // simulates a long computation
    println(s"[${Thread.currentThread().getName}] computing the meaning of life on some other thread...")
    42
  }

  def computeMeaningOfLifeEither(): Either[Throwable, Int] =
    Try { computeMeaningOfLife() }.toEither

  // Invoke computeMeaningOfLife on the above threadPool by calling execute
  def computeMolOnThreadPool(): Unit =
    threadPool execute (() => computeMeaningOfLifeEither())

  /**
   * The problem is that this is a side-effecting operation, as it is returning
   * Unit. So it's really hard to get the results of the computation when it is
   * running on some thread that's not managed by Cats Effect.
   *
   * We would like to get hold of the result and somehow lift (i.e. wrap) it
   * into IO, so we can compose it with other IOs. This is where IO.async
   * comes in to the rescue.
   *
   * IO.async is a complex structure, with a lot of magic happening under the
   * hood.
  */

  // Lift computation to an IO. async_ suspends an asynchronous side-effect in IO
  // IO.async_ creates an effect. Executing the effect semantically blocks the
  // fiber that runs the effect UNTIL the callback is evaluated (invoked) from
  // some other thread, through some notification mechanism.
  // async_ is a Foreign Function Interface (FFI)
  val asyncMolIO: IO[Int] =
    IO.async_ { (callback: Callback[Int]) => // CE thread blocks (semantically) until this callback is invoked (by some other thread)
      threadPool execute { () =>  // computation not managed by CE
        val result: Either[Throwable, Int] = computeMeaningOfLifeEither()
        callback(result)  // CE thread is notified of the result.
    } // When CE gets the notification, CE fiber takes that result and stores it in IO[Int]
  }

  /**
   * Run:
   * override def run: IO[Unit] = asyncMolIO.myDebug >> IO(threadPool.shutdown())
   *
   * Output:
   * [pool-1-thread-1] computing the meaning of life on some other thread...
   * [io-compute-3] 42
   *
   * Analysis:
   * [io-compute-3] is a tag specific to cats-effect, while [pool-1-thread-1]
   * is a non-CE thread. Using IO.async_, we have somehow magically pulled
   * a result that was computed on a non CE-thread onto an IO effect managed by
   * CE. So async_ is a method to bring an asynchronous computation from another
   * thread pool into CE. For that reason, async_ is a Foreign Function
   * Interface (FFI).
   */

  //---------------------------------------------------------------------------
  /**
   * Exercise
   * Generalize the above for any computation.
   */
  def asyncToIO[A](computation: () => A)(using ec: ExecutionContext): IO[A] =
    IO.async_[A] { (callback: Callback[A]) => // need to return Unit here
      ec execute { () =>
        // if we write val result = computation(), then result will have type A. But we want Either
        val result: Either[Throwable, A] = Try(computation()).toEither
        callback(result)
      }
    }

  // Use this generalized function to redefine the mol function above.
  val asyncMolIO_v2: IO[Int] = asyncToIO(computeMeaningOfLife)

  //---------------------------------------------------------------------------
  /**
   * We can lift ANY asynchronous computation into an IO. It doesn't have to be
   * on a thread pool or an execution context; it can be executed anywhere else
   * other than Cats Effect.
   */

  /**
   * Lift an async computation as a Future into an IO
   */
  def convertFutureToIO[A](future: => Future[A])(using ec: ExecutionContext): IO[A] =
    IO.async_[A]      { (callback: Callback[A]) => // need to return Unit
      future onComplete { tryResult =>  // instead of ec.execute we have future.onComplete
        val result = tryResult.toEither // Convert tryResult to Either so we can invoke callback
        callback(result)
      }
    }

  // This pattern is so prevalent in CE that CE provides named IO.fromFuture
  // to make such conversions, as shown below.

  lazy val molFuture: Future[Int] = Future { computeMeaningOfLife() }
  val asyncMolIO_v3: IO[Int] = convertFutureToIO(molFuture)

  // molFuture is wrapped in an IO because not all programmers define their futures
  // as lazy vals. If it's not a lazy val, it will start executing immediately.
  // Wrapping it in an IO delays the execution.
  val asyncMolIO_v4: IO[Int] = IO fromFuture IO(molFuture)

  //---------------------------------------------------------------------------
  // Exercise: a never-ending IO
  val neverEndingIO: IO[Int] = IO.async_[Int](_ => ())  // no callback, so never finishes

  // My own code for a generalized never-ending IO with a type parameter A
  def generalNeverEndingIO[A](io: IO[A]): IO[A] = IO.async_[A](_ => ())

  // CE library provides a built-in method for a never-ending IO
  // We can use any type parameter such as Int, String, etc.
  val neverEndingIO_v2: IO[Int] = IO.never

  //---------------------------------------------------------------------------
  /**
   * FULL ASYNC
   *
   * The async_ method takes a callback from A to Unit. We can have a more
   * general version with much more control, but with an even more complicated
   * mechanism. So far we haven't dealt with cancellation with async_.
   * Cancellation adds another dimension of complexity.
   *
   * The full async calls provides the ability to run a finalizer to handle
   * cancellations.
   */

  def demoAsyncCancellation(): IO[Unit] = {
    val asyncMeaningOfLife_v2: IO[Int] = IO.async { (callback: Callback[Int]) =>
      /*
        Result type of callback has to be IO[Option[IO[Unit]]]
        Why do we need such a complicated nested type?
        - Finalizer in case computation gets canceled
        - finalizers are of type IO[Unit]
        - But we have the option of not specifying a finalizer, making them
          optional, hence Option[IO[Unit]]
        - But creating an Option by allocating its memory is an effectful
          operation. Hence we wrap it in an IO, leading to IO[Option[IO[Unit]]]
       */
      IO {
        threadPool execute { () =>
          val result = computeMeaningOfLifeEither()
          callback(result)  // Unit
        }
      } // This has type IO[Unit]. We need to turn it into IO[Option[IO[Unit]]]. So map it using as
      .as(Some(IO("Canceled!").myDebug.void))  // finalizer that will get triggered if computation gets canceled
    }

    for {
      fib <- asyncMeaningOfLife_v2.start
      _   <- IO.sleep(500.millis) >> IO("cancelling...").myDebug >> fib.cancel
      _   <- fib.join
    } yield ()
  }

  /**
   * Run:
   * override def run: IO[Unit] = demoAsyncCancellation() >> IO(threadPool.shutdown())
   *
   * Output:
   * [io-compute-1] cancelling...
   * [io-compute-1] Canceled!
   * [pool-1-thread-1] computing the meaning of life on some other thread...
   *
   * Analysis:
   * Even after we cancel the IO effect (on the Cats Effect thread io-compute-1,
   * the computation of the meaning of life continues on the other thread
   * because we have no way of interrupting the external thread. But what we
   * care about is for our IOs to be properly canceled, and that takes place.
   *
   * If we would like to interrupt the external thread, we would have to use
   * machinery such as Thread.interrupt if we are using a Java thread pool.
   *
   * But the bottom line is that we can run a finalizer if the asynchronous
   * computation is complicated enough that it gets canceled, so we can handle
   * the cancellation gracefully and carry on.
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = demoAsyncCancellation() >> IO(threadPool.shutdown())
}
