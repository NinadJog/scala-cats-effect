package part3concurrency

import cats.effect.{Fiber, IO, IOApp, Outcome}

object Fibers extends IOApp.Simple {

  val meaningOfLife:  IO[Int]     = IO.pure(42)
  val favLang:        IO[String]  = IO.pure("Scala")

  import utils._  // Import the myDebug method
  val sameThreadIOs: IO[Unit] =
    for
      _ <- meaningOfLife.myDebug
      _ <- favLang.myDebug
    yield ()

  /* Output shows that they run on the same thread when unsafeRun is called through run
      [io-compute-2] 42
      [io-compute-2] Scala
   */

  // Fibers
  // A fiber is an incredibly lightweight data structure that DESCRIBES a
  // computation that will run on SOME thread managed by the Cats Effect runtime.
  // It's a low-level concurrency primitive.

  // Of the 3 types that a Fiber takes, the first is the effect type that will
  // run on a thread, the second is the error type and the third the result
  // type.
  // Leaving this method unimplemented because it's almost impossible to
  // create fibers manually. They can only be created through cats-effect
  // APIs
  def createFiber: Fiber[IO, Throwable, String] = ???

  // The fiber created with the start method is wrapped in an IO because
  // creating a fiber is an effectful operation: The starting of a computation
  // or the allocation of a fiber might produce side-effects.
  // The fiber is not actually started but the fiber allocation is wrapped in
  // another effect.
  val aFiber: IO[Fiber[IO, Throwable, Int]] = meaningOfLife.myDebug.start

  val differentThreadIOs: IO[Unit] =
    for
      _ <- aFiber
      _ <- favLang.myDebug
    yield ()

  /* Runs on different threads because one of the IOs is a fiber!
    [io-compute-1] Scala
    [io-compute-3] 42
   */

  // Managing the lifecycle: joining a fiber
  // joining means waiting for the fiber to finish in a purely functional way

  def runOnSomeOtherThread[A](io: IO[A]): IO[Outcome[IO, Throwable, A]] =
    for
      fib <- io.start // turns into an effect that will be performed on a different thread
      result <- fib.join // an effect that waits for the fiber fib to terminate
    yield result
  /*
    Return type is === IO[Result type of fib.join]
    fib.join has type Outcome[IO, Throwable, A]
    So return type is IO[Outcome[IO, Throwable, A]]

    possible outcomes
    - success with an IO
    - failure with an exception
    - canceled

    Result of running this method:
    runOnSomeOtherThread(meaningOfLife).myDebug.void
    // IO(Succeeded(IO(42)))

    Fibers are a very low-level concurrency primitive in cats-effect that
    allow cancellation at specific points in the computation.
   */

  //---------------------------------------------------------------------------
  // Managing successes, failures, and cancellations

  import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}

  // Handling success, error, and cancellation using a partial function
  val someIOOnAnotherThread       = runOnSomeOtherThread(meaningOfLife)
  val someResultFromAnotherThread = someIOOnAnotherThread.flatMap {
    case Succeeded(effect)  => effect
    case Errored(e)         => IO(0)
    case Canceled()         => IO(0)
  }

  // Example of Errored
  def throwOnAnotherThread(): IO[Outcome[IO, Throwable, Nothing]] =
    for
      fib     <- IO.raiseError(new RuntimeException("no number for you")).start
      result  <- fib.join
    yield
      result

  /* Call this function:
     throwOnAnotherThread().myDebug.void
     Output: [io-compute-2] Errored(java.lang.RuntimeException: no number for you)
   */

  //---------------------------------------------------------------------------
  // Canceling a fiber

  import scala.concurrent.duration._  // So we can use 1.second

  def testCancel = {
    // Here's a chain of computations
    val task =
      IO("starting").myDebug  >>
      IO.sleep(1.second)      >>  // same as IO(Thread.sleep(1000))
      IO("done").myDebug          // does not get printed if the task is canceled

    // Set up a finalizer. This is run on the same thread as task, when task is canceled.
    // The finalizer takes an effect, in this case an IO. The finalizer allows us to
    // free up resources such as file handles, sockets, ports, etc.
    val taskWithCancellationHandler = task onCancel IO("I'm being canceled!").myDebug.void

    // Start the chain of computations on a separate thread but then cancel it
    // (We can instead use task.start if we don't want the cancellation handler)
    for
      fib     <- taskWithCancellationHandler.start // starts it on a different thread.
      _       <- IO.sleep(500.millis) >> IO("cancelling").myDebug // runs on the calling thread (main thread)
      _       <- fib.cancel // Sends signal to CE runtime to stop execution of fiber
      result  <- fib.join   // Wait on the calling thread for the fiber to join
    yield
      result
  }
  /* Output when we run testCancel.myDebug.void with task.start
    [io-compute-2] starting
    [io-compute-3] cancelling
    [io-compute-1] Canceled()

    Note that the "done" message from the task does not get printed, as the task
    gets canceled before that.

    Output when we use taskWithCancellationHandler.start:
    [io-compute-3] starting
    [io-compute-1] cancelling
    [io-compute-1] I'm being canceled!
    [io-compute-1] Canceled()
   */

  override def run: IO[Unit] =
    testCancel //
      .myDebug
      .void
}
