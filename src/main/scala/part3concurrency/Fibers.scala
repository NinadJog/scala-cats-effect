package part3concurrency

import cats.effect.{Fiber, IO, IOApp, Outcome}

object Fibers extends IOApp.Simple {

  val meaningOfLife: IO[Int] = IO.pure(42)
  val favLang: IO[String] = IO.pure("Scala")

  import utils._ // Import the myDebug method

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
  val someIOOnAnotherThread = runOnSomeOtherThread(meaningOfLife)
  val someResultFromAnotherThread = someIOOnAnotherThread flatMap {
    case Succeeded(effect) => effect
    case Errored(e) => IO(0)
    case Canceled() => IO(0)
  }

  // Example of Errored
  def throwOnAnotherThread(): IO[Outcome[IO, Throwable, Nothing]] =
    for
      fib <- IO.raiseError(new RuntimeException("no number for you")).start
      result <- fib.join
    yield
      result

  /* Call this function:
     throwOnAnotherThread().myDebug.void
     Output: [io-compute-2] Errored(java.lang.RuntimeException: no number for you)
   */

  //---------------------------------------------------------------------------
  // Canceling a fiber

  import scala.concurrent.duration._ // So we can use 1.second

  def testCancel: IO[Outcome[IO, Throwable, String]] = {
    // Here's a chain of computations
    val task =
      IO("starting").myDebug >>
        IO.sleep(1.second) >> // same as IO(Thread.sleep(1000))
        IO("done").myDebug // does not get printed if the task is canceled

    // Set up a finalizer. This is run on the same thread as task, when task is canceled.
    // The finalizer takes an effect, in this case an IO. The finalizer allows us to
    // free up resources such as file handles, sockets, ports, etc.
    val taskWithCancellationHandler = task onCancel IO("I'm being canceled!").myDebug.void

    // Start the chain of computations on a separate thread but then cancel it
    // (We can instead use task.start if we don't want the cancellation handler)
    for
      fib <- taskWithCancellationHandler.start // starts it on a different thread.
      _ <- IO.sleep(500.millis) >> IO("cancelling").myDebug // runs on the calling thread (main thread)
      _ <- fib.cancel // Sends signal to CE runtime to stop execution of fiber
      result <- fib.join // Wait on the calling thread for the fiber to join
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

  //---------------------------------------------------------------------------
  // Exercises

  /**
   * 1. Write a function that runs an IO on another thread and depending
   * upon the result of the fiber
   *    - Return the result in an IO
   *    - if errorred or canceled, return a failed IO
   */
  def processResultsFromFiber[A](io: IO[A]): IO[A] = {
    for
      fib <- io.myDebug.start // spawn a fiber
      result <- fib.join
      r <- result match {
        case Succeeded(effect) => effect
        case Errored(ex) => IO.raiseError(ex)
        case Canceled() => IO.raiseError(new RuntimeException("Computation canceled"))
      }
    yield r
  }

  def testExercise1(): IO[Unit] = {
    val aComputation =
      IO("starting").myDebug >>
        IO.sleep(1.second) >>
        IO("done").myDebug >>
        IO(42)
    processResultsFromFiber(aComputation).void
  }
  
  //---------------------------------------------------------------------------
  /**
   * 2. Write a function that takes two IOs, runs them on different fibers,
   * and returns an IO with a tuple containing both results
   *    - If both IOs return successfully, tuple their results
   *    - If the first IO returns an error, raise that error (ignoring the second IO's result/error)
   *    - If the first IO doesn't error but the second one does, raise that error.
   *    - If either IO is cancelled, raise a runtime exception.
   */
  def tupleIOs[A, B](ioa: IO[A], iob: IO[B]): IO[(A, B)] =
    for
      fib1 <- ioa.start
      fib2 <- iob.start
      a <- fib1.join
      b <- fib2.join
      result <- (a, b) match {
        case (Succeeded(effect1), Succeeded(effect2)) =>
          for // unpack (extract) effects from IO wrappers
            eff1 <- effect1
            eff2 <- effect2
          yield (eff1, eff2)
        case (Errored(ex), _) => IO.raiseError(ex)
        case (_, Errored(ex)) => IO.raiseError(ex)
        case _ => IO.raiseError(new RuntimeException("Some computation cancelled"))
      }
    yield result

  // Instructor's version has the same logic but it separates the gathering of
  // the result from the fibers and processing that result. This solution is
  // cleaner and easier to read although my solution is more concise.
  def tupleIOs_v2[A, B](ioa: IO[A], iob: IO[B]): IO[(A, B)] = {
    val result = for
      fib1 <- ioa.start
      fib2 <- iob.start
      a <- fib1.join
      b <- fib2.join
    yield (a, b)

    result flatMap {
      case (Succeeded(fa), Succeeded(fb)) =>
        for // unpack (extract) effects from IO wrappers
          eff1 <- fa
          eff2 <- fb
        yield (eff1, eff2)
      case (Errored(ex), _) => IO.raiseError(ex)
      case (_, Errored(ex)) => IO.raiseError(ex)
      case _ => IO.raiseError(new RuntimeException("Some computation cancelled"))
    }
  }

  def testExercise2(): IO[Unit] = {
    val firstIO = IO.sleep(2.seconds) >> IO(1).myDebug
    val secondIO = IO.sleep(3.second) >> IO(2).myDebug

    tupleIOs(firstIO, secondIO).myDebug.void
  }
  //---------------------------------------------------------------------------

  /**
   * 3. Write a function that adds a timeout to an IO.
   *    - IO runs on a fiber
   *    - If the timeout duration passes, the fiber is cancelled
   *    - the method returns an IO[A] which contains
   *      - the original value if computation is successful before the timeout signal
   *      - the exception if the computation fails before the timeout
   *      - a RuntimeException if it times out (i.e. is cancelled by the timeout)
   */
  // My version with a single for comprehension. Note that we join the computation
  // fiber fib only AFTER the entire timeout duration is finished. If the timeout
  // duration is long, this strategy is inefficient, as the computation results
  // are presented only after the timeout duration even if they might be ready
  // beforehand.
  def timeout[A](io: IO[A], duration: FiniteDuration): IO[A] =
    for
      fib     <- io.start // spawn a fiber to execute this io.
      _       <- IO.sleep(duration) >> fib.cancel // runs on the calling thread (main thread)
      result  <- fib.join // Wait on the calling thread for the fiber to join
      r       <- result match {
        case Succeeded(a) => a
        case Errored(ex) => IO.raiseError(ex)
        case Canceled() => IO.raiseError(new RuntimeException("Computation canceled"))
      }
    yield r

  //------------------------------
  // Instructor's version. Logically the same as my version above
  // Waits for the timeout duration to pass before presenting the computation results
  def timeout_v2[A](io: IO[A], duration: FiniteDuration): IO[A] = {
    val computation = for
      fib     <- io.start // spawn a fiber to execute this io.
      _       <- IO.sleep(duration) >> fib.cancel // runs on the calling thread (main thread)
      result  <- fib.join // Wait on the calling thread for the fiber to join
    yield result

    computation flatMap {
      case Succeeded(a) => a
      case Errored(ex)  => IO.raiseError(ex)
      case Canceled()   => IO.raiseError(new RuntimeException("Computation canceled"))
    }
  }

  //------------------------------
  /**
   * Does not wait for timeout before presenting computation results
   *
   * If the timeout is very long, should we wait that long before presenting
   * the results of the computation? We can skip the wait by starting the
   * cancellator fiber on a different thread, as shown below.
   *
   * But be careful with this version, as fibers can leak. (It's not the fibers that
   * leak but the resources associated with them.) Here we don't have a handle
   * on the cancellator fiber and we are not joining on it.
   *
   * If we run this version with aComputation, the "done" and "42" appear soon
   * without waiting for the timeout.
   */
  def timeout_v3[A](io: IO[A], duration: FiniteDuration): IO[A] = {
    val computation = for
      fib <- io.start // spawn a fiber to execute this io.
      _ <- (IO.sleep(duration) >> fib.cancel).start // start cancellator fiber on a different thread. careful - fibers can leak
      result <- fib.join // Join the computation fiber without waiting for the cancellator fiber
    yield result

    computation flatMap {
      case Succeeded(a) => a
      case Errored(ex) => IO.raiseError(ex)
      case Canceled() => IO.raiseError(new RuntimeException("Computation canceled"))
    }
  }
  //------------------------------
  def testExercise3(): IO[Unit] = {
    val aComputation =
      IO("starting").myDebug  >>
      IO.sleep(1.second)      >>
      IO("done").myDebug      >>
      IO(42)
    timeout_v2(aComputation, 500.millis).myDebug.void // or 2.seconds
  }

  /* Output from running testExercise3() with timeout 2 seconds. Some of the
     components of aComputation ran on different threads.
        [io-compute-1] starting
        [io-compute-1] done
        [io-compute-3] 42

     Output when the timeout period (500 ms) is less than the computation time (1s):
        [io-compute-2] starting
        java.lang.RuntimeException: Computation canceled
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = testExercise3()
  /*
    With timeout = 2 seconds, output is:
    [io-compute-3] starting
    [io-compute-3] done
    [io-compute-3] 42
   */

  // override def run: IO[Unit] = testExercise2()
  /* Output is: Notice that the two IOs run on different fibers (threads)
    [io-compute-2] 1
    [io-compute-1] 2
    [io-compute-1] (1,2)
   */

  // override def run: IO[Unit] = testExercise1()
  /* Output is:
    [io-compute-2] starting
    [io-compute-2] done
    [io-compute-2] 42
   */

  // override def run: IO[Unit] = testCancel.myDebug.void
}
