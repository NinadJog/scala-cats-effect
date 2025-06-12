package part4coordination

import cats.effect.kernel.Deferred
import cats.effect.{FiberIO, IO, IOApp, OutcomeIO, Ref}
import scala.concurrent.duration.*
import utils.*

/**
 * A Deferred is a concurrency primitive for waiting for an effect (an IO)
 * while some other effect completes with a value from some other fiber.
 * It's like Promises but in a purely functional way.
 *
 * Deferred is important for two reasons.
 * 1. Deferred can block (semantically) on a fiber in a purely functional way
 * 2. As an inter-fiber communication mechanism because multiple fibers
 *    can send messages or values through the signal get and complete methods
 *
 * Its two methods are:
 * get:       blocks the fiber (semantically) until a value is present
 * complete:  inserts a value that can be read by the blocked fibers
 *
 * Uses
 * - Allows inter-fiber communication
 * - Avoids busy waiting
 * - Maintains thread safety
 *
 * Main use-cases
 * - Producer-consumer kind of problems
 * - Sending data between fibers
 * - Notification mechanisms
 */

object Defers extends IOApp.Simple {

  val aDeferred:    IO[Deferred[IO, Int]] = Deferred[IO, Int] // apply method from companion object
  val aDeferred_v2: IO[Deferred[IO, Int]] = IO.deferred[Int]  // same

  // Deferred has two main methods: get and complete. get blocks the calling
  // fiber (semantically) until some other fiber completes the Deferred with
  // a value.
  val reader: IO[Int] = aDeferred flatMap { signal =>
    signal.get // blocks the calling fiber (semantically) until this deferred (signal) completes
  }

  // shorter notation
  val reader_v2: IO[Int] = aDeferred flatMap { _.get }  // same as above

  // A writer is an effect that completes a signal (i.e. a deferred) with some value.
  val writer: IO[Boolean] = aDeferred flatMap { signal => signal complete 42}

  // If the reader and writer are run on different threads, the reader fiber will
  // block (semantically) until the writer fiber completes. Upon completion,
  // the writer fiber will send a signal to the reader fiber, waking it up.

  // A drawback of Deferred is that it's not useful on its own, but is useful
  // when it's embedded in some other flow.

  //---------------------------------------------------------------------------
  // Example 1:
  // A one-time producer-consumer demo similar to the Promise demo.

  def demoDeferred(): IO[Unit] = {

    def consumer(signal: Deferred[IO, Int]): IO[Unit] = for {
      _     <- IO("[consumer] waiting for value...").myDebug
      value <- signal.get  // blocks the calling fiber
      _     <- IO(s"[consumer] obtained value $value").myDebug
    } yield ()

    def producer(signal: Deferred[IO, Int]): IO[Unit] = for {
      _             <- IO("[producer] crunching numbers...").myDebug
      _             <- IO.sleep(1.second)
      meaningOfLife <- IO(42)
      _             <- IO(s"[producer] complete: $meaningOfLife").myDebug
      _             <- signal complete meaningOfLife // unblocks the calling fiber
    } yield ()

    for {
      signal      <- Deferred[IO, Int]    // signal has type Deferred[IO, Int]
      fibConsumer <- consumer(signal).start
      fibProducer <- producer(signal).start
      _           <- fibProducer.join
      _           <- fibConsumer.join
    } yield ()
  }
  /**
   * Output:
   * [io-compute-0] [consumer] waiting for value...
   * [io-compute-3] [producer] crunching numbers...
   * [io-compute-3] [producer] complete: 42
   * [io-compute-1] [consumer] obtained value 42
   */

  //---------------------------------------------------------------------------
  // Example 2.
  // Simulate downloading some content and getting a notification when it's done

  val fileParts = List("I ", "love S", "cala", " with Cat", "s Effect!<EOF>")

  /**
   * Two IOs (effects) on different fibers. One will wait for the file content
   * to be downloaded until we get to the EOF token, at which point we
   * signal to the console that the file download has been completed.
   *
   * The other effect will take each piece of the content and will "download"
   * it every second. We use an atomic Ref to modify the current state of the
   * file download. Traverse the fileParts list one element at a time and
   * combine it whatever we have in the current reference.
   *
   * Implementation #1: Use Ref but not Deferred
   */
  def fileNotifierWithRef(): IO[Unit] = {

    // Chain of effects that simulates downloading the file from the web
    def downloadFile(contentRef: Ref[IO, String]): IO[Unit] = {
      fileParts
        .map { part =>
            IO(s"[downloader] got part '$part'").myDebug  >>
            IO.sleep(1.second)                            >>
            contentRef.update (_ + part)
        }
        .sequence   // has type IO[List[Unit]]. we flipped from List[IO[Unit]]
        .void
    }

    // Another chain of effects to notify the console when file downloading completes
    def notifyFileComplete(contentRef: Ref[IO, String]): IO[Unit] = for {
      file  <-  contentRef.get  // file has type String
      _     <-  if file endsWith "<EOF>" then
                  IO("[notifier] File download complete!").myDebug
                else
                  IO("[notifier] downloading...").myDebug    >>
                  (IO sleep 500.millis)   >>  // If we remove sleep, the console will be flooded with "downloading" messages
                  notifyFileComplete(contentRef)
    } yield ()

    // Start the two flows (effect chains) on two separate fibers
    for {
      contentRef  <- Ref[IO] of ""
      fibNotify   <- notifyFileComplete(contentRef).start
      fibDownload <- downloadFile(contentRef).start
      _           <- fibDownload.join
      _           <- fibNotify.join
    } yield ()
  }

  /**
   * Output:
   *
   * [io-compute-3] [notifier] downloading...
   * [io-compute-2] [downloader] got part 'I '
   * [io-compute-3] [notifier] downloading...
   * [io-compute-3] [notifier] downloading...
   * ...
   * [io-compute-3] [downloader] got part 's Effect!<EOF>'
   * [io-compute-3] [notifier] downloading...
   * [io-compute-0] [notifier] downloading...
   * [io-compute-0] [notifier] File download complete!
   */

  /**
   * Drawbacks of the above approach:
   *
   * The approach is thread-safe, but if we remove the 'IO sleep 500.millis'
   * from the file download notifier, the console will be flooded with
   * 'downloading...' messages. In other words, it will be busy waiting, so it's
   * not an efficient approach.
   *
   * Fortunately, busy waiting is a primary use case where Deferred can come
   * to the rescue. This implementation is shown next.
   */

  //---------------------------------------------------------------------------
  // Deferred solves the busy waiting problem. Refactor the file notifier
  // using Deferred
  def fileNotifierWithDeferred(): IO[Unit] = {

    def notifyFileComplete(signal: Deferred[IO, String]): IO[Unit] = for {
      _ <- IO("[notifier] downloading...").myDebug
      _ <- signal.get // blocks until the signal is completed
      _ <- IO("[notifier] File download complete!").myDebug
    } yield ()

    // The file downloader is invoked on every part of the file
    // We still need contentRef because it's a purely functional, mutable
    // data structure to store the file contents as it gets downloaded. The
    // Ref serves as the data storage, the accumulator to store the data as it
    // arrives whereas the Deferred is used for signalling between the two fibers.
    def filePartDownloader(
         part:        String,
         contentRef:  Ref[IO, String],
         signal:      Deferred[IO, String]): IO[Unit] = for {

      _             <- IO(s"[downloader] got part '$part'").myDebug
      _             <- IO sleep 1.second
      latestContent <- contentRef updateAndGet (_ + part) // update file content and get latest
      _             <- if latestContent contains "<EOF>" then signal complete latestContent
                          else IO.unit  // else do nothing
    } yield ()

    // We can parallelize the file downloader, but we will keep the code
    // sequential for the sake of simplicity.
    // The call to sequence flips the List[IO[..]] to IO[List[..]]
    for {
      contentRef  <- Ref[IO] of ""          // same as IO ref ""
      signal      <- Deferred[IO, String]   // same as IO.deferred[String]
      fibNotify   <- notifyFileComplete(signal).start
      fileTasks   <- fileParts
                      .map(filePartDownloader(_, contentRef, signal)) // List[IO[Unit]]
                      .sequence // Converts to IO[List[Unit]]
                      .start
      _           <- fibNotify.join
      _           <- fileTasks.join
    } yield ()
  }
  /**
   * The output is much more compact this time:
   *
   * [io-compute-1] [notifier] downloading...
   * [io-compute-1] [downloader] got part 'I '
   * [io-compute-1] [downloader] got part 'love S'
   * [io-compute-1] [downloader] got part 'cala'
   * [io-compute-1] [downloader] got part ' with Cat'
   * [io-compute-1] [downloader] got part 's Effect!<EOF>'
   * [io-compute-3] [notifier] File download complete!
   */

  //===========================================================================
  // Exercises

  /**
   * 1. (Medium) Write a small alarm notification with two simultaneous IOs
   * - one fiber increments a counter every second (a clock)
   *   (hint: counter is the shared state, so use a Ref)
   * - another that waits for the counter to reach 10, then prints a message
   *   saying, "time's up!" (hint: the structure of this program is similar to
   *   that of demoDeferred.)
   */
  def alarmNotification(): IO[Unit] = {

    // The notifier is a consumer. The signal is a Deferred Unit because
    // there's no content to be sent; just a notification that 10 seconds
    // have passed.
    def notifier(signal: Deferred[IO, Unit]): IO[Unit] =
      for {
        _ <- IO("[notifier] waiting for 10 seconds to pass...").myDebug
        _ <- signal.get  // blocks the calling fiber
        _ <- IO(s"[notifier] time's up!").myDebug
      } yield ()

    // The clock acts as a producer
    def tickingClock(ticks:   Ref[IO, Int],
                     signal:  Deferred[IO, Unit]): IO[Unit] =
      for {
        _       <- IO.sleep(1.second)
        seconds <- ticks updateAndGet (_ + 1)
        _       <- IO(s"[clock] $seconds").myDebug
        _       <- if seconds >= 10 then
                     IO(s"[clock] 10 seconds have passed").myDebug >>
                     (signal complete ()) // unblocks the calling fiber
                   else
                     tickingClock(ticks, signal) // clock continues ticking if < 10 seconds
      } yield ()

    for {
      ticksRef    <- Ref[IO] of 0
      signal      <- Deferred[IO, Unit]    // signal has type Deferred[IO, Int]
      fibNotifier <- notifier(signal).start
      fibClock    <- tickingClock(ticksRef, signal).start
      _           <- fibClock.join
      _           <- fibNotifier.join
    } yield ()
  }

  /**
   * [io-compute-2] [notifier] waiting for 10 seconds to pass...
   * [io-compute-0] [clock] 1
   * [io-compute-0] [clock] 2
   * ...
   * [io-compute-0] [clock] 10
   * [io-compute-0] [clock] 10 seconds have passed
   * [io-compute-1] [notifier] time's up!
   */

  //---------------------------------------------------------------------------
  /**
   * 2. (Mega hard) Implement a racePair with Deferred.
   * - Use a Deferred which can hold an Either[outcome for ioa, outcome for iob]
   * - start two fibers, one for each IO
   * - on completion (with any status), each IO needs to complete that Deferred
   *   (hint 1: use a finalizer from the Resources lesson)
   *   (hint 2: use a guarantee call to make sure the fibers complete the Deferred)
   * - what do you do in case of cancellation (the hardest part)?
   *
   */

  // Use the following type aliases from Cats Effect to simplify the type signature
  //  type FiberIO[A]   = Fiber[IO, Throwable, A]
  //  type OutcomeIO[A] = Outcome[IO, Throwable, A]

  type RaceResult[A, B] =
    Either[(OutcomeIO[A], FiberIO[B]),
           (FiberIO[A],   OutcomeIO[B])]

  // outcome of either io
  type EitherOutcome[A, B] = Either[OutcomeIO[A], OutcomeIO[B]]

  /**
   * Add finalizer to complete signal regardless of result of completion of fiber
   * The blocking call 'signal.get' should be cancelable because we would
   * like to cancel the race pair from a different fiber if signal.get is
   * taking forever. So mark the entire for comprehension as uncancelable
   * but make signal.get uncancelable by wrapping it in poll.
   *
   * What if ioa and iob are two effect chains in which some IOs are cancelable
   * and others are not?
   */
  def ourRacePair[A, B](ioa: IO[A], iob: IO[B]): IO[RaceResult[A, B]] = {

    // Helper method to cancel both fibers. Start separate fibers to cancel
    // fibA and fibB because we want to cancel both of them at the same time.
    def cancelFibers(fibA: FiberIO[A], fibB: FiberIO[B]): IO[Unit] =
      for { // Start separate fibers to cancel fibA and fibB because we want both of them to be canceled at the same time
        cancelFibA <- fibA.cancel.start
        cancelFibB <- fibB.cancel.start
        _ <- cancelFibA.join  // These joins can happen in the future; we don't care
        _ <- cancelFibB.join
      } yield ()

    //-----------------------
    IO.uncancelable { poll =>
      for {
        signal  <- Deferred[IO, EitherOutcome[A, B]]
        fibA    <- (ioa guaranteeCase (outcomeA => (signal complete Left(outcomeA)).void)).start
        fibB    <- (iob guaranteeCase (outcomeB => (signal complete Right(outcomeB)).void)).start
        result  <- poll(signal.get) onCancel cancelFibers(fibA, fibB) // blocking call waits for either fibA or fibB to complete. should be cancelable

      } yield result match {
        case Left(outcomeA) => Left(outcomeA, fibB) // A won
        case Right(outcomeB) => Right(fibA, outcomeB) // B won
      }
    }
  }

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = alarmNotification()
}
