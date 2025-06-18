package part5polymorphic

import cats.effect.kernel.Async
import cats.effect.{Concurrent, Deferred, Fiber, FiberIO, IO, IOApp, Outcome, OutcomeIO, Ref, Spawn}

import scala.concurrent.duration.*
import utils.general.*

object PolymorphicCoordination extends IOApp.Simple {

  // Typeclass: Concurrent: Ref + Deferred for ANY effect type
  //
  // Describes the capability of Cats Effect to create the fundamental concurrency
  // coordination primitives Ref and Deferred for any effect type

  trait MyConcurrent[F[_]] extends Spawn[F] {
    // this is an effect that when evaluated, produces a purely functional atomic
    // reference with the value a stored inside.
    def ref[A](a: A): F[Ref[F, A]]

    // Produces an effect which, when evaluated, creates the Deferred concurrency primitive.
    def deferred[A]: F[Deferred[F, A]]
  }

  // Spawn was a typeclass that had the ability to create fibers. Concurrent is
  // a stronger typeclass that also has the ability to create concurrency primitives,
  // in this case Ref and Deferred. In terms of Ref and Deferred we can define all
  // the other concurrency primitives.

  // The following typeclass instance has ALL the capabilities of Spawn plus
  // ref and deferred.
  val concurrentIO = Concurrent[IO] // fetches the given (implicit) instance of Concurrent[IO]

  val aDeferred     = Deferred[IO, Int] // requires the presence of a given (implicit) Concurrent[IO] in scope.
  val aDeferred_v2  = Concurrent[IO].deferred[Int]  // same

  val aRef    = IO ref 42
  val aRef_v2 = Concurrent[IO] ref 42  // same

  // capabilities of the Concurrent typeclass:
  // pure, map/flatMap, raiseError, uncancelable, start (fibers), ref, deferred

  //---------------------------------------------------------------------------

  /**
   * Exercise from the Defers lesson. Generalize it for effects other than IOs.
   * Here's the generalized version followed by the original specific solution for IO.
   *
   * An alarm notification with two simultaneous IOs
   * - one fiber increments a counter every second (a clock)
   *   (hint: counter is the shared state, so use a Ref)
   * - another that waits for the counter to reach 10, then prints a message
   *   saying, "time's up!"
   */
  // Import extension methods
  import cats.syntax.flatMap._      // flatMap
  import cats.syntax.functor._      // map
  import cats.effect.syntax.spawn._ // start

  def polymorphicEggBoiler[F[_]](using concurrent: Concurrent[F]): F[Unit] = {

    // The notifier is a consumer. The signal is a Deferred Unit because
    // there's no content to be sent; just a notification that 10 seconds
    // have passed.
    def eggReadyNotification(signal: Deferred[F, Unit]): F[Unit] =
      for {
        _ <- concurrent.pure("[notifier] Egg boiling on some other fiber; waiting...").myDebug
        _ <- signal.get // blocks the calling fiber
        _ <- concurrent.pure(s"[notifier] Egg is ready!").myDebug
      } yield ()

    // The clock acts as a producer. This effect chain ticks a clock every second
    // and after 10 seconds pass, it sends a notification to the other effect
    // chain, the eggReadyNotification.
    def tickingClock(ticks: Ref[F, Int], signal: Deferred[F, Unit]): F[Unit] =
      for {
        _       <- unsafeSleep[F, Throwable](1.second)  // Pass [F, Throwable] so as not to confuse the compiler
        seconds <- ticks.updateAndGet(_ + 1)
        _       <- concurrent.pure(s"[clock] $seconds").myDebug
        // Added .void in the following line to change return type to Unit, since the return type
        // of the complete() method is Boolean. Otherwise we get a compiler error saying map
        // does not support the return type 'F[Boolean] | F[Unit]'
        _       <- if seconds >= 10 then signal.complete(()).void else tickingClock(ticks, signal)
      } yield ()

    for {
      ticksRef    <- concurrent ref 0 // same as 'Concurrent[F] ref 0'
      signal      <- concurrent.deferred[Unit]
      fibNotifier <- eggReadyNotification(signal).start // works because we imported the start extension method
      fibClock    <- tickingClock(ticksRef, signal).start
      _           <- fibClock.join
      _           <- fibNotifier.join
    } yield ()
  }

  //---------------------------------------------------------------------------
  // This is the specific version; same as the one from the Defers section of part 4.
  def ioEggBoiler(): IO[Unit] = {

    // The notifier is a consumer. The signal is a Deferred Unit because
    // there's no content to be sent; just a notification that 10 seconds
    // have passed.
    def eggReadyNotification(signal: Deferred[IO, Unit]): IO[Unit] =
      for {
        _ <- IO("[notifier] Egg boiling on some other fiber; waiting...").myDebug
        _ <- signal.get // blocks the calling fiber
        _ <- IO(s"[notifier] Egg is ready!").myDebug
      } yield ()

    // The clock acts as a producer
    def tickingClock(ticks: Ref[IO, Int],
                     signal: Deferred[IO, Unit]): IO[Unit] =
      for {
        _ <- IO.sleep(1.second)
        seconds <- ticks updateAndGet (_ + 1)
        _ <- IO(s"[clock] $seconds").myDebug
        _ <- if seconds >= 10 then
          IO(s"[clock] 10 seconds have passed").myDebug >>
            (signal complete()).void // unblocks the calling fiber
        else
          tickingClock(ticks, signal) // clock continues ticking if < 10 seconds
      } yield ()

    for {
      ticksRef    <- Ref[IO] of 0
      signal      <- Deferred[IO, Unit] // signal has type Deferred[IO, Int]
      fibNotifier <- eggReadyNotification(signal).start
      fibClock    <- tickingClock(ticksRef, signal).start
      _           <- fibClock.join
      _           <- fibNotifier.join
    } yield ()
  }

  //===========================================================================
  // Exercises
  /**
   * Exercise 1. Generalize ourRacePair for any Effect type, not just IO.
   */

  type RaceResult[F[_], A, B] =
    Either[(Outcome[F, Throwable, A], Fiber[F, Throwable, B]),  // (winner outcome, loser fiber)
           (Fiber[F, Throwable, A], Outcome[F, Throwable, B])]  // (loser fiber, winner outcome)

  // outcome of either fa or fb
  type EitherOutcome[F[_], A, B] = Either[Outcome[F, Throwable, A], Outcome[F, Throwable, B]]

  /**
   * Add finalizer to complete signal regardless of result of completion of fiber
   * The blocking call 'signal.get' should be cancelable because we would
   * like to cancel the race pair from a different fiber if signal.get is
   * taking forever. So mark the entire for comprehension as uncancelable
   * but make signal.get uncancelable by wrapping it in poll.
   *
   * What if fa and fb are two effect chains in which some F's are cancelable
   * and others are not?
   */
  import cats.effect.syntax.monadCancel._ // guaranteeCase extension method

  def ourRacePair[F[_], A, B](fa: F[A], fb: F[B])(using concurrent: Concurrent[F]): F[RaceResult[F, A, B]] = {

    // Helper method to cancel both fibers. Start separate fibers to cancel
    // fibA and fibB because we want to cancel both of them at the same time.
    def cancelFibers(fibA: Fiber[F, Throwable, A], fibB: Fiber[F, Throwable, B]): F[Unit] =
      for { // Start separate fibers to cancel fibA and fibB because we want both of them to be canceled at the same time
        cancelFibA <- fibA.cancel.start
        cancelFibB <- fibB.cancel.start
        _ <- cancelFibA.join // These joins can happen in the future; we don't care
        _ <- cancelFibB.join
      } yield ()

    //-----------------------
    concurrent.uncancelable { poll =>
      for {
        signal <- concurrent.deferred[EitherOutcome[F, A, B]]  // I wrote: Deferred[F, EitherOutcome[F, A, B]]
        fibA <- (fa guaranteeCase (outcomeA => (signal complete Left(outcomeA)).void)).start
        fibB <- (fb guaranteeCase (outcomeB => (signal complete Right(outcomeB)).void)).start
        result <- poll(signal.get) onCancel cancelFibers(fibA, fibB) // blocking call waits for either fibA or fibB to complete. should be cancelable

      } yield result match {
        case Left(outcomeA) => Left(outcomeA, fibB) // A won
        case Right(outcomeB) => Right(fibA, outcomeB) // B won
      }
    }
  } // method ourRacePair

  /**
   * The above racePair implementation actually sits inside the Concurrent
   * typeclass. Since the Spawn typeclass has the ability to create fibers,
   * racePair is DEFINED in the Spawn typeclass.
   *
   * But since racePair is implemented in terms of ref and deferred, and those
   * are defined in the Concurrent typeclass, the IMPLEMENTATION of racePair
   * sits in the GenConcurrent typeclass.
   */

  //---------------------------------------------------------------------------
  // Original code from the Defers lesson for the IO effect for Exercise 1.

  type RaceResultIO[A, B] =
    Either[(OutcomeIO[A], FiberIO[B]),
      (FiberIO[A], OutcomeIO[B])]

  // outcome of either io
  type EitherOutcomeIO[A, B] = Either[OutcomeIO[A], OutcomeIO[B]]

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
  def ourRacePairIO[A, B](ioa: IO[A], iob: IO[B]): IO[RaceResultIO[A, B]] = {

    // Helper method to cancel both fibers. Start separate fibers to cancel
    // fibA and fibB because we want to cancel both of them at the same time.
    def cancelFibers(fibA: FiberIO[A], fibB: FiberIO[B]): IO[Unit] =
      for { // Start separate fibers to cancel fibA and fibB because we want both of them to be canceled at the same time
        cancelFibA <- fibA.cancel.start
        cancelFibB <- fibB.cancel.start
        _ <- cancelFibA.join // These joins can happen in the future; we don't care
        _ <- cancelFibB.join
      } yield ()

    //-----------------------
    IO.uncancelable { poll =>
      for {
        signal <- Deferred[IO, EitherOutcomeIO[A, B]]
        fibA <- (ioa guaranteeCase (outcomeA => (signal complete Left(outcomeA)).void)).start
        fibB <- (iob guaranteeCase (outcomeB => (signal complete Right(outcomeB)).void)).start
        result <- poll(signal.get) onCancel cancelFibers(fibA, fibB) // blocking call waits for either fibA or fibB to complete. should be cancelable

      } yield result match {
        case Left(outcomeA) => Left(outcomeA, fibB) // A won
        case Right(outcomeB) => Right(fibA, outcomeB) // B won
      }
    }
  }

  //===========================================================================
  /**
   * Exercise 2. Generalize the Mutex concurrency primitive for any F.
   * This is implemented in Mutex.scala as GenMutex.
   */



  //---------------------------------------------------------------------------
  override def run: IO[Unit] = polymorphicEggBoiler[IO]
}
