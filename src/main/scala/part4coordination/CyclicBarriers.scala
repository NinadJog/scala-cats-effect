package part4coordination

import cats.effect.kernel.Deferred
import cats.effect.std.CyclicBarrier
import cats.effect.{IO, IOApp, Ref}

import scala.concurrent.duration.*
import scala.util.Random
import cats.syntax.parallel.*
import utils.*

/**
 * A cyclic barrier is a coordination primitive that's initialized with a count
 * and has a single method called await. The cyclic barrier semantically blocks
 * all fibers that call await until there are exactly N fibers waiting, at
 * which point the barrier unblocks all N fibers and resets to its original
 * state. The cycle continues: any other fiber that calls await will again be
 * blocked until n fibers wait on it.
 *
 * Use case: Hold off until we have a signal that we can then repeat.
 */
object CyclicBarriers extends IOApp.Simple {

  // Helper method that sleeps a random amount of time up to durationMillis.
  def sleepUpto(durationMillis: Int): IO[Unit] =
    IO sleep (Random.nextDouble * durationMillis).toInt.millis

  // Example: Signing up for a social network that's just about to be launched.
  // Assume that the network will open only when there's a sufficient quorum.
  // CyclicBarrier[IO] is Cats Effect's cyclic barrier, whereas CBarrier is our own.
  def createUser(id: Int, barrier: CBarrier /*CyclicBarrier[IO]*/): IO[Unit] = for {
    _ <- sleepUpto(500)  // sleep for at most half a second
    _ <- IO(s"[user $id] Signing up for new social network's waitlist...").myDebug
    _ <- sleepUpto(1500)
    _ <- IO(s"[user $id] On the waitlist now, can't wait!").myDebug
    _ <- barrier.await  // blocks the fiber until there are exactly N users waiting
    _ <- IO(s"[user $id] OMG I got added to the social network!").myDebug
  } yield ()

  /**
   * Announcer announces the social network, creates a cyclic barrier with a
   * count of 10 and invites n users to join.
   * If n < 10, all n users will be on the waitlist.
   * If n > 10 (say n = 14), a random 10 of those 14 users will get added to
   * the network while the other 4 will continue to be on the waitlist.
   * If n = 20, all 20 users will be added to the network, but in two batches
   * of 10 each.
   *
   * A cyclic barrier allows us to continue in batches of whatever we control.
   *
   * In the following code, CyclicBarrier[IO] is Cats Effect's cyclic barrier,
   * whereas CBarrier is our own. Both work equally well for this example.
   */
  def openNetwork(): IO[Unit] = for {
    _ <- IO(s"[announcer] Rock the JVM social network is open for registration! Launching when we have 10 users!").myDebug
    barrier <- CBarrier(10) // CyclicBarrier[IO](10)
    _ <- (1 to 14).toList parTraverse (createUser(_, barrier))
  } yield ()

  //---------------------------------------------------------------------------
  /**
   * Exercise: Implement cyclic barrier in terms of Ref + Deferred. Ignore
   * cancellations because it will complicate the code quite a bit.
   *
   * The exercises on implementations of Mutex and CountdownLatch that we did
   * earlier should serve as starting points.
   */
  abstract class CBarrier {
    def await: IO[Unit]
  }

  object CBarrier {

    // nWaiting goes down whenever a fiber calls await.
    case class State(nWaiting: Int, signal: Deferred[IO, Unit])

    /**
     * count is the number of fibers we want to block. Return type is a CBarrier
     * wrapped in an IO, just like all the other concurrency primitives in Cats
     * Effect.
     *
     * Creates the new signal as part of the state modification transaction, as
     * it creates it inside the flatMap that's part of the state modification.
     */
    def apply(count: Int): IO[CBarrier] = for {
      signal  <- Deferred[IO, Unit]
      state   <- Ref[IO] of State(count, signal)  // Ref containing the State
    } yield new CBarrier {

      // newSignal should be created OUTSIDE of state.modify
      override def await: IO[Unit] =
        Deferred[IO, Unit] flatMap { newSignal =>
          state.modify {
            // Reset cyclic barrier's count to count, create a new signal and complete the old signal
            case State(1, signal) => State(count, newSignal) -> (signal complete()).void

            // Reduce the count by 1 and run an effect to wait on the signal
            case State(n, signal) => State(n - 1, signal) -> signal.get // affected by cancellations
          }.flatten // flattens IO[IO[Unit]] to IO[Unit]
      }

      /*
        It's WRONG to use a for comprehension instead of a flatMap after
        creating a new signal, as the following code does. This code does not
        work correctly. It has two issues.

        1. Premature Signal Creation: The newSignal is created before the state
        modification happens in the for comprehension. This means:

        - A new Deferred is created every time await_v2 is called, regardless
          of whether it will be used.
        - The signal that gets stored in the state might not be the same one
          that was created at the beginning of the for comprehension
          (due to concurrency).

        2. Race Condition: Between creating the newSignal and modifying the
           state, other fibers could modify the state, leading to inconsistencies.
           The correct version (await) creates the signal during the state
           modification, making it atomic.

        This code will fail as follows when multiple fibers call it simultaneously:
        1) Each creates its own newSignal before entering the state modification.
        2) When they modify the state, they might overwrite each other's signals.
        3) Some fibers might end up waiting on the wrong signal or a signal that
        never gets completed.

      def await_v2: IO[Unit] =
        for {
          newSignal <- Deferred[IO, Unit] // create a new signal
        } yield
          state.modify {
            // Reset cyclic barrier's count to count, create a new signal and complete the old signal
            case State(1, signal) => State(count, newSignal) -> (signal complete()).void

            // Reduce the count by 1 and wait on the signal
            case State(n, signal) => State(n - 1, signal) -> signal.get // affected by cancellations
          }.flatten // flattens IO[IO[Unit]] to IO[Unit]
      */
    }

  }
  //---------------------------------------------------------------------------
  override def run: IO[Unit] = openNetwork()
}

