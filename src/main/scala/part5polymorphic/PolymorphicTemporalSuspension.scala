package part5polymorphic

import cats.effect.{Concurrent, IO, IOApp, Temporal}
import scala.concurrent.duration._
import utils.general._

// Temporal = Ability to create time-limited blocking effects.
// Fundamental operation: sleep
// Extra functionality: timeout
// Generalizes time-based code for any effect type
object PolymorphicTemporalSuspension extends IOApp.Simple {

  trait MyTemporal[F[_]] extends Concurrent[F] {
    def sleep(time: FiniteDuration): F[Unit]  // semantically blocks calling fiber for a specified time
  }
  // abilities: pure, map/flatMap, raiseError, uncancelable, start, ref/deferred, +sleep

  // Using it manually:
  val temporalIO = Temporal[IO] // fetches the given Temporal[IO] that's in scope

  val chainOfEffects: IO[String] =
    IO("Loading...").myDebug  *>
    IO.sleep(1.second)        *>
    IO("game ready!").myDebug

  val chainOfEffects_v2: IO[String] = // same as chainOfEffects
    temporalIO.pure("Loading...").myDebug   *>
    temporalIO.sleep(1.second)              *>
    temporalIO.pure("game ready!").myDebug

  // As with the other typeclasses, Temporal generalizes any effect for which
  // there is a Temporal instance in scope to also have the sleep functionality.

  //---------------------------------------------------------------------------
  /*
   * Exercise: Generalize the timeout functionality. (It's from the RacingIOs
   * section of Part 3)
   *
   * Implements timeout pattern with race. Races the given IO with another IO
   * that sleeps for the duration of the timeout. Then takes into account
   * which one wins. Returns the effect if it wins, else return a runtime
   * exception if it times out.
   */
  import cats.syntax.flatMap._
  def timeout[F[_], A](fa: F[A], duration: FiniteDuration)
                      (using temporal: Temporal[F]): F[A] = {

    val timeoutEffect:  F[Unit]             = temporal sleep duration
    val result:         F[Either[A, Unit]]  = temporal race(fa, timeoutEffect)

    result flatMap {
      case Left(value)  => temporal pure value
      case Right(_)     => temporal raiseError new RuntimeException("Computation timed out")
    }
  }
  // The timeout method is implemented in the GenTemporal typeclass itself.

  // Original Code with IO
  def timeoutIO[A](io: IO[A], duration: FiniteDuration): IO[A] = {

    val timeoutIO: IO[Unit] = IO sleep duration
    val result: IO[Either[A, Unit]] = IO race(io, timeoutIO)

    result flatMap {
      case Left(value) => IO(value)
      case Right(_) => IO raiseError new RuntimeException("Computation timed out")
    }
  }
  //---------------------------------------------------------------------------
  override def run: IO[Unit] = ???
}
