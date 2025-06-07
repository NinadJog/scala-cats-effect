package part3concurrency

import cats.effect.{Fiber, IO, IOApp, Outcome, OutcomeIO, FiberIO}
import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}
import scala.concurrent.duration.*
import utils.*

object RacingIOs extends IOApp.Simple {

  def runWithSleep[A](value: A, duration: FiniteDuration): IO[A] = {
    (
      IO(s"starting computation: $value").myDebug   >>
      IO.sleep(duration)                              >>
      IO(s"computation done. value = $value").myDebug >>
      IO(value)
    ).onCancel(IO(s"computation CANCELED for $value").myDebug.void)
  }

  // create a race condition
  def testRace(): IO[String] = {
    val meaningOfLife = runWithSleep(42,      1.second)
    val favLang       = runWithSleep("Scala", 2.seconds)

    // Run these two IOs (effects) on two separate fibers and race them i.e.
    // return whichever effect finishes first.
    val first: IO[Either[Int, String]] = IO.race(meaningOfLife, favLang)
    /*
       Here's how race works.
       - Both IOs run on separate fibers
       - The first one to finish returns the result
       - The loser is canceled
       - Fibers and cancellation happen automatically with IO.race so no need
         to manage fibers manually
       - IO.race is widely used in code that uses Cats Effect
     */

    first flatMap {
      case Left(mol)    => IO(s"Meaning of life won: $mol")
      case Right(lang)  => IO(s"Favorite language won: $lang")
    }
  }
  /**
   * run:
   * override def run: IO[Unit] = testRace().void
   *
   * Output:
   * [io-compute-2] starting computation of 42
   * [io-compute-1] starting computation of Scala
   * [io-compute-2] computation done. value = 42
   * [io-compute-2] computation CANCELED for value Scala
   * [io-compute-2] Meaning of life won: 42
   */

  //---------------------------------------------------------------------------
  /**
   * racePair
   * - More general version of race
   * - Allows us to manage the fiber of the losing effect
   */
  // IO[Outcome[IO, Throwable, ? >: Int & String]]
  // In the return type, the '? >: Int & String' is Scala's way of saying the
  // exact type is unknown, but it must be a supertype of both Int and String,
  // per DeepSeek AI. In practice, this usually resolves to Any, which is often
  // too broad. You might want to refine it to a more precise type
  // (like Int | String or Either[Int, String]) if possible.
  def testRacePair(): IO[Outcome[IO, Throwable, ? >: Int & String]] = {
    val meaningOfLife = runWithSleep(42,      1.second)
    val favLang       = runWithSleep("Scala", 2.seconds)

    // Here's the return type of the racePair method. The method is implemented
    // later after simplifying the type signature using type aliases.
    /**
    val raceResult_v0: IO[Either[
      (Outcome[IO, Throwable, Int], Fiber[IO, Throwable, String]),  // (winner result, loser fiber)
      (Fiber[IO, Throwable, Int], Outcome[IO, Throwable, String])   // (loser fiber, winner result)
    ]] = ???
    */

    // Use the following type aliases from Cats Effect to simplify the type signature
    //  type FiberIO[A]   = Fiber[IO, Throwable, A]
    //  type OutcomeIO[A] = Outcome[IO, Throwable, A]

    val raceResult: IO[Either[
      (OutcomeIO[Int], FiberIO[String]),  // (winner result, loser fiber)
      (FiberIO[Int], OutcomeIO[String])   // (loser fiber, winner result)
    ]] = IO.racePair(meaningOfLife, favLang)

    // The loser's fiber does not get canceled automatically; we have to do it
    // ourselves.
    raceResult flatMap {
      case Left((outMol, fibLang)) => fibLang.cancel >> IO("MOL won").myDebug >> IO(outMol).myDebug
      case Right((fibMol, outLang)) => fibMol.cancel >> IO("fav lang won").myDebug >> IO(outLang).myDebug
    }
  }
  /**
   * Run:
   * override def run: IO[Unit] = testRacePair().myDebug.void
   *
   * Output:
   * [io-compute-0] starting computation: 42
   * [io-compute-1] starting computation: Scala
   * [io-compute-0] computation done. value = 42
   * [io-compute-2] computation CANCELED for Scala
   * [io-compute-2] MOL won
   * [io-compute-2] Succeeded(IO(42))
   */

  //---------------------------------------------------------------------------
  /**
   * Exercises
   * 1. Implement timeout pattern with race.
   *    Hint: race the given IO with another IO that sleeps for the duration
   *    of the timeout. Then take into account which one wins. Returns the
   *    IO if it wins, else return a runtime exception if it times out.
   */
  def timeout[A](io: IO[A], duration: FiniteDuration): IO[A] = {

    val timeoutIO = IO.sleep(duration)
    val result    = IO.race(io, timeoutIO)

    result flatMap {
      case Left(value) => IO(value)
      case Right(_)    => IO.raiseError(new RuntimeException("Computation timed out"))
    }
  }

  val importantTask = IO.sleep(2.seconds) >> IO(42).myDebug
  val testTimeout = timeout(importantTask, 1.seconds)

  // The timeout pattern is so commonly used that CE provides an API for it:
  val testTimeout_v2 = importantTask timeout 1.seconds

  /**
   * Run:     testTimeout
   * Output:  java.lang.RuntimeException: Computation timed out
   *
   * Run:     testTimeout with duration 3.seconds
   * Output:  [io-compute-1] 42
   */

  //---------------------------------------------------------------------------
  /**
   * 2. A method to return a losing effect from a race.
   *    Hint: use racePair so we can get a handle on the losing fiber
   *    and maybe wait for it to finish.
   */
  def unrace[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    IO.racePair(ioa, iob) flatMap {
      case Left((_, fiberB)) => fiberB.join flatMap {  // ioa won but we return iob
        case Succeeded(resultEffect)  => resultEffect.map(Right(_))
        case Errored(e)               => IO.raiseError(e)
        case Canceled()               => IO.raiseError(new RuntimeException("Loser canceled"))
      }
      case Right((fiberA, _)) => fiberA.join.flatMap {  // iob won but we return ioa
        case Succeeded(resultEffect)  => resultEffect.map(Left(_))
        case Errored(e)               => IO.raiseError(e)
        case Canceled()               => IO.raiseError(new RuntimeException("Loser canceled"))
      }
  }

  // More readable code using a for comprehension and a helper method instead
  // of nested flatMaps. Same logic as the above version
  def unrace_v2[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    for {
      raceResult  <- IO.racePair(ioa, iob)
      result      <- raceResult match {
        case Left(_, fiberB)  => handleLosingFiber(fiberB) map (Right(_))
        case Right(fiberA, _) => handleLosingFiber(fiberA) map (Left(_))
      }
    } yield result

  // Helper method that prevents duplication
  def handleLosingFiber[A](fiber: Fiber[IO, Throwable, A]): IO[A] =
    for {
      outcome <- fiber.join
      result  <- outcome match {
        case Succeeded(resultEffect)  => resultEffect
        case Errored(e)               => IO.raiseError(e)
        case Canceled()               => IO.raiseError(new RuntimeException("Loser canceled"))
      }
    } yield result

  // Identical to testRace, except for the call to unrace instead of IO.race
  // This method uses a for comprehension instead of a flatMap
  def testUnrace(): IO[String] = {
    val meaningOfLife = runWithSleep(42, 1.second)
    val favLang       = runWithSleep("Scala", 2.seconds)

    for {
      result <- unrace_v2(meaningOfLife, favLang)   // result is of type Either[Int, String]
      answer <- result match {                      // answer is of type String
        case Left(mol)    => IO(s"Meaning of life won: $mol")
        case Right(lang)  => IO(s"Favorite language won: $lang")
      }
    } yield answer
  }

  /**
   * Run:
   * override def run: IO[Unit] = testUnrace().myDebug.void
   *
   * Output:
   * [io-compute-3] starting computation: Scala
   * [io-compute-1] starting computation: 42
   * [io-compute-1] computation done. value = 42
   * [io-compute-3] computation done. value = Scala
   * [io-compute-3] Favorite language won: Scala
   */

  //---------------------------------------------------------------------------
  /**
   * 3. Implement race in terms of racePair
   */
  // Cancel the loser's fiber. If the supposed winner gets canceled then the
  // loser becomes the de-facto winner unless it errors out or gets canceled.
  def simpleRace[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] = {
    IO.racePair(ioa, iob) flatMap {
      case Left((outA, fibB))   => outA match { // outA: OutcomeIO[A], fibB: FiberIO[B]
        // effectA: IO[A], whereas (effectA map (Left(_))): IO[Left[A]]
        case Succeeded(effectA) => fibB.cancel >> effectA map (Left(_)) // A is the winner
        case Errored(ex)        => fibB.cancel >> IO.raiseError(ex) // fibB: FiberIO[B]
        case Canceled()         => fibB.join flatMap {  // A is the loser since it has been canceled
          case Succeeded(effectB) => effectB map (Right(_)) // B is the real winner
          case Errored(ex) => IO.raiseError(ex)
          case Canceled() => IO.raiseError(new RuntimeException("No winners since both computations canceled"))
        }
      }
      case Right((fibA, outB))  => outB match { // outB: OutcomeIO[B], fibA: FiberIO[A]
        // effectB: IO[B], whereas (effectB map (Right(_))): IO[Right[B]
        case Succeeded(effectB) => fibA.cancel >> effectB map (Right(_))  // B is the winner
        case Errored(ex)        => fibA.cancel >> IO.raiseError(ex)
        case Canceled()         => fibA.join flatMap {  // B is the loser since it has been canceled
          case Succeeded(effectA) => effectA map (Left(_)) // A is the real winner
          case Errored(ex) => IO.raiseError(ex)
          case Canceled() => IO.raiseError(new RuntimeException("No winners since both computations canceled"))
        }
      }
    }
  }

  /**
   * Attempt to simplify the code and prevent code duplication did not work
   * because Scala does not have dependent types. Should revisit this another day,
   * maybe by passing wrapper types G[_] and H[_] for the Outcome and Fiber in
   * the handleWinnerLoser helper function.
   *
   * Here the answer ends up having the type Either[A, B] | Either[B, A], when
   * what we want is always Either[A, B].
  def simpleRace_v2[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    for {
      raceResult  <- IO.racePair(ioa, iob)
      answer      <- raceResult match {
        case Left((outA, fibB))   => handleWinnerLoser[A, B](outA, fibB, Left(_), Right(_))
        case Right((fibA, outB))  => handleWinnerLoser[B, A](outB, fibA, Right(_), Left(_))
      }
    } yield answer
  */

  // This helper function compiles but is of no use.
  def handleWinnerLoser[L, R](
      winnerOutcome: OutcomeIO[L],  // Sometimes the winnerOutcome type is OutcomeIO[R],
      loserFiber:    FiberIO[R],    // in which case the loserFiber type is FiberIO[L]
      winnerWrap:    L => Either[L, R],
      loserWrap:     R => Either[L, R]): IO[Either[L, R]] =
    for {
      winnerResult <- winnerOutcome match {
        case Succeeded(fa) => fa map winnerWrap
        case Errored(e) => IO.raiseError(e)
        case Canceled() =>
          loserFiber.join flatMap {
            case Succeeded(fb) => fb map loserWrap
            case Errored(e) => IO.raiseError(e)
            case Canceled() => IO.raiseError(new RuntimeException("No winners since both computations canceled"))
          }
      }
      _ <- loserFiber.cancel // Ensure loser is canceled if not already
    } yield winnerResult

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = testUnrace().myDebug.void
}
