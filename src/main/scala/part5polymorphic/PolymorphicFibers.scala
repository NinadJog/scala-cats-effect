package part5polymorphic

import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.effect.{Fiber, FiberIO, IO, IOApp, MonadCancel, Outcome, OutcomeIO, Spawn}

object PolymorphicFibers extends IOApp.Simple {

  // Spawn is a typeclass used to generalize the concept of a fiber.
  // It can create fibers for any effect.
  // Spawn describes the properties of the effects capable of creating fibers.
  // The main API of Spawn is start.
  trait MySpawn_v0[F[_]] extends MonadCancel[F, Throwable] {
    // main API
    def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] // creates a fiber wrapped in the effect type F
    // start runs an effect on a fiber

    // secondary APIs
    def never[A]: F[A]  // a forever-suspending effect
    def cede: F[Unit]   // a "yield" effect, a hint to the CE runtime to switch
                        // the thread on which this effect is being evaluated.
  }

  // To demonstrate the return type of the start method, here's a reminder of
  // what happens when we call start on an IO.
  val mol:    IO[Int]                       = IO(42)
  val fiber:  IO[Fiber[IO, Throwable, Int]] = mol.start
  // By analogy, the return type of the start method will be F[Fiber[F, Throwable, A]]
  // as shown above in the MySpawn trait.

  //--------
  // MySpawn is generic because it takes an effect type F[_], but it's not
  // generic enough because it's taking java.lang.Throwable as the error type.
  // There's a more general type class in Cats Effect called GenSpawn.

  trait MyGenSpawn[F[_], E] extends MonadCancel[F, E] {
    def start[A](fa: F[A]): F[Fiber[F, E, A]] // runs an effect on a fiber
    def never[A]: F[A]
    def cede: F[Unit]

    def racePair[A, B](fa: F[A], fb: F[B]): F[Either[
      (Outcome[F, E, A], Fiber[F, E, A]),
      (Fiber[F, E, A], Outcome[F, E, A])]]
  }

  // (The prefix Gen is used to denote typeclasses that are generic in
  // the error type. Examples of such typeclasses are: GenConcurrent,
  // GenTemporal, etc.)

  //--------
  // In fact, Spawn is a concrete extension of GenSpawn, substituting Throwable
  // for E, as shown below.

  trait MySpawn[F[_]] extends MyGenSpawn[F, Throwable]

  // Since Spawn is a monad, it comes with several other APIs:
  // pure               from Applicative
  // map/flatMap        from Functor
  // raiseError         from MonadError
  // uncancelable       from MonadCancel
  // start              from Spawn
  // That's why Spawn is a more powerful typeclass than any we have discovered
  // so far.
  //
  // We can access all these capabilities manually by using the
  // typeclass instance that we can fetch from the implicit scope:

  val spawnIO = Spawn[IO] // fetch the given (implicit) Spawn[IO] that comes with Cats Effect

  // Example from the fibers lesson. Either run the start method on IOs or
  // use the SpawnIO typeclass to start them ourselves.

  def ioOnSomeThread[A](io: IO[A]): IO[Outcome[IO, Throwable, A]] = for {
    fib     <- spawnIO.start(io)  // io.start assumes the presence of a Spawn[IO]
    result  <- fib.join
  } yield result

  //---------------------------------------------------------------------------
  // GENERALIZE

  // When we say io.start, we assume the presence of a Spawn[IO] in scope.
  // We can generalize to effects other than IO:

  // (The following 'effectOnSomeThread' method compiles only if we import map
  // and flatMap, because otherwise as the compiler tries to translate the
  // for comprehension into map and flatMap, it fails, as it does not have
  // access to map and flatMap as EXTENSION methods on F[A]. The import
  // statements provide the access.)

  import cats.syntax.functor._      // for map
  import cats.syntax.flatMap._      // for flatMap
  import cats.effect.syntax.spawn.* // start extension method for any effect that has a Spawn[F] implicit in scope

  def effectOnSomeThread[F[_], A](fa: F[A])(using spawn: Spawn[F]): F[Outcome[F, Throwable, A]] =
    for {
      fib     <- fa.start // would need to use spawn.start(fa) if we didn't import cats.effect.syntax.spawn.*
      result  <- fib.join
    } yield result

  // Now we can run effectOnSomeThread on ANY effect type F that has a given
  // (implicit) Spawn[F] in scope. So we can call either of the two following
  // lines; they are equivalent.

  val molOnFiber    = ioOnSomeThread(mol)
  val molOnFiber_v2 = effectOnSomeThread(mol) // compiler infers [IO, Int]

  //---------------------------------------------------------------------------

  /**
   * Exercise: Generalize the following racePair code for any effect type F
   * that has Spawn[F] in scope. Implement race in terms of racePair.
   * The code comes from an exercise in the RacingIOs section of part 3.
   */
  def generalRace[F[_], A, B](fa: F[A], fb: F[B])(using spawn: Spawn[F]): F[Either[A, B]] =
    spawn.racePair(fa, fb) flatMap {
      case Left((outA, fibB))   => outA match { // outA: Outcome[F, Throwable, A], fibB: Fiber[F, Throwable, B]
        // effectA: F[A], whereas (effectA map (Left(_))): F[Left[A]]
        case Succeeded(effectA) => fibB.cancel >> effectA map (Left(_)) // A is the winner

        case Errored(ex)        => fibB.cancel >> (spawn raiseError ex) // fibB: Fiber[F, Throwable, B]
        // case Errored(ex)  => fibB.cancel.flatMap(_ => spawn.raiseError(ex)) // same code uses flatMap in place of >>

        case Canceled()         => fibB.join flatMap {  // A is the loser since it has been canceled
          case Succeeded(effectB) => effectB map (Right(_)) // B is the real winner
          case Errored(ex) => spawn raiseError ex
          case Canceled() => spawn raiseError new RuntimeException("No winners since both computations canceled")
        }
      }
      case Right((fibA, outB))  => outB match { // outB: Outcome[F, Throwable, B], fibA: Fiber[F, Throwable, A]
        // effectB: F[B], whereas (effectB map (Right(_))): F[Right[B]]
        case Succeeded(effectB) => fibA.cancel >> effectB map (Right(_))  // B is the winner
        case Errored(ex)        => fibA.cancel >> (spawn raiseError ex)
        case Canceled()         => fibA.join flatMap {  // B is the loser since it has been canceled
          case Succeeded(effectA) => effectA map (Left(_)) // A is the real winner
          case Errored(ex) => spawn raiseError ex
          case Canceled() => spawn raiseError new RuntimeException("No winners since both computations canceled")
        }
      }
    }

  //---------------------------------------------------------------------------
  /**
   * Same functionality as simpleRace, but refactored to eliminate code
   * duplication. Helper method uses type parameters W and L for Winner and Loser.
   */
  def generalRace_v2[F[_], A, B](fa: F[A], fb: F[B])(using spawn: Spawn[F]): F[Either[A, B]] = {

    // Helper method to handle the winner's outcome and the loser's fiber
    def handleWinner[W, L](
                            winnerOutcome:  Outcome [F, Throwable, W],
                            loserFiber:     Fiber   [F, Throwable, L],
                            wrapWinner:     W => Either[A, B],  // function to wrap the winner's result
                            wrapLoser:      L => Either[A, B]   // function to wrap the loser's result
                          ): F[Either[A, B]] = {
      winnerOutcome match {
        // Cancel loser's fiber and wrap result in Left(_) if A won and Right(_) if B won
        case Succeeded(effectW) => loserFiber.cancel >> effectW map wrapWinner

        // Since the winning action failed, cancel the loser's fiber and propagate the error
        case Errored(ex)        => loserFiber.cancel >> (spawn raiseError ex)

        // Since the winning action was canceled, join the loser's fiber, examine its
        // outcome and either return its success (wrapped appropriately), propagate its
        // error, or raise an exception if both were canceled.
        case Canceled()         =>
          loserFiber.join flatMap {
            case Succeeded(effectL) => effectL map wrapLoser
            case Errored(ex)        => spawn raiseError ex
            case Canceled()         => spawn raiseError new RuntimeException("No winners since both computations canceled")
          }
      }
    }

    spawn.racePair(fa, fb) flatMap {
      // Winner is A, loser is B; winner's result is wrapped with Left(_), loser's with Right(_)
      case Left((outA, fibB))   => handleWinner(outA, fibB, Left(_), Right(_))

      // Winner is B, loser is A; winner's result is wrapped with Right(_), loser's with Left(_)
      case Right((fibA, outB))  => handleWinner(outB, fibA, Right(_), Left(_))
    }
  }

  //---------------------------------------------------------------------------
  // Original code from the RacingIOs section of part 3:
  // Cancel the loser's fiber. If the supposed winner gets canceled then the
  // loser becomes the de-facto winner unless it errors out or gets canceled.
  def ioRace[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    IO.racePair(ioa, iob) flatMap {
      case Left((outA, fibB))   => outA match { // outA: OutcomeIO[A], fibB: FiberIO[B]
        // effectA: IO[A], whereas (effectA map (Left(_))): IO[Left[A]]
        case Succeeded(effectA) => fibB.cancel >> effectA map (Left(_)) // A is the winner
        case Errored(ex)        => fibB.cancel >> (IO raiseError ex) // fibB: FiberIO[B]
        case Canceled()         => fibB.join flatMap {  // A is the loser since it has been canceled
          case Succeeded(effectB) => effectB map (Right(_)) // B is the real winner
          case Errored(ex) => IO raiseError ex
          case Canceled() => IO raiseError new RuntimeException("No winners since both computations canceled")
        }
      }
      case Right((fibA, outB))  => outB match { // outB: OutcomeIO[B], fibA: FiberIO[A]
        // effectB: IO[B], whereas (effectB map (Right(_))): IO[Right[B]
        case Succeeded(effectB) => fibA.cancel >> effectB map (Right(_))  // B is the winner
        case Errored(ex)        => fibA.cancel >> (IO raiseError ex)
        case Canceled()         => fibA.join flatMap {  // B is the loser since it has been canceled
          case Succeeded(effectA) => effectA map (Left(_)) // A is the real winner
          case Errored(ex) => IO raiseError ex
          case Canceled() => IO raiseError new RuntimeException("No winners since both computations canceled")
        }
      }
    }

  //---------------------------------------------------------------------------
  // Test these implementations

  import scala.concurrent.duration._
  import utils.general._

  val fast: IO[Int]    = (IO sleep 1.second)  >> IO(42).myDebug
  val slow: IO[String] = (IO sleep 2.seconds) >> IO("Scala").myDebug

  val race    = ioRace(fast, slow)
  val race_v2 = generalRace(fast, slow)
  val race_v3 = generalRace_v2(fast, slow)

  /**
   * Run either of race, race_v2, race_v3:
   * override def run: IO[Unit] = race_v3.void
   *
   * Output:
   * [io-compute-2] 42
   */
  //---------------------------------------------------------------------------
  override def run: IO[Unit] = race_v3.void
}
