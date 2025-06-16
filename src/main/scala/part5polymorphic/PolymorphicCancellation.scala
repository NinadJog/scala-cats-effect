package part5polymorphic

import cats.{Applicative, Monad}
import cats.effect.{IO, IOApp, MonadCancel, Poll}
import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}
import scala.concurrent.duration._

object PolymorphicCancellation extends IOApp.Simple {

  // E is an error type
  trait MyApplicativeError[F[_], E] extends Applicative[F] {

    // Creates a failed effect
    def raiseError[A](error: E): F[A]

    // Decides what happens if that effect is failed
    def handleErrorWith[A](fa: F[A])(f: E => F[A]): F[A]
  }

  // Since IO is a Monad, this type class is relevant to us.
  trait MyMonadError[F[_], E] extends MyApplicativeError[F, E] with Monad[F]

  // This is where the Cats Effect type class hierarchy starts

  // Monad Cancel
  // It's a specialized type of MonadError.

  // MonadCancel uses Poll, so we first define that.
  //
  // Poll is a curried function F[A] => F[A] => F[A]. The actual type signature
  // is more complicated because Poll is defined in terms of the natural
  // transformations between functors. Here we show a simplified version.
  // We can think of Poll[F[_]] as a higher-kinded function type.
  trait MyPoll[F[_]] {
    def apply[A](f: F[A]): F[A]
  }

  trait MyMonadCancel[F[_], E] extends MyMonadError[F, E] {
    def canceled: F[Unit]   // When we call IO.canceled, it cancels the IO chain
    def uncancelable[A](poll: Poll[F] => F[A]): F[A] // The returned F[A] is uncancellable

    // Poll[F] is invoked on an effect F to make it an uncancellable region.
  }

  // monadCancel for IO

  // Here's how we can add a MonadCancel instance in scope for our own effect
  // types.
  // On the R.H.S., MonadCancel invoked with IO simply fetches whichever given
  // or implicit monadCancel instance the compiler can find at that moment.
  // By importing MonadCancel and IO from Cats Effect, we also import the given
  // instance for this type.
  val monadCancelIO: MonadCancel[IO, Throwable] = MonadCancel[IO]

  // Since MonadCancel is also a Monad, it allows us to create values
  val molIO: IO[Int] = monadCancelIO pure 42

  // We can also call map and flatMap on it, although that's not the main purpose
  // of MonadCancel.
  val ambitiousMolIO: IO[Int] = monadCancelIO.map(molIO)(_ * 10)

  // The real value comes from calling the canceled or uncancelable methods.
  // The following is the same as calling IO.uncancelabe, which comes by default
  // with Cats Effect.
  val mustCompute: IO[Int] =
    monadCancelIO.uncancelable { _ =>
      for {
        _   <- monadCancelIO pure "Once started, I can't go back"
        res <- monadCancelIO pure 56
      } yield res
    }

  // With MonadCancel, we can generalize this sort of computation for any effect
  // type for which there's a MonadCancel instance in scope. Example:
  // (First do the following map and flatMap imports for the for comprehension to
  // compile correctly.)
  import cats.syntax.flatMap._  // flatMap
  import cats.syntax.functor._  // map
  def mustComputeGeneral[F[_], E](using mc: MonadCancel[F, E]): F[Int] =
    mc uncancelable { _ =>
      for {
        _   <- mc pure "Once started, I can't go back"
        res <- mc pure 56
      } yield res
    }

  // We can substitute IO for F to get the following
  val mustCompute_v2: IO[Int] = mustComputeGeneral[IO, Throwable]

  //---------------------------------------------------------------------------
  // Other functionalities and APIs of MonadCancel

  // allow cancellation listeners
  // here the onCancel method is actually using the MonadCancel instance under the hood.
  val mustComputeWithListener: IO[Int] = mustCompute onCancel IO("I'm being cancelled!").void

  // The following code is applicable to ALL MonadCancel instances.
  val mustComputeWithListener_v2: IO[Int] =
    monadCancelIO onCancel (mustCompute, IO("I'm being cancelled!").void) // same

  // .onCancel as an extension method
  import cats.effect.syntax.monadCancel._

  // APIs for finalizers: guarantee and guaranteeCase
  // These will prove useful when we discuss Ref and Deferred in polymorphic terms.
  val aComputationWithFinalizers: IO[Int] = monadCancelIO.guaranteeCase(IO(42)) {
    case Succeeded(fa) => fa flatMap { a => IO(s"successful: $fa").void }
    case Errored(e) => IO(s"failed: $e").void
    case Canceled() => IO(s"canceled").void
  }

  // The bracket pattern is specific to MonadCancel
  // The acquisition and release of resources happens because there's a MonadCancel
  // in scope. The bracket pattern is applicable to ANY effect type for which
  // there's a MonadCancel in scope.
  val aComputationWithUsage =
    monadCancelIO.bracket
      { IO(42) }                                                          // acquisition
      { value => IO(s"Using the meaning of life: $value") }               // usage
      { value => IO(s"Releasing the meaning of life resource...").void }  // release

  //---------------------------------------------------------------------------
  /**
   * Exercise: Generalize the auth flow that we created in the CancellingIOs
   * module of Part 3 Concurrency. Generalize it to use any effect type
   * instead of IO for which there is a given MonadCancel in scope.
   *
   * Since IO.sleep is not easy to generalize, make use of unsafeSleep for now.
   * This method is unsafe as it blocks the thread that calls it. We will fix
   * that later when we study the Temporal type class. The unsafeSleep method
   * is in the general.utils package.
   */
  import utils.general._

  // The thread sleep in unsafeSleep is NOT semantic blocking. This sleep is 
  // non-interruptible because Cats Effect has no way of knowing what we are 
  // going to do inside mc.pure. We are performing a side-effect inside it; 
  // that's not advisable but it will do for this exercise.
  def unsafeSleep[F[_], E](duration: FiniteDuration)(using mc: MonadCancel[F, E]): F[Unit] =
    mc pure (Thread sleep duration.toMillis) // performing a side-effect inside mc.pure!

  // Rewrite the IO chain as a for comprehension, since andThen (>>) is flatMap
  def inputPassword[F[_], E](using mc: MonadCancel[F, E]): F[String] = // Simulates a user typing a password
    for {
      _ <- mc.pure("Input password:").myDebug // Replace IO with mc.pure
      _ <- mc.pure("(typing password)").myDebug
      _ <- unsafeSleep[F, E](5.seconds)
      password <- mc.pure("RockTheJVM1!") // Not displayed by calling myDebug since password is secret.
    } yield password

  def verifyPassword[F[_], E](using mc: MonadCancel[F, E]): String => F[Boolean] =
    (pw: String) => for {
      _ <- mc.pure("verifying...").myDebug
      _ <- unsafeSleep[F, E](2.seconds)
      matches <- mc pure (pw == "RockTheJVM1!")
    } yield matches


  // Since inputPassword is cancellable, set up a finalizer in its onCancel callback.
  // Authentication flow that's partly cancellable. Wrapping 'inputPassword' in
  // poll unmasks the "do not cancel" mask placed by the call to IO.uncancelable.
  // Everything else remains uncancelable as it still has the mask on.
  def authFlow[F[_], E](using mc: MonadCancel[F, E]): F[Unit] =
    mc.uncancelable { poll =>
      for {
        pw        <- poll(inputPassword)
                      .onCancel(mc.pure("Authentication timed out. Try again later.").myDebug.void)
        verified  <- verifyPassword[F, E](pw) // compiles & runs without [F,E] but added it b/c IntelliJ complained
        _         <- if verified then
                        mc.pure("Authentication successful.").myDebug
                     else
                        mc.pure("Authentication failed.").myDebug
      } yield ()
  }

  // The authProgram remains exactly the same, except for the addition of the
  // actual type parameters [IO, Throwable] to authFlow in the following code,
  // whereby IO gets substituted for F in authFlow and Throwable gets
  // substituted for E.
  val authProgram: IO[Unit] = for {
    authFib <- authFlow[IO, Throwable].start
    _ <-  IO.sleep(3.seconds) >>
          IO("Authentication timeout, attempting cancel...").myDebug >>
          authFib.cancel
    _ <- authFib.join
  } yield ()

  /**
   * Run:
   * override def run: IO[Unit] = authProgram
   *
   * Output:
   * [io-compute-0] Input password:
   * [io-compute-0] (typing password)
   * [io-compute-0] verifying...
   * [io-compute-0] Authentication successful.
   * [io-compute-0] Authentication timeout, attempting cancel...
   */
  //---------------------------------------------------------------------------
  override def run: IO[Unit] = authProgram
}
