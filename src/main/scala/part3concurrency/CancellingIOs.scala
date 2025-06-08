package part3concurrency

import cats.effect.{IO, IOApp}
import scala.concurrent.duration._

object CancellingIOs extends IOApp.Simple {

  import utils._

  /*
    Ways of cancelling IOs
    - fib.cancel
    - IO.race and other APIs cancel IOs automatically
    - manual cancellation
   */

  // Whatever occurs after IO.canceled in an IO chain will not be evaluated.
  // This expression has type IO[Int] because the last expression in this chain is
  // IO(42). It has type IO[Int] even though the IO(42) is never evaluated
  val chainOfIos: IO[Int] = IO("waiting").myDebug >> IO.canceled >> IO(42).myDebug

  // uncancellable
  // It's a wrapper that prevents an IO from being cancelled by some other
  // thread or some other fiber.

  // example: online store, proven payment processor that never takes more than
  // a second to purchase a payment from a customer.
  // payment process must NOT be cancelled.
  val specialPaymentSystem = (
      IO("Payment running, don't cancel me...").myDebug >>
      IO.sleep(1.second) >>
      IO("Payment completed").myDebug
    ).onCancel(IO("MEGA CANCEL OF DOOM!").myDebug.void)

  // Cancel this payment system
  val cancellationOfDoom = for {
    fib <- specialPaymentSystem.start
    _   <- IO.sleep(500.millis) >> fib.cancel
    _   <- fib.join // not strictly necessary
  } yield ()

  val atomicPayment: IO[String] = IO.uncancelable(_ => specialPaymentSystem)  // "masking"
  val atomicPayment_v2 = specialPaymentSystem.uncancelable    // this is the same

  // The cats-effect runtime will detect that specialPaymentSystem is uncancellable
  // and will not permit its cancellation.

  val noCancellationOfDoom = for {
    fib <- atomicPayment.start
    _ <- IO.sleep(500.millis) >> IO("attempting cancellation...").myDebug >> fib.cancel
    _ <- fib.join // not strictly necessary
  } yield ()

  /**
   * Run:
   * override def run: IO[Unit] = noCancellationOfDoom
   *
   * Output (cancellation attempt fails, as it gets ignored):
   * [io-compute-1] Payment running, don't cancel me...
   * [io-compute-3] attempting cancellation...
   * [io-compute-1] Payment completed
   */

  //---------------------------------------------------------------------------
  /**
   * The uncancellable API is much more complex and more general. It takes a
   * function from Poll[IO] to IO. We didn't use the poll feature in the above
   * example. But the Poll object can be used to mark sections within the
   * returned effect (the IO chain) that CAN be cancelled. That's the hard part
   * of uncancellable.
   *
   * Example: Authentication service with two parts.
   * - input password, can be cancelled, because otherwise we would block
   *   indefinitely on user input.
   * - verify password. CANNOT be cancelled once it's started.
   */
  val inputPassword: IO[String] =   // Simulates a user typing a password
    IO("Input password:").myDebug     >>
    IO("(typing password)").myDebug   >>
    IO.sleep(5.seconds)               >>
    IO("RockTheJVM1!")  // This is the password. (Not displayed by calling myDebug since it's secret.)

  val verifyPassword: String => IO[Boolean] = (pw: String) =>
    IO("verifying...").myDebug >> IO.sleep(2.seconds) >> IO(pw == "RockTheJVM1!")

  // Since inputPassword is cancellable, set up a finalizer in its onCancel callback.
  // poll is not used here -- not yet.
  // This is the authentication flow without cancellation.
  val authFlow: IO[Unit] = IO.uncancelable { poll =>
    for {
      pw        <- inputPassword.onCancel(IO("Authentication timed out. Try again later.").myDebug.void)
      verified  <- verifyPassword(pw)
      _         <- if (verified) IO("Authentication successful.").myDebug
                   else IO("Authentication failed.").myDebug
    } yield ()
  }
  /**
   * Run:
   * override def run: IO[Unit] = authFlow
   *
   * Output:
   * [io-compute-1] Input password:
   * [io-compute-1] (typing password)
   * [io-compute-1] verifying...
   * [io-compute-1] Authentication successful.
   */

  // Attempt to cancel the auth flow. Since we have defined it within
  // IO.uncancellable and not made use of the poll, the cancellation attempt
  // will be ignored, as shown in the output. This demonstrates that
  // uncancellable masks everything that's inside.
  val authProgram: IO[Unit] = for {
    authFib <- authFlow.start
    _       <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").myDebug >> authFib.cancel
    _       <- authFib.join
  } yield ()

  /**
   * Run:
   * override def run: IO[Unit] = authProgram
   *
   * Output:
   * [io-compute-1] Input password:
   * [io-compute-1] (typing password)
   * [io-compute-1] verifying...
   * [io-compute-1] Authentication timeout, attempting cancel...
   * [io-compute-1] Authentication successful.
   */

  //---------------------------------------------------------------------------
  // Use Poll to make partly cancellable

  // Authentication flow that's partly cancellable. Wrapping 'inputPassword' in
  // poll unmasks the "do not cancel" mask placed by the call to IO.uncancelable.
  // Everything else remains uncancelable as it still has the mask on.
  val authFlowPartlyCancellable: IO[Unit] = IO.uncancelable { poll =>
    for {
      pw        <- poll(inputPassword)  // This becomes cancellable
                    .onCancel(IO("Authentication timed out. Try again later.").myDebug.void)
      verified  <- verifyPassword(pw) // not cancellable
      _         <- if (verified) IO("Authentication successful.").myDebug // not cancellable
      else IO("Authentication failed.").myDebug
    } yield ()
  }

  val authProgramPartlyCancellable: IO[Unit] = for {
    authFib <- authFlowPartlyCancellable.start
    _ <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").myDebug >> authFib.cancel
    _ <- authFib.join
  } yield ()

  /**
   * Here the cancellation attempt succeeded because poll unmasked inputPassword.
   * Works when the fiber is canceled before (2 seconds) inputPassword completes.  (5 seconds)
   *
   * Run:
   * override def run: IO[Unit] = authProgramPartlyCancellable
   *
   * Output:
   * [io-compute-0] Input password:
   * [io-compute-0] (typing password)
   * [io-compute-1] Authentication timeout, attempting cancel...
   * [io-compute-1] Authentication timed out. Try again later.
   */

  //---------------------------------------------------------------------------
  // Uncancelable and Poll are the opposites of each other

  // The following auth flow is completely cancellable because the poll wrapper
  // around the for comprehension completely outdoes the call to IO.uncancelable.
  val authFlowCompletelyCancellable: IO[Unit] = IO.uncancelable { poll =>
    poll(for {
      pw <- inputPassword.onCancel(IO("Authentication timed out. Try again later.").myDebug.void)
      verified <- verifyPassword(pw)
      _ <- if (verified) IO("Authentication successful.").myDebug
      else IO("Authentication failed.").myDebug
    } yield ())
  }

  /*
    Uncancelable calls are MASKS that suppress cancellation.
    Poll calls are GAPS opened in the uncancellable region.
   */

  //---------------------------------------------------------------------------
  /**
   * Exercises
   */
  // 1
  val cancelBeforeMol: IO[Int] = IO.canceled >> IO(42).myDebug

  // IO.uncancelable undoes the effect of IO.cancelable in the following val.
  // It eliminates ALL the cancel points in the IO chain regardless of whether
  // the cancellation is attempted from another fiber or the same fiber (the
  // latter of which is the case here.) It eliminates all cancel points
  // except the ones protected by poll, of which there aren't any here.

  val uncancelableMol: IO[Int] = IO.uncancelable(_ => IO.canceled >> IO(42).myDebug)
  // Output: [io-compute-3] 42

  // 2
  val invincibleAuthProgram: IO[Unit] = for {
    authFib <- IO.uncancelable(_ => authFlowPartlyCancellable).start
    _       <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").myDebug >> authFib.cancel
    _       <- authFib.join
  } yield ()

  // The authFlowPartlyCancellable was wrapped in uncancelable but had a poll
  // gap for inputPassword. Wrapping auth flow in another uncancelable closes
  // that gap, so that none of the components in its chain will be cancelable.
  // Which means this auth flow will be atomic (uncancelable) no matter how
  // long we wait before attempting to cancel the auth fiber. The output
  // bears this out:

  /**
   * Run:
   * override def run: IO[Unit] = invincibleAuthProgram
   *
   * Output:
   * [io-compute-0] Input password:
   * [io-compute-0] (typing password)
   * [io-compute-3] Authentication timeout, attempting cancel...
   * [io-compute-0] verifying...
   * [io-compute-0] Authentication successful.
   */

  //---------------------------------------------------------------------------
  // 3.
  // Question: Will 'second cancelable' get printed to the console?
  // (The correct answer is No.)
  def threeStepProgram(): IO[Unit] = {
    val sequence = IO.uncancelable { poll =>
      poll(IO("first cancelable").myDebug   >> IO.sleep(1.second) >> IO("first cancelable end").myDebug)  >>
      IO("uncancelable").myDebug            >> IO.sleep(1.second) >> IO("uncancelable end").myDebug       >>
      poll(IO("second cancelable").myDebug  >> IO.sleep(1.second) >> IO("second cancelable end").myDebug)
    }

    for {
      fib <- sequence.start
      _ <- IO.sleep(1500.millis) >> IO("CANCELING").myDebug >> fib.cancel
      _ <- fib.join
    } yield ()
  }

  /**
   * Since we are sending the cancel signal after 1.5 seconds, right in the
   * middle of the uncancelable effect, we would expect the signal to get
   * ignored, which means 'second cancelable' should print to the console.
   * But it does not, as shown by the following output. (This was run
   * BEFORE adding the IO("... end") statements to the 3 regions
   *
   * [io-compute-3] first cancelable
   * [io-compute-3] uncancelable
   * [io-compute-2] CANCELING
   *
   * That's because even though the cancel signal is ignored by the
   * uncancelable region, it is acted upon as soon as it enters the scope of
   * the next available cancelable region of the sequence, the
   * 'second cancelable' region. That effect is therefore canceled. The cancel
   * signal is valid throughout the ENTIRE SCOPE of the sequence effect.
   *
   * If there were no other cancelable regions in the rest of the sequence, the
   * cancel signal would have been ignored.
   *
   * Output after adding the IO("... end") statements to the 3 regions:
   *
   * [io-compute-1] first cancelable
   * [io-compute-1] first cancelable end
   * [io-compute-1] uncancelable
   * [io-compute-2] CANCELING
   * [io-compute-1] uncancelable end
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = threeStepProgram()

}
