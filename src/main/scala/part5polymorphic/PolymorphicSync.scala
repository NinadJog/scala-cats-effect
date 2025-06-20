package part5polymorphic

import cats.Defer
import cats.effect.kernel.MonadCancel
import cats.effect.{IO, IOApp, Sync}
import java.io.{BufferedReader, InputStreamReader}

object PolymorphicSync extends IOApp.Simple {

  /*
    Recap:
    IO is the ultimate effect type. It is the suspension of any computation in
    a purely functional data type. We can delay computations that perform
    side-effects by wrapping them in IO.

    The following computations are performed only when the IO is evaluated.
   */
  // Method #1: Using IO
  val aDelayedIO = IO delay {   // "suspends" computation in IO
    println("I'm an effect!")
    42
  }

  // performed on some specific thread pool for blocking computations
  val aBlockingIO = IO blocking { // takes a call-by-name "thunk" just like delay
    println("loading...")
    Thread sleep 1000
  }

  /*
    The ability to suspend computations in an effect type or the ability to
    suspend a blocking computation on an effect that's actually evaluated on a
    special thread pool for blocking computations embodies the concept of
    SYNCHRONY.

    Synchronous Computation = ability to run some external computations from
    outside the context of Cats Effect and wrap them INTO the context of Cats
    Effect (in the above example in terms of IO) and also a blocking computation
    in the context of IO.

    The trait Sync is the capability to suspend any computation on the effect
    type F, not just IO.

    (In the Cats Effect library, Sync also extends other typeclasses such as
    Clock and Unique, but we exclude them here because they are not that
    important in the current context.)

    With the delay, blocking and defer methods, we can raise computations
    executed OUTSIDE the context of Cats Effect INTO the context of Cats Effect.
   */

  trait MySync[F[_]] extends MonadCancel[F, Throwable] with Defer[F] {

    def delay[A](thunk: => A): F[A] // "suspension" of a computation in F - will run on the CE thread pool
    def blocking[A](thunk: => A): F[A]  // effect that runs on the blocking thread pool

    // Exercise: Implement the defer method
    // defer can be implemented in terms of delay and flatMap, as shown below.
    // defer comes for free from the Defer typeclass, since Sync extends Defer.
    // Defer has NO relationship to any other typeclass in Cats and its only API
    // is defer, which we have implemented below.
    def defer[A](thunk: => F[A]): F[A] = flatMap(delay(thunk))(identity)  // delay(thunk) returns F[F[A]]
  }

  // Using the Sync typeclass manually
  // Fetches whatever given instance of Sync[IO] exists in scope
  val syncIO = Sync[IO]

  // abilities: pure, map/flatMap, raiseError, uncancelable, + delay/blocking

  // Method #2: Using Sync[IO]
  val aDelayedIO_v2 = syncIO delay {   // "suspends" computation in IO
    println("I'm an effect!")
    42
  } // same as IO.delay

  // performed on some specific thread pool for blocking computations
  val aBlockingIO_v2 = syncIO blocking { // same as IO.blocking
    println("loading...")
    Thread sleep 1000
  }

  // Method #3. Third way of creating delayed computations.
  // defer takes a thunk which is an EFFECT: (thunk :=> IO[A])
  // unlike delay or blocking whose thunk take a computation of type A.
  val aDeferredIO = IO defer aDelayedIO

  //---------------------------------------------------------------------------
  /**
   * Exercise: Write a polymorphic console
   */

  trait Console[F[_]] {
    def println[A](a: A): F[Unit]
    def readLine(): F[String]
  }
  
  import cats.syntax.functor._  // map extension method
  object Console {

    /**
     * Create a factory method that creates a Console[F] wrapped in another F.
     * First instantiate System.in and System.out streams suspended in F and
     * build a console based on them.
     */
    def apply[F[_]](using sync: Sync[F]): F[Console[F]] =
      sync.pure((System.in, System.out)) map {  // sync.pure is F[tuple]
        case (in, out) => new Console[F] {
          def println[A](a: A): F[Unit] = {
            sync blocking out.println(a) // use blocking to prevent println from hogging CE threads
          }

          // Convert the bytes that System.in reads to a string using buffered reader
          def readLine(): F[String] = {
            val bufferedReader = new BufferedReader(new InputStreamReader(in))
            sync blocking bufferedReader.readLine()
            /*
              sync.blocking is not ideal because if the user doesn't enter any 
              data in the console, we will get a forever blocking computation 
              that will hang a thread from the blocking thread pool or, worse,
              from the CE thread pool.
              
              There's also sync.interruptible (true/false) which attempts to
              block the thread via thread interrupts in case of cancel. If the
              flag is true, the thread interrupt signals are sent repeatedly,
              otherwise not.
             */
          }
        }
      }
  }

  /**
   * Test the console. First build a console.
   */
  def consoleReader(): IO[Unit] = 
    for {
      console <- Console[IO]  // apply method is called. Works bc we have a Sync[IO] in scope
      _       <- console.println("Hi, what's your name?")
      name    <- console.readLine()
      _       <- console.println(s"Nice to meet you, $name")
    } yield ()

  /**
   * Summary
   * 
   * Sync     = ability to suspend effects synchronously
   * delay    = wrapping any computation in F
   * blocking = a semantically blocking computation, wrapped in F
   * 
   * Goal: Generalize synchronous code for any effect type.
   * Foreign Function Interface (FFI): Suspend computations (including side-effects)
   * executed in another context. Bring them into CE.
   */
  
  //---------------------------------------------------------------------------
  override def run: IO[Unit] = consoleReader()
}
