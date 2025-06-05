package part3concurrency

/**
 * Key takeaways
 *
 * 1. Bracket pattern for managing resource lifecycle
 * 2. Nesting brackets is clunky, so use Resource
 * 3. Resource decouples acquisition and release from use
 * 4. Nested resources are straightforward to specify as they can be composed
 * 5. IO (which means any effect) can have finalizers
 */

import cats.effect.{IO, IOApp, Resource}
import java.io.{File, FileReader}
import java.util.Scanner
import scala.concurrent.duration.*
import utils.*

object Resources extends IOApp.Simple {

  // use-case: managing a connection lifecycle
  class Connection(url: String) {
    def open(): IO[String] = IO(s"opening connection to $url").myDebug
    def close(): IO[String] = IO(s"closing connection to $url").myDebug
  }

  //---------------------------------------------------------------------------
  // 1. Leaking fiber

  // We start with why resources are needed and how to use them
  // Define a small IO by opening a connection on a separate fiber.
  val connection: IO[Unit] = {
    IO("About to open connection").myDebug      *>
    IO(new Connection("rockthejvm.com").open()) *>
    IO.sleep(Int.MaxValue.seconds)  // sleep forever, which means keep the connection open
  }

  // We can use either *> or >> for this use case; it does not matter
  val asyncFetchUrl: IO[Unit] = for {
    fib <-  connection.start
    _   <-  IO.sleep(1.second) *> fib.cancel
  } yield ()
  // problem: leaking resources because the connection is not closed

  //---------------------------------------------------------------------------
  // 2. No more leaks, but this code is error-prone, becomes tedious with
  //    complex resources and hard to understand.

  // Correctly close the connection when the fiber is canceled by adding a
  // cancellation listener.
  def connectionLifecycle(conn: Connection): IO[Unit] =
    (conn.open() *> IO.sleep(Int.MaxValue.seconds)).onCancel(conn.close().void)

  val correctAsyncFetchUrl: IO[Unit] = for {
    conn <- IO(new Connection("rockthejvm.com"))
    fib <-  connectionLifecycle(conn).start
    _   <-  IO.sleep(1.second) *> fib.cancel
  } yield ()

  /* Output:
    [io-compute-3] opening connection to rockthejvm.com
    [io-compute-2] closing connection to rockthejvm.com
   */
  /**
   * But this kind of example gets tedious and complex to read and understand
   * when there are complex resources that are quite hard to get a handle of.
   * That's why Cats-Effect provides the bracket pattern.
   */

  //---------------------------------------------------------------------------
  // 3. The bracket pattern

  /**
   * Overall pattern is of the form
   *    someIO.bracket(useResourceCallback)(releaseResourceCallback)
   *
   * Forces programmers to handle cancellations in case we acquire resources.
   * The first argument of the bracket method is a function that CREATES AN
   * EFFECT out of the resource that we have just acquired by calling IO first.
   *
   * IO wraps the resource that we will be using, which is the new Connection
   * in this example: IO(new Connection("rockthejvm.com")).
   *
   * In the first argument of bracket, the result of the use of the resource
   * must be another effect. This is conn => conn.open() in the following code.
   *
   * In the second argument, we specify what we want to do with the resource
   * when we want to release it. So bracket is a description of the resource
   * acquisition and release.
  */
  val bracketFetchUrl =
    IO(new Connection("rockthejvm.com"))
      .bracket(conn => conn.open() *> IO.sleep(Int.MaxValue.seconds))(conn => conn.close().void)

  val bracketProgram = for {
    fib <- bracketFetchUrl.start
    _   <- IO.sleep(1.second) *> fib.cancel
  } yield ()

  // When the bracket pattern is used, resource release occurs both when the
  // effect is successful or if the fiber on which it's running is canceled.
  // bracket is equivalent to try-catches, but it's pure FP

  //---------------------------------------------------------------------------
  /**
   * Exercise: Read a file with the bracket pattern
   * - Open a scanner
   * - Read a file line by line every 100 millis
   * - close the scanner
   * - if cancelled/throws error, close the scanner
   *
   * This exercise is useful because it brings a mutable operation (reading a
   * file line by line) into a purely functional realm using effects and the
   * bracket pattern.
   */
  def openFileScanner(path: String): IO[Scanner] =
    IO(new Scanner(new FileReader(new File(path))))

  def bracketReadFile(path: String): IO[Unit] =
    IO(s"opening file at $path").myDebug >>
    openFileScanner(path)
      .bracket { scanner =>
        readLineByLine(scanner)
      } { scanner =>
        IO(s"closing file at $path").myDebug >>
        IO(scanner.close())
      }

  /**
   * Reads a file line by line every 100 milliseconds. Given a scanner,
   * this function produces an effect. The andThen operator should be the lazy
   * andThen (>>) to prevent a stack overflow. If we use the eager andThen
   * (*>), it causes a stack overflow since this function is recursive.
  */
  def readLineByLine(scanner: Scanner): IO[Unit] =
    if scanner.hasNextLine then
      IO(scanner.nextLine()).myDebug  >>
      IO.sleep(100.millis)            >>
      readLineByLine(scanner)
    else IO.unit

  /**
   * In this case, there's no need to spawn a
   * fiber explicitly because bracket takes care of asynchrony and of fiber
   * scheduling automatically.
  *
  override def run: IO[Unit] = bracketReadFile(
    "src/main/scala/part3concurrency/Resources.scala").void
  */

  //---------------------------------------------------------------------------
  // Nested Brackets

  /**
   * Use case: Two resources. Open a file, read configuration information
   * from it and use it to open a URL connection.
   *
   * If we use the bracket pattern to implement this, it leads to nested
   * brackets as shown in the following code because there are two resources.
   * The code is no longer easily readable, as it becomes similar to nested
   * try-catches.
   */
  def connFromConfig(path: String): IO[Unit] =
    openFileScanner(path)
      .bracket { scanner =>
        // acquire a connection based on the contents of the file.
        IO(new Connection(scanner.nextLine()))  // 1. resource acquisition
          .bracket { conn =>
            conn.open().myDebug >>              // 2. resource usage
            IO.never  // sleep forever and make an IO effect that never terminates
          } { conn =>
            IO("closing connection").myDebug >>
            conn.close().void                   // 3. resource release
          }
      } { scanner =>
        IO("closing file").myDebug >>
        IO(scanner.close())
      }

  //---------------------------------------------------------------------------
  // Resources
  //
  // It's tedious to nest resources with the bracket pattern.
  // A better option is to use Resource.

  /**
   * The gist of a Resource is that we can first construct it along with its
   * finalizer and use it later in our code.
   *
   * While the bracket had all 3 components of a resource: resource acquisition,
   * resource usage, and resource release, the Resource has only two:
   * acquisition and release. The resource usage can happen later. We decouple
   * the logic of using the resource from the logic of acquiring and releasing
   * it. The code becomes modular and it's also easy to compose multiple
   * resources. Resource is one of the most powerful features of Cats-Effect.
   */

  val connectionResource: Resource[IO, Connection] =
    Resource.make {
      IO(new Connection("rockthejvm.com")) // acquire the resource
    } { conn =>
      conn.close().void                    // release it
    }

  // ... at a later point in the code, use the resource as follows.
  // This code is highly readable
  val resourceFetchUrl: IO[Unit] = for {
    fib <- connectionResource.use(conn => conn.open() >> IO.never).start
    _   <- IO.sleep(1.second) >> fib.cancel
  } yield ()

  // We can use this code without having to care how the resource is handled
  // behind the scenes if the computation succeeds, fails, or is canceled.

  /* Test it:
    override def run: IO[Unit] = resourceFetchUrl.void

    Output:
    [io-compute-2] opening connection to rockthejvm.com
    [io-compute-1] closing connection to rockthejvm.com
   */

  //---------------------------------------------------------------------------
  // Resources are equivalent to brackets

  // Example: create a resource, a usage callback and a finalizing callback:
  val simpleResource = IO("some resource")
  val usingResource: String => IO[String] = string => IO(s"using resource $string").myDebug
  val releaseResource: String => IO[Unit] = string => IO(s"releasing resource $string").void

  // Use this resource and its callbacks with the bracket and with the Resource
  // data structure.
  // With bracket:
  val tryBracket: IO[String] = simpleResource.bracket(usingResource)(releaseResource)

  // With Resource:
  val tryResource: Resource[IO, String] = Resource.make(simpleResource)(releaseResource)
  // later in the code we can call
  val useResource: IO[String] = tryResource.use(usingResource)

  //---------------------------------------------------------------------------
  /**
   * Exercise: Read a text file with one line every 100 millis, using Resource
   * (Refactor the bracket exercise to use Resource.)
   */

  // Acquire the file resource and set up its finalizer
  def makeResourceFromFile(path: String): Resource[IO, Scanner] =
    Resource.make {
      IO(s"opening file at $path").myDebug >>
      openFileScanner(path)
    } { scanner =>
      IO(s"closing file at $path").myDebug >>
      IO(scanner.close())
    }

  // Use the file resource to read the file
  def resourceReadFile(path: String): IO[Unit] =
    makeResourceFromFile(path) use readLineByLine
    // same as makeResourceFromFile(path).use(scanner => readLineByLine(scanner))

  val readTextFile: IO[Unit] = resourceReadFile("src/main/scala/part3concurrency/Resources.scala")

  // Example of how to cancel the read
  def cancelReadFile(path: String): IO[Unit] = for {
    fib <-  resourceReadFile(path).start  // Start a fiber to read the file
    _   <-  IO.sleep(2.seconds)                             >>
            IO(s"Canceling reading of file $path").myDebug  >>
            fib.cancel
  } yield ()

  /**
   * Run this:
   * override def run: IO[Unit] = cancelReadFile("src/main/scala/part3concurrency/Resources.scala").void
   *
   * Output:
   * [io-compute-3] opening file at src/main/scala/part3concurrency/Resources.scala
   * ...
   * [io-compute-2] Canceling reading of file src/main/scala/part3concurrency/Resources.scala
   * [io-compute-2] closing file at src/main/scala/part3concurrency/Resources.scala
   */

  //---------------------------------------------------------------------------
  // Nested resources

  // First let's create a resource from Connection
  def makeResourceFromConnection(scanner: Scanner): Resource[IO, Connection] =
    Resource.make {
      IO(new Connection(scanner.nextLine())) // acquire the resource
    } { conn =>
      IO("closing connection").myDebug >>
      conn.close().void                    // release it
    }

  // Create nested resources using flatMap
  // Make file resource first and then make connection resource
  def connFromConfigResource(path: String): Resource[IO, Connection] =
    makeResourceFromFile(path) flatMap makeResourceFromConnection

  // Equivalent code with for comprehension. This code is cleaner and
  // easier to understand
  def connFromConfigResourceClean(path: String): Resource[IO, Connection] =
    for {
      scanner <- makeResourceFromFile(path)
      conn    <- makeResourceFromConnection(scanner)
    } yield conn

  val openConnection =
    connFromConfigResourceClean("src/main/resources/connection.txt")
      .use(conn => conn.open() >> IO.never)
  // connection + file will close automatically

  /**
   * Run the following:
   * override def run: IO[Unit] = openConnection.void
   *
   * Output:
   * [io-compute-2] opening file at src/main/resources/connection.txt
   * [io-compute-2] opening connection to rockthejvm.com
   * [io-compute-2] closing connection
   * [io-compute-2] closing connection to rockthejvm.com
   * [io-compute-2] closing file at src/main/resources/connection.txt
   */

  // Cancel the connection
  val canceledConnection: IO[Unit] = for {
    fib <- openConnection.start
    _   <- IO.sleep(2.seconds) >> IO("canceling fiber!").myDebug >> fib.cancel
  } yield ()

  /**
   * Run:
   * override def run: IO[Unit] = canceledConnection
   *
   * Output:
   * [io-compute-3] opening file at src/main/resources/connection.txt
   * [io-compute-3] opening connection to rockthejvm.com
   * [io-compute-0] canceling fiber!
   * [io-compute-0] closing connection
   * [io-compute-0] closing connection to rockthejvm.com
   * [io-compute-0] closing file at src/main/resources/connection.txt
   */

  /**
   * One of the reasons Resource is so powerful is that resource acquisition
   * and cancellation can happen automatically regardless of whether we are
   * using sequential processing or concurrent processing with a fiber.
   *
   */
  //---------------------------------------------------------------------------
  // Finalizers

  // Finalizers can also be attached to regular IOs i.e. to regular effects.
  // The finalizer is called regardless of whether the IO is successful or
  // if it fails or is canceled.
  val ioWithFinalizer =
    IO("some resource").myDebug guarantee IO("freeing resource").myDebug.void

  /**
   * Run:
   *  override def run: IO[Unit] = ioWithFinalizer.void
   *
   * Output:
   * [io-compute-1] some resource
   * [io-compute-1] freeing resource
   */

  // We can also specify different finalizers for different outcomes such as
  // success, failure, cancellations
  import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}

  val ioWithFinalizer_v2 = IO("some resource").myDebug.guaranteeCase {
    case Succeeded(fa) => for {
                            result <- fa
                            _ <- IO(s"releasing resource $result").myDebug
                          } yield ()
    case Errored(e) => IO("nothing to release").myDebug.void
    case Canceled() => IO("resource got canceled, releasing what's left").myDebug.void
  }

  // Equivalent code for the Succeeded case using flatMap instead of for comprehension
  val ioWithFinalizer_v3 = IO("some resource").myDebug.guaranteeCase {
    case Succeeded(fa)  => fa.flatMap(result => IO(s"releasing resource $result").myDebug).void
    case Errored(e)     => IO("nothing to release").myDebug.void
    case Canceled()     => IO("resource got canceled, releasing what's left").myDebug.void
  }

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = ioWithFinalizer.void

}
