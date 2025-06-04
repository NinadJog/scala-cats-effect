package part3concurrency

import cats.effect.{IO, IOApp}
import java.io.{File, FileReader}
import java.util.Scanner
import scala.concurrent.duration.*
import utils._

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

  //---------------------------------------------------------------------------
  /**
   * In the case of the exercise problem, there's no need for us to spawn a
   * fiber explicitly because bracket takes care of asynchrony and of fiber
   * scheduling automatically.
  */
  override def run: IO[Unit] = bracketReadFile(
    "src/main/scala/part3concurrency/Resources.scala").void
}
