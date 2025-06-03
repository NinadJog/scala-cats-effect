package part2effects

import cats.Applicative
import cats.effect.{IO, IOApp}
import scala.concurrent.Future
import scala.util.Random

object IOTraversal extends IOApp.Simple {

  import scala.concurrent.ExecutionContext.Implicits.global

  //---------------------------------------------------------------------------
  // 1. Traverse for Futures

  // Simulates a heavy, time-consuming computation.
  // Named 'heavyComputation' in the original course.
  def computeAsFuture(string: String): Future[Int] = Future {
    Thread.sleep(Random.nextInt(1000))  // sleep at most one second
    string.split(" ").length
  }

  val workload: List[String] = List(
    "I quite like Cats Effect", "Scala is great", "looking forward to some awesome stuff")

  // Place the futures in a method because otherwise the futures start computing right
  // away, as Future is not an effect type.
  def clunkyFutures(): Unit = {
    // Run the workload through the heavy computation
    val futures: List[Future[Int]] = workload map computeAsFuture
    // Future[List[Int]] would be hard to obtain. This is where traverse comes in

    // foreach is used twice: first for list then on every single future
    futures.foreach(_.foreach(println))
  }

  // Use traversal
  import cats.Traverse
  import cats.instances.list._

  val listTraverse = Traverse[List]

  def traverseFutures(): Unit = {
    val singleFuture: Future[List[Int]] = listTraverse.traverse(workload)(computeAsFuture)
    //  ^ ^ This stores ALL the results, and is a LOT easier to process.
    // Traverse allows double-nested data structures to be defined inside out.
    singleFuture foreach println  // Prints all the results in one go
  }

  //---------------------------------------------------------------------------
  // 2. Traverse for IO

  import utils._  // Import the myDebug method

  // The same heavy computation as IO rather than as Future.
  // (IO's apply is the same as calling IO.delay)
  def computeAsIO(string: String): IO[Int] = IO {
    Thread.sleep(Random.nextInt(1000))  // sleep at most one second
    string.split(" ").length
  }.myDebug

  // Run the workload through the heavy computation to produce a list of IOs.
  // computeAsIO applied to an element turns it into an IO[Int] i.e.
  // computeAsIO(element) :: IO[Int]
  val ios: List[IO[Int]] = workload map computeAsIO

  // Call traverse to instead flip it to an IO of List.
  val singleIO: IO[List[Int]] = listTraverse.traverse(workload)(computeAsIO)

  //---------------------------------------------------------------------------
  /*
    override def run: IO[Unit] = singleIO.void
    Output:
    [io-compute-0] 5
    [io-compute-0] 3
    [io-compute-0] 6
   */

  // Calculates the sum.
  override def run: IO[Unit] =
    parallelSingleIO
      .map(_.sum)
      .myDebug    // prints the sum
      .void       // discard the result list

  /* Output from using parallelSingleIO: Each result is calculated on a different thread
    [io-compute-1] 6
    [io-compute-0] 5
    [io-compute-3] 3
    [io-compute-3] 14
   */

  /* Output if we use singleIO:
    [io-compute-3] 5
    [io-compute-3] 3
    [io-compute-3] 6
    [io-compute-3] 14
   */

  //---------------------------------------------------------------------------
  // 3. Parallel Traversal

  import cats.syntax.parallel._   // parTraverse extension method
  val parallelSingleIO: IO[List[Int]] = workload parTraverse computeAsIO

  // We used the parTraverse extension method instead of using the Traverse
  // type class instance manually or explicitly (i.e. val listTraverse)
  // The parTraverse method aggregates the parallel results into an IO of
  // list of integers without us having to care about which result is
  // evaluated on which thread.

  // This is very powerful because we can distribute parallel workloads
  // very easily across different threads with a combination of parallel
  // and traverse.

  //---------------------------------------------------------------------------
  // 4. Exercises
  //    Turn double nested wrappers inside-out

  import cats.syntax.traverse._ // Import the traverse extension method

  // Turn the list of IOs inside out to IO of List using the traverse API.
  // The identity function gets lifted into the applicative context of IO
  def sequence[A](listOfIOs: List[IO[A]]): IO[List[A]] =
    // listTraverse.traverse(listOfIOs)(identity)
    listOfIOs traverse identity

  // hard version: define sequence for any kind of container, not just list
  // The only requirement is that F is context bound to Traverse, i.e. there's
  // a Traverse[F] in scope.
  // Turns a double-nested container inside-out.
  def sequenceGeneral[F[_] : Traverse, A](wrapperOfIOs: F[IO[A]]): IO[F[A]] =
    // Traverse[F].traverse(ios)(identity)  // my original attempt
    wrapperOfIOs traverse identity

  // parallel version of sequence
  def parSequence[A](listOfIOs: List[IO[A]]): IO[List[A]] =
    listOfIOs parTraverse identity

  // parallel version of sequenceGeneral
  def parSequenceGeneral[F[_] : Traverse, A](wrapperOfIOs: F[IO[A]]): IO[F[A]] =
    wrapperOfIOs parTraverse identity

  // existing sequence API from Cats
  val singleIO_v2: IO[List[Int]] = listTraverse.sequence(ios)

  // parallel sequencing
  val parallelSingleIO_v2: IO[List[Int]] = parSequence(ios) // from the exercise
  val parallelSingleIO_v3: IO[List[Int]] = ios.parSequence  // extension method from the Parallel syntax package.

  // My own attempt to generalize it even further. Not part of the instructor-
  // assigned exercises. We need Applicative[G] because traverse requires the
  // target type constructor to be applicative.
  // The identity function gets lifted into the Applicative context of G
  def sequenceVeryGeneral[F[_] : Traverse, G[_] : Applicative, A](elements: F[G[A]]): G[F[A]] =
    elements traverse identity
}
