package part4coordination

import cats.effect.std.Semaphore
import cats.effect.{IO, IOApp}
import scala.concurrent.duration._
import scala.util.Random
import cats.syntax.parallel._
import utils._

/**
 * A semaphore is a concurrency primitive that allows only a certain number of
 * threads into a critical region.
 *
 * It has an internal counter named permits that it allocates to whichever
 * thread requests permits. If a thread requests a number of permits that are
 * not available, the thread will block until they become available.
 *
 * Semaphore is built on top of the Ref and Deferred primitives. Challenge
 * exercise: Build semaphore using Ref and Deferred.
 */
object Semaphores extends IOApp.Simple {

  // The semaphore constructor is itself an effect because it has an IO
  // that will create a semaphore when it evaluates. Semaphore is polymorphic,
  // i.e. it is higher-kinded in the effect type; here we create a semaphore of IO.
  val semaphore: IO[Semaphore[IO]] = Semaphore[IO](2)  // 2 total permits

  // example: limit the number of concurrent sessions on a fictitious server
  // This effect will be performed when we acquire a semaphore.
  def doWorkWhileLoggedIn(): IO[Int] =
    (IO sleep 1.second) >> IO(Random nextInt 100)

  // Describes what happens when a user wants to log in to our server
  def login(id: Int, semaphore: Semaphore[IO]): IO[Int] = for {
    _ <- IO(s"[session $id] waiting to log in...").myDebug
    _ <- semaphore.acquire  // similar to mutex.acquire, except that it has multiple permits

    // start critical section
    _ <- IO(s"[session $id] logged in; working...").myDebug
    result <- doWorkWhileLoggedIn()
    _ <- IO(s"[session $id] done: $result, logging out").myDebug
    // end critical section

    _ <- semaphore.release // releases the permit acquired earlier
    // If we don't release the semaphore, it will block other fibers

  } yield result

  //---------------------------------------------------------------------------
  /**
   * Create 3 fibers and pass them a semaphore that has just 2 permits.
   *
   */
  def demoSemaphore(): IO[Unit] = for {
    sem <- Semaphore[IO](2)
    user1fib <- login(1, sem).start
    user2fib <- login(2, sem).start
    user3fib <- login(3, sem).start
    _ <- user1fib.join
    _ <- user2fib.join
    _ <- user3fib.join
  } yield ()

  /**
   * Output:
   * We can see that sessions 2 and 1 logged in first. Session 3 was able to
   * log in only after one of the two sessions completed.
   *
   * [io-compute-1] [session 1] waiting to log in...
   * [io-compute-0] [session 2] waiting to log in...
   * [io-compute-3] [session 3] waiting to log in...
   * [io-compute-0] [session 2] logged in; working...
   * [io-compute-1] [session 1] logged in; working...
   * [io-compute-1] [session 1] done: 13, logging out
   * [io-compute-0] [session 2] done: 34, logging out
   * [io-compute-2] [session 3] logged in; working...
   * [io-compute-2] [session 3] done: 24, logging out
   */

  //===========================================================================
  /**
   * Acquiring several permits
   *
   * A semaphore with a single permit amounts to a mutex. A task can also
   * request multiple permits. The acquisition of permits is all-or-nothing:
   * all the permits should be acquired at the same time, or none will be
   * acquired if a sufficient number of permits are not available. For example,
   * if a task needs 3 permits but only 2 are available, none of the permits
   * will be acquired by the task.
   */
  def weightedLogin(id: Int, requiredPermits: Int, sem: Semaphore[IO]): IO[Int] =
    for {
      _ <- IO(s"[session $id] waiting to log in...").myDebug
      _ <- sem.acquireN(requiredPermits)

      // start critical section
      _ <- IO(s"[session $id] logged in; working...").myDebug
      result <- doWorkWhileLoggedIn()
      _ <- IO(s"[session $id] done: $result, logging out").myDebug
      // end critical section

      // It's a good practice to release the same number of permits that we
      // acquired earlier in the IO chain, otherwise there will be dangling
      // permits.
      _ <- sem.releaseN(requiredPermits)
    } yield result

  //---------------------------------------------------------------------------
  /**
   * Here the third task has no chance of completing its work because it is
   * requesting 3 permits, when the semaphore can issue a maximum of 2.
   * So task 3 will experience starvation
   */
  def demoWeightedSemaphore(): IO[Unit] = for {
    sem <- Semaphore[IO](2) // Just 2 permits
    user1fib <- weightedLogin(1, 1, sem).start
    user2fib <- weightedLogin(2, 2, sem).start
    user3fib <- weightedLogin(3, 3, sem).start  // will be starved when semaphore issues max 2 permits
    _ <- user1fib.join
    _ <- user2fib.join
    _ <- user3fib.join
  } yield ()

  /**
   * Output:
   * (Task 3 never runs, as it gets starved from not being able to get 3 permits.
   * Tasks 2 and 1 execute sequentially.)
   *
   * [io-compute-3] [session 1] waiting to log in...
   * [io-compute-0] [session 2] waiting to log in...
   * [io-compute-1] [session 3] waiting to log in...
   * [io-compute-0] [session 2] logged in; working...
   * [io-compute-0] [session 2] done: 30, logging out
   * [io-compute-1] [session 1] logged in; working...
   * [io-compute-1] [session 1] done: 83, logging out
   */

  //===========================================================================
  /**
   * Exercise:
   * 1. What's wrong, if anything, with the following 'val users' code?
   * 2. Why?
   * 3. What's the fix?
   */
  // semaphore with 1 permit == mutex
  val mutex: IO[Semaphore[IO]] = Semaphore[IO](1)

  /**
    What's wrong is that a NEW semaphore is created for every task, so all
    the tasks end up running in parallel rather than sequentially, as each
    of them readily acquires a different semaphore instead of trying to
    acquire the same one.
  */
  val users: IO[List[Int]] = (1 to 10).toList parTraverse { id =>
    for {
      semaphore <- mutex  // WRONG!! because a new semaphore is being created
      // for each task. The above line is equivalent to flatMap Semaphore[IO](1)

      _ <- IO(s"[session $id] waiting to log in...").myDebug
      _ <- semaphore.acquire // similar to mutex.acquire, except that it has multiple permits

      // start critical section
      _ <- IO(s"[session $id] logged in; working...").myDebug
      result <- doWorkWhileLoggedIn()
      _ <- IO(s"[session $id] done: $result, logging out").myDebug
      // end critical section

      _ <- semaphore.release
    } yield result
  }

  //---------------------------------------------------------------------------
  /**
   * My solution (works correctly):
   *
   * Corrected code passes the same semaphore to the loginWithMutex function.
   * The semaphore is passed by the caller, the method demoSemaphoreAsMutex
   */
  def loginWithMutex(id: Int, semaphore: Semaphore[IO]): IO[Int] = for {
    _ <- IO(s"[session $id] waiting to log in...").myDebug
    _ <- semaphore.acquire
    // start critical section
    _ <- IO(s"[session $id] logged in; working...").myDebug
    result <- doWorkWhileLoggedIn()
    _ <- IO(s"[session $id] done & logging out: $result").myDebug
    // end critical section
    _ <- semaphore.release
  } yield result

  //---------------------------------------------------------------------------
  def demoSemaphoreAsMutex(): IO[List[Int]] = for {
    semaphore <- mutex
    results <- (1 to 10).toList parTraverse (loginWithMutex(_, semaphore))
  } yield results

  /**
   * Output of demoSemaphoreAsMutex:
   * (The tasks run sequentially, as they should)
   *
   * [io-compute-3] [session 9] waiting to log in...
   * [io-compute-1] [session 10] waiting to log in...
   * [io-compute-3] [session 3] done & logging out: 31
   * [io-compute-1] [session 5] logged in; working...
   * [io-compute-1] [session 5] done & logging out: 47
   * ...
   * [io-compute-3] [session 10] done & logging out: 64
   * [io-compute-3] List(68, 15, 31, 49, 47, 87, 1, 89, 95, 64)
   */

  //---------------------------------------------------------------------------
  // Instructor's solution (functionally same as mine but uses flatMap and
  // continues to use a val instead of a method.
  val usersFixed: IO[List[Int]] = mutex flatMap { semaphore =>
    (1 to 10).toList parTraverse { id =>
      for {
        _ <- IO(s"[session $id] waiting to log in...").myDebug
        _ <- semaphore.acquire
        // start critical section
        _ <- IO(s"[session $id] logged in; working...").myDebug
        result <- doWorkWhileLoggedIn()
        _ <- IO(s"[session $id] done: $result, logging out").myDebug
        // end critical section
        _ <- semaphore.release
      } yield result
    }
  }

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = usersFixed.myDebug.void
}
