package part4coordination

import cats.effect.kernel.{Deferred, Ref}
import cats.effect.{IO, IOApp}
import scala.concurrent.duration.*
import scala.util.Random
import utils.*
import cats.syntax.parallel.*
import scala.collection.immutable.Queue
import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}


abstract  class Mutex {
  def acquire: IO[Unit]
  def release: IO[Unit]
}

object Mutex {

  // State stores the state of the mutex: whether it is locked or not and
  // a queue of the fibers waiting for the mutex to be released.
  type Signal = Deferred[IO, Unit]
  case class State(locked: Boolean, waiting: Queue[Signal])

  // initial state of the mutex
  val unlocked = State(locked = false, waiting = Queue())
  // Use an atomic ref to hold the state. Modify the state whenever anyone tries
  // to acquire the mutex.

  def createSignal(): IO[Signal] = Deferred[IO, Unit]

  def create: IO[Mutex] = (Ref[IO] of unlocked) map createMutexWithCancellation // createSimpleMutex

  //---------------------------------------------------------------------------
  def createMutexWithCancellation(state: Ref[IO, State]): Mutex =
    new Mutex {
      /*
        When we block on a deferred signal (by calling signal.get), that should
        be a cancelable event because a blocked fiber can be canceled by
        an external factor. That's when we should release the mutex because
        otherwise the mutex will still be locked even though our fiber which
        is blocked has already acquired the mutex.

        Example of a cancellation flow effect: Suppose a fiber calls acquire
        while the mutex has been blocked. Acquire then simply adds a new
        signal to the queue, and the fiber will wait on that signal.

        As time passes, other fibers also call acquire and add their signals
        to the queue, so our fiber's signal gets burrowed into the queue --
        it's no longer at the end of the queue.

        But then our fiber (which has been waiting on its signal) gets
        cancelled. In that case, the effect defined with 'val cleanup' in the
        following method will start to unfold. We will have to remove ourselves
        from the queue (using filterNot) and then call release as if the blocking
        fiber released the mutex correctly.

        This is demonstrated in the method 'createCancelingTask' later in this file.
       */
      override def acquire: IO[Unit] = IO.uncancelable { poll =>
        createSignal() flatMap { signal =>

          // Unlock the mutex if we get canceled while waiting. This is needed
          // only for the cancellation logic. See detailed comment before this
          // method about the cancellation flow.
          val cleanup = state.modify {
            case State(locked, queue) =>
              val newQueue = queue filterNot (_ eq signal) // remove ourselves from the queue
              State(locked, newQueue) -> release  // release the mutex
          }.flatten // Call flatten to change it from IO[IO[Unit]] to IO[Unit]

          state.modify { // has type IO[IO[Unit]]
            case State(false, _) => State(locked = true, Queue()) -> IO.unit // arrow notation for tuples
            case State(true, queue) => State(locked = true, queue enqueue signal) -> poll(signal.get).onCancel(cleanup)
          }
            .flatten // modify returns IO[IO[Unit]]. Flatten it to IO[Unit]
        }
      }

      /*
        Releasing a lock should not be cancelable. But we don't need to wrap
        this implementation in IO.uncancelable because state.modify is an
        atomic operation on its own. So the release code works as it is.
       */
      override def release: IO[Unit] = // no need to create a new signal
        state.modify {
          case State(false, queue) => unlocked -> IO.unit
          case State(true, queue) =>
            if queue.isEmpty then
              unlocked -> IO.unit // unlock the mutex
            else { // queue is not empty
              val (signal, rest) = queue.dequeue // take a signal out of the queue
              State(true, rest) -> (signal complete()).void // complete the signal
            }
        }.flatten
    } // new Mutex

  //---------------------------------------------------------------------------
  // This was the original create method in the first exercise. It does NOT
  // handle the case where one or more of the fibers can be canceled.
  def createSimpleMutex(state: Ref[IO, State]): Mutex = new Mutex {
    /*
      Change the state of the Ref:
      - if the mutex is currently unlocked, state becomes (true, [])
      - if the mutex is locked, state becomes (true, queue + signal) and wait on that signal
     */
    override def acquire: IO[Unit] = createSignal() flatMap { signal =>
      state.modify {  // has type IO[IO[Unit]]
        case State(false, _)    => State(locked = true, Queue()) -> IO.unit // arrow notation for tuples
        case State(true, queue) => State(locked = true, queue enqueue signal) -> signal.get
      }.flatten // modify returns IO[IO[Unit]]. Flatten it to IO[Unit]
    }

    /*
      Change the state of the Ref:
      - If the mutex is unlocked, leave the state unchanged
      - If the mutex is locked
        - if the queue is empty, unlock the mutex i.e. state becomes (false, [])
        - If the queue is not empty, take a signal out of the queue and complete it,
          thereby unblocking a fiber waiting on it.
     */
    override def release: IO[Unit] = // no need to create a new signal
      state.modify {
        case State(false, queue) => unlocked -> IO.unit
        case State(true, queue) =>
          if queue.isEmpty then
            unlocked -> IO.unit  // unlock the mutex
          else {  // queue is not empty
            val (signal, rest) = queue.dequeue  // take a signal out of the queue
            State(true, rest) -> (signal complete ()).void // complete the signal
          }
      }.flatten
  } // new Mutex

} // object Mutex

//---------------------------------------------------------------------------
object MutexPlayground extends IOApp.Simple {

  // Sleep for a second, then generate a random number
  def criticalTask(): IO[Int] = (IO sleep 1.second) >> IO(Random nextInt 100)

  def createNonLockingTask(id: Int): IO[Int] =
    for {
      _ <- IO(s"[task $id] working...").myDebug
      result <- criticalTask()
      _ <- IO(s"[task $id] got result: $result").myDebug
    } yield result

  // Parallelize this task 10 times by running it on 10 different fibers
  // and collect the result as a list of int
  def demoNonLockingTasks(): IO[List[Int]] =
    (1 to 10).toList parTraverse createNonLockingTask

  //---------------------------------------------------------------------------
  def createLockingTask(id: Int, mutex: Mutex): IO[Int] = {
    for {
      _ <- IO(s"[task $id] waiting for permission...").myDebug
      _ <- mutex.acquire

      // critical section
      _ <- IO(s"[task $id] working...").myDebug
      result <- criticalTask()
      _ <- IO(s"[task $id] got result: $result").myDebug
      // end critical section

      _ <- mutex.release
      _ <- IO(s"[task $id] lock removed").myDebug
    } yield result
  }

  //---------------------------------------------------------------------------
  /**
   * All 10 tasks will start simultaneously and wait to acquire the
   * mutex. But only one task will get it, so they will proceed sequentially
   * and in order by task id.
   */
  def demoLockingTasks(): IO[List[Int]] = for {
    mutex <- Mutex.create
    results <- (1 to 10).toList parTraverse (createLockingTask(_, mutex))
  } yield results
  // only one task will proceed at  a time

  //---------------------------------------------------------------------------

  /**
   * Cancellation can be a problem as the task has acquired a mutex.
   * Test how the mutex behaves when one of the blocked fibers receives a
   * cancellation signal.
   *
   * Here's how this works if we use the mutex that does NOT have support for
   * cancellation.
   * 1. All the tasks will say 'waiting for permission', as all of them start
   *    simultaneously and the cancellation signal is sent only after 2 seconds.
   * 2. One of the tasks will acquire the mutex. While it works in the critical
   *    section, 5 of the naughty tasks (those with odd ids) will receive their
   *    cancellation signals.
   * 3. But because these naughty tasks don't release their mutex upon receiving
   *    cancellation signals, the subsequent task that's trying to acquire the
   *    mutex to do its own work will NOT be able to acquire the mutex because
   *    it has been locked, causing a DEADLOCK.
   */
  def createCancelingTask(id: Int, mutex: Mutex): IO[Int] = {
    if id % 2 == 0 then
      createLockingTask(id, mutex)
    else for {  // for odd ids, create a task and then cancel it. This is naughty behavior!
      fib     <- createLockingTask(id, mutex)
                  .onCancel(IO(s"[task $id] received cancellation!").myDebug.void)
                  .start
      _       <- (IO sleep 2.seconds) >> fib.cancel // this cancellation can be problematic
      outcome <- fib.join
      result  <- outcome.match {
                  case Succeeded(effect)  => effect
                  case Errored(_)         => IO(-1)
                  case Canceled()         => IO(-2)
                }
    } yield result
  }

  def demoCancelingTasks(): IO[List[Int]] = for {
    mutex <- Mutex.create
    results <- (1 to 10).toList parTraverse (createCancelingTask(_, mutex))
  } yield results

  /**
   * Output of demo with cancelling tasks when we use the simple mutex that
   * does NOT support cancellation. The result is a deadlock; the analysis
   * follows after the output.
   *
   * Note: In the Mutex.create method, choose 'createSimpleMutex' to run
   * this scenario.
   *
   * [io-compute-0] [task 3] waiting for permission...
   * [io-compute-1] [task 2] waiting for permission...
   * [io-compute-2] [task 4] waiting for permission...
   * [io-compute-3] [task 1] waiting for permission...
   * [io-compute-0] [task 3] working...
   * [io-compute-2] [task 5] waiting for permission...
   * [io-compute-2] [task 6] waiting for permission...
   * [io-compute-3] [task 7] waiting for permission...
   * [io-compute-2] [task 8] waiting for permission...
   * [io-compute-0] [task 10] waiting for permission...
   * [io-compute-0] [task 9] waiting for permission...
   * [io-compute-0] [task 3] got result: 54
   * [io-compute-1] [task 4] working...
   * [io-compute-0] [task 3] lock removed
   * [io-compute-2] [task 1] received cancellation!
   * [io-compute-2] [task 5] received cancellation!
   * [io-compute-3] [task 7] received cancellation!
   * [io-compute-3] [task 9] received cancellation!
   * [io-compute-1] [task 4] got result: 43
   * [io-compute-1] [task 4] lock removed
   *
   * Analysis
   * - All 10 tasks try to acquire the mutex.
   * - Task# 3 and 4 were able to get it, do their work and release the mutex.
   * - Task # 1, 5, 7, 9 received cancellation signals, and got canceled
   *   without releasing their mutexes.
   * - Task # 2, 6, 8, 10 got stuck in the queue, waiting for the mutex to be
   *   released. This caused a deadlock and the program never terminated.
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = demoCancelingTasks().myDebug.void

  /**
   * Output of demo using mutex that supports cancellations. (In Mutex.create,
   * call 'createMutexWithCancellation')
   *
   * Program terminates gracefully with no deadlocks, as all 5 canceled tasks
   * release their mutexes. The remaining 5 tasks performed their computations
   * in the critical section,
   *
   * But for some reason the final output List
   * contains all 10 entries instead of just 5, with the entries for the
   * canceled tasks being -2. (This needs to be fixed, perhaps by using
   * Option[Int] for each list entry. In that case the output will be of
   * type IO[List[Option[Int]]])
   *
   * [io-compute-1] [task 1] waiting for permission...
   * [io-compute-2] [task 4] waiting for permission...
   * [io-compute-3] [task 2] waiting for permission...
   * [io-compute-0] [task 3] waiting for permission...
   * [io-compute-3] [task 2] working...
   * [io-compute-3] [task 5] waiting for permission...
   * [io-compute-1] [task 6] waiting for permission...
   * [io-compute-2] [task 7] waiting for permission...
   * [io-compute-3] [task 8] waiting for permission...
   * [io-compute-3] [task 10] waiting for permission...
   * [io-compute-2] [task 9] waiting for permission...
   * [io-compute-3] [task 2] got result: 80
   * [io-compute-0] [task 3] working...
   * [io-compute-3] [task 2] lock removed
   * [io-compute-0] [task 3] received cancellation!
   * [io-compute-2] [task 1] received cancellation!
   * [io-compute-2] [task 4] working...
   * [io-compute-1] [task 7] received cancellation!
   * [io-compute-2] [task 6] working...
   * [io-compute-0] [task 8] working...
   * [io-compute-3] [task 5] received cancellation!
   * [io-compute-0] [task 9] received cancellation!
   * [io-compute-0] [task 10] working...
   * [io-compute-2] [task 4] got result: 85
   * [io-compute-2] [task 4] lock removed
   * [io-compute-0] [task 8] got result: 86
   * [io-compute-2] [task 6] got result: 42
   * [io-compute-1] [task 10] got result: 80
   * [io-compute-2] [task 6] lock removed
   * [io-compute-1] [task 10] lock removed
   * [io-compute-0] [task 8] lock removed
   * [io-compute-3] List(-2, 80, -2, 85, -2, 42, -2, 86, -2, 80)
   */

  /**
   *  Key takeaway: If you create your own concurrency primitive, make sure
   *  it is cancellation-aware, otherwise you could end up in nasty situations
   *  such as deadlocks.
   *  */
}
