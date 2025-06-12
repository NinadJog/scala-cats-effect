package part4coordination

import cats.effect.kernel.{Deferred, Ref}
import cats.effect.{IO, IOApp}
import scala.concurrent.duration.*
import scala.util.Random
import utils.*
import cats.syntax.parallel.*
import scala.collection.immutable.Queue

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

  def create: IO[Mutex] = {
    (Ref[IO] of unlocked) map { state =>
      new Mutex {
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
      }
    }
  }
}

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

  /**
   * All 10 tasks will start simultaneously and wait to acquire the
   * mutex. But only one task will get it, so they will proceed sequentially
   * and in order by task id.
   */
  def demoLockingTasks(): IO[Unit] = for {
    mutex <- Mutex.create
    results <- (1 to 10).toList parTraverse (createLockingTask(_, mutex))
  } yield results
  // only one task will proceed at  a time

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = demoLockingTasks().myDebug.void
}
