package utils.general

import cats.Functor
import cats.effect.MonadCancel
import cats.syntax.functor.*
import scala.concurrent.duration._

extension [F[_], A](fa: F[A]) {
  def myDebug(using functor: Functor[F]): F[A] =
    fa map { a =>
      val t = Thread.currentThread().getName
      println(s"[$t] $a") // print thread name and IO value
      a // returns the same fa, but prints something before
    }
}

// This method will apply as an extension method to any effect type F for
// which there's a MonadCancel in scope.
// It's not an extension method because we will first have to create an effect
// and then call unsafeSleep on that. So it's a standalone method here.
def unsafeSleep[F[_], E](duration: FiniteDuration)(using mc: MonadCancel[F, E]): F[Unit] =
  mc pure (Thread sleep duration.toMillis)

