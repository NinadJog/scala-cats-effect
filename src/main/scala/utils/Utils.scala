package utils

import cats.effect.IO

// Decorate an IO with a method called myDebug that prints the thread the IO is
// running on and the IO's value. Calling it myDebug because IO already has an
// extension method called debug with the same parameters and return type.
//
// (This method is called debug in the course probably because the course 
// used an older version of Cats Effect.)
extension [A](io: IO[A])
  def myDebug: IO[A] =
    for
      a <- io
      t = Thread.currentThread().getName
      _ = println(s"[$t] $a")  // print thread name and IO value
    yield a

