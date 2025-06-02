package part2effects

import cats.effect.{ExitCode, IO, IOApp}
import scala.io.StdIn

/* Key takeaway: To avoid boilerplate code while running Cats Effect programs,
 * either extend the IOApp or IOApp.Simple traits and override the run method.
 */
object IOApps {

  // What we have been doing until now is define an IO such as the following
  // one and call unsafeRunSync() on it in main
  val program: IO[Unit] =
    for
      _ <- IO(println("Write something and hit Enter"))
      line <- IO(StdIn.readLine())
      _ <- IO(println(s"You've just written: $line"))
    yield ()
}

/*
    The pattern demonstrated in the TestApp object is so common in Cats Effect
    applications that it's actually deconstructed in CE by creating a special
    kind of object that extends a special kind of trait specific to Cats Effect
    IO apps called IOApp. This is demonstrated in the FirstCEApp object below.
*/
object TestApp {

  import IOApps._

  def main(args: Array[String]): Unit = {
    import cats.effect.unsafe.implicits.global
    program.unsafeRunSync()
  }
}

//---------------------------------------------------------------------------
object FirstCEApp extends IOApp {

  import IOApps._
  override def run(args: List[String]): IO[ExitCode] = {
    // We can't return just program, as its return type is IO[Unit], while
    // we want to return IO[ExitCode]. So we map it to return an ExitCode:
    // program map (_ => ExitCode.Success) // Same as the following:
    program as ExitCode.Success
  }

  // Success is the most commonly used exit code. Failure is another.
  // We can also override it and return some other number to the operating
  // system, but that's less common.
}

//---------------------------------------------------------------------------
// run method with no argument list and returns Unit. Simple is an inner
// trait of IOApp.
object MySimpleApp extends IOApp.Simple {

  import IOApps._
  override def run: IO[Unit] = program
}