package part2effects

import scala.concurrent.Future
import scala.io.StdIn

object Effects {

  // example of an impure program, a side-effect
  val printSomething: Unit = println("Cats Effect")
  val printSometing_v2: Unit = ()   // not the same. We can't replace printSomething by printSomething_v2.
  // referential transparency is broken.

  // example 2. changing a variable
  var anInt = 0
  val changingVar: Unit = anInt += 1 // the act of changing the var returns a Unit
  val changingVar_v2: Unit = () // not the same because the above line modified a variable while this one does not

  /* Desires for effect types (Properties):
     - type signature describes the kind of calculation that will be performed
     - type signature describes the VALUE produced by the calculation
     - construction of the effect should be separated from its execution. (when side effects are needed, effect
       construction is separate from effect execution.)
   */

  /*
    example: Option is an effect type, as it satisfies all the 3 properties:
    - describes a possibly absent value
    - computes a value of type A if it exists
    - side effects are not needed because the Option constructor does not produce any side-effects
   */
  val anOption: Option[Int] = Option(42)

  /* example: Future is NOT an effect type, as it violates property #3.
     - describes an asynchronous computation
     - computes a value of type A, if it's successful
     - side-effect is required (allocating or scheduling a thread), but the description of the effect cannot be
       separated from its asynchronous computation. Execution is NOT separate from construction
   */
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture: Future[Int] = Future(42) // the code (42 in this case) is immediately scheduled for execution on a thread

  //---------------------------------------------------------------------------
  /* Example: MyIO data type from the Monads lesson of the Advanced Scala course
     - describes any computation that might produce side effects.
     - calculates a value of type A if it's successful
     - side effects are required for the evaluation of the zero lambda () => A
     - The creation of MyIO does not produce the side effects on construction
     Therefore MyIO IS an effect type, as it meets all the conditions for effect types.

     IO is the holy grail in bridging functional programming and expressions that
     might produce side effects.
   */
  case class MyIO[A](unsafeRun: () => A) {
    def map[B](f: A => B): MyIO[B] =
      MyIO(() => f(unsafeRun()))

    // f(unsafeRun()) :: MyIO[B] (where :: stands for 'has type', same as in Haskell)
    // f(unsafeRun()).unsafeRun() :: B
    def flatMap[B](f: A => MyIO[B]): MyIO[B] =
      MyIO(() => f(unsafeRun()).unsafeRun())
  }

  // data structure that wraps a computation that'll produce a side-effect only
  // if and when unsafeRun is called.
  val anIO: MyIO[Int] = MyIO (() => {
    println("I'm writing something")
    42
  })

  /* Summary
    Expressions performing side effects are not replaceable.
    i.e. they break referential transparency, (the ability to replace an
    expression with its value)

    Effects are data types which
    - Embody a computational concept (e.g. side effects, absence of value, etc.)
    - Are referentially transparent

    Effect properties
    - describes what kind of computation it will perform
    - the type signature describes the VALUE it will calculate
    - Separates effect description from effect execution
      (when externally visible side effects are produced)
   */

  //---------------------------------------------------------------------------
  /*
     Exercises
     1. An IO that returns the current time of the system
     2. An IO that measures the duration of a computation (hint: use ex 1)
     3. An IO that prints something to the console
     4. An IO that reads a line (a string) from the std input
   */

  // 1
  val clock: MyIO[Long] = MyIO(() => System.currentTimeMillis())

  //---------------------------------------
  // 2
  def measure[A](computation: MyIO[A]): MyIO[Long] =
    for
      startTime <- clock
      _         <- computation
      endTime   <- clock
    yield
      endTime - startTime

  // 2b - Version 2: Deconstruct the for comprehension
  def measure_v2[A](computation: MyIO[A]): MyIO[Long] =
    clock       flatMap (startTime  =>
    computation flatMap (_          =>  // The _ is the result of the computation
    clock       map     (endTime    => endTime - startTime)))

  /* Copied from above for ease of reference:

    // f(unsafeRun()) :: MyIO[B] (where :: stands for 'has type', same as in Haskell)
    // f(unsafeRun()).unsafeRun() :: B
    def flatMap[B](f: A => MyIO[B]): MyIO[B] =
      MyIO(() => f(unsafeRun()).unsafeRun())

    // map
    def map[B](f: A => B): MyIO[B] =
      MyIO(() => f(unsafeRun()))
   */

  /*
    2c - Version 3: Deconstruct the for comprehension.
    Replace ALL the flatMaps and maps with their implementation
    from the MyIO data type. Let's start with the last call, the map.

    clock map (endTime => endTime - startTime) == MyIO(() => clock.unsafeRun() - startTime)
                                               == MyIO(() => System.currentTimeMillis() - startTime)

    Substituting this in the original expression, we get
    clock       flatMap (startTime  =>
    computation flatMap (_          => MyIO(() => System.currentTimeMillis() - startTime)

    The last line above is
    computation flatMap (lambda)
    where lambda == (_ => MyIO(() => System.currentTimeMillis() - startTime)

    Expanding flatMap from its definition,
    computation flatMap lambda == MyIO(() => lambda(computation.unsafeRun()).unsafeRun()
                               == MyIO(() => lambda(____COMPUTATION____).unsafeRun()
    Substituting for lambda,
                               == MyIO(() => MyIO(() => System.currentTimeMillis() - startTime)).unsafeRun()

    In the nested MyIO of the above line, we are defining its zero lambda and immediately invoking it by calling
    unsafeRun. The result is simply the contents of the lambda: System.currentTimeMillis() - startTime.
    Substituting this, we get
                               == MyIO(() => System.currentTimeMillis_after_computation() - startTime)
                               == xxx     // let's call it xxx

    The final expression is
    clock flatMap (startTime => xxx)

    Substituting for xxx, this becomes
    clock flatMap (startTime => MyIO(() => System.currentTimeMillis_after_computation() - startTime) ===
    clock flatMap (lambda2)

    where lambda2 is the stuff in parens. Expanding the flatMap, we get
    == MyIO(() => lambda2(clock.unsafeRun()).unsafeRun())
    == MyIO(() => lambda2(System.currentTimeMillis()).unsafeRun())

    lambda2 invoked on System.currentTimeMillis() will simply return the MyIO data type xxx
    Replacing lambda2(System.currentTimeMillis()) with xxx, we get

    == MyIO(() => MyIO(() => System.currentTimeMillis_after_computation() - startTime).unsafeRun()
    == MyIO(() => MyIO(() => System.currentTimeMillis_after_computation() - System.currentTimeMillis()).unsafeRun()

    Because we are calling a MyIO with a zero lambda and then immediately calling unsafeRun on it, it simplifies to

    MyIO(() => System.currentTimeMillis_after_computation() - System.currentTimeMillis_before_computation())

    which is the desired answer
  */

  // test the measure function.
  def testTimeIO(): Unit = {
    val test = measure(MyIO(() => Thread.sleep(1000)))
    println(test.unsafeRun())
  }

  //---------------------------------------
  // 3
  def putStrLn(line: String): MyIO[Unit] = MyIO(() =>
    println(line))

  // 4
  val read: MyIO[String] = MyIO(() => StdIn.readLine())

  // Exercises 3 and 4 can be combined to write powerful functional programs.
  // Read two lines from the console and print their combination back
  def testConsole(): Unit = {
    val program: MyIO[Unit] = for {
      _ <- putStrLn("Please enter two lines of text")
      line1 <- read
      line2 <- read
      _ <- putStrLn(line1 ++ line2)
    } yield ()
    // Yielding Unit is a common pattern in Cats-Effect when we write
    // purely functional programs with for comprehensions

    program.unsafeRun() // Triggers the sequence of computations in the for comprehension

    /* The for comprehension looks like an imperative program. But rather than
       writing imperative code, we are describing it through MyIO data structure
       transformations with pure functional programming.
     */
  }

  //---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    anIO.unsafeRun() // This is the only place where the side-effect will be produced.
    testTimeIO()  // 1011
    testConsole()
  }
}
