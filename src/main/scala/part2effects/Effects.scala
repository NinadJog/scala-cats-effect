package part2effects

import scala.concurrent.Future

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
  def main(args: Array[String]): Unit = {
    anIO.unsafeRun() // This is the only place where the side-effect will be produced.
  }
}
