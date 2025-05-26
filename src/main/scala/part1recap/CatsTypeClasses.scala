package part1recap

object CatsTypeClasses {

  /* Cats type classes we will need for cats-effect
    - applicative
    - functor
    - flatMap
    - monad
    - apply
    - monadError/applicativeError
    - traverse
   */

  //---------------------------------------------------------------------------
  // Functor - describes "mappable" data structures
  // The cats functor has a definition similar to the following one.
  // Note that this is a higher-kinded type class, as it takes F[_], a wrapper type.
  trait MyFunctor[F[_]]:
    def map[A, B](initialValue: F[A])(f: A => B): F[B]

  import cats.Functor   // The cats library has implicit functor instances for common data types
  import cats.instances.list.*

  val listFunctor = Functor[List]

  // used for generalizable "mapping" APIs
  // we require F to be a Functor; this is specified in the using clause
  // F is a container type (a one-hole type) such as List, Option, Try, Future, etc.
  def increment[F[_]](container: F[Int])(using functor: Functor[F]): F[Int] =
    functor.map(container)(_+1)

  import cats.syntax.functor.*  // to use extension methods
  // If we have a Functor in scope (specified in the context bound here),
  // then the extension method 'map' is available on ANY F[Int], such
  // as List[Int], Option[Int], etc.
  def increment_v2[F[_] : Functor](container: F[Int]): F[Int] =
    container map (_+1)

  // Almost all the type classes in Cats have been created with the goal of
  // generalizing code.

  //---------------------------------------------------------------------------
  // Applicative
  // Applicative means the ability to wrap code in a wrapper type, i.e. the
  // ability to "wrap" types.

  // The 'pure' method can be thought of as a constructor of the wrapper
  // type F.
  trait MyApplicative[F[_]] extends MyFunctor[F]:
    def pure[A](value: A): F[A] // wraps the value in the wrapper type F

  // applicatives include Lists, Try, Option, Future, etc.
  import cats.Applicative

  // An applicative can be fetched by the compiler by using the apply method of
  // that type class:
  val applicativeList = Applicative[List]
  val aSimpleList: List[Int] = applicativeList.pure(43)  // List(43)

  // We can imagine some generalizable APIs where we might need to construct an
  // F of a given type if we have access to an instance of that type.

  import cats.syntax.applicative.*    // This also imports the pure extension method
  val aSimpleList_v2 = 43.pure[List]  // type of val is List[Int]

  //---------------------------------------------------------------------------
  // FlatMap - ability to chain multiple wrapper computations

  trait MyFlatMap[F[_]] extends MyFunctor[F]:
    def flatMap[A, B](container: F[A])(f: A => F[B]): F[B]

  // FlatMap is rarely used by itself; it's more commonly used through its
  // extension method.
  import cats.FlatMap

  // calls the apply method from the companion object of FlatMap. Fetches
  // whichever given value the compiler has for List.
  val flatMapList = FlatMap[List]

  import cats.syntax.flatMap.*  // imports the flatMap extension method
  // the flatMap extension method is available for ALL types for which
  // there is a FlatMap of that type in scope.

  // Cross product of two wrapper types (instructor's solution)
  def crossProduct[F[_] : FlatMap, A, B](fa: F[A], fb: F[B]): F[(A, B)] =
    fa.flatMap(a => fb.map(b => (a, b)))

  // alternative implementation (I wrote this one)
  def crossProduct_v2[F[_] : FlatMap, A, B](fa: F[A], fb: F[B]): F[(A, B)] =
    for
      a <- fa
      b <- fb
    yield (a, b)

  //---------------------------------------------------------------------------
  // Monad = applicative + flatMap in practical terms
  // Combines the ability of applicative to wrap things in F with the ability
  // of flatMap to chain computations together
  // Bridges the gap between functional programming and imperative programming
  // on a philosophical level

  // implement the map method from functor in terms of pure (from Applicative)
  // and flatMap (from FlatMap)
  trait MyMonad[F[_]] extends MyApplicative[F] with MyFlatMap[F]:
    override def map[A, B](initialValue: F[A])(f: A => B): F[B] =
      flatMap(initialValue)(a => pure(f(a)))  // f(a) has type B and pure wraps it to make it F[B]

  import cats.Monad
  val monadList = Monad[List] // fetch the implicit monad for list

  // no need to import any of the syntax methods, as they come from functor,
  // applicative, and flatMap.

  def crossProduct_v3[F[_] : Monad, A, B](fa: F[A], fb: F[B]): F[(A, B)] =
    for
      a <- fa
      b <- fb
    yield (a, b)

  /* Type hierarchy so far. (FlatMap and Applicative both extend Functor)

      Functor -> FlatMap ->
              \             \
                Applicative  -> Monad
   */

  //---------------------------------------------------------------------------
  // Error-like typeclasses in Cats
  // ApplicativeError

  // Instructor's recommendation: go through the Cats course for this typeclass

  trait MyApplicativeError[F[_], E] extends MyApplicative[F]:
    def raiseError[A](error: E): F[A] // controls the creation of F[A] instances

  // Just as Applicative's pure builds instances of F[A], raiseError also builds
  // such instances, but it builds FAILED instances of F[A].

  import cats.ApplicativeError

  // Define a type alias ErrorOr[A] as a wrapper type over a single desirable
  // type A. String is the undesirable (error) type.
  type ErrorOr[A] = Either[String, A]
  val appErrorEither = ApplicativeError[ErrorOr, String]

  // Both the desirable value and the failed value have the same type, ErrorOr[Int].
  // So we can combine them, map and flatMap them, etc.
  val desirableValue: ErrorOr[Int] = appErrorEither.pure(42)
  val failedValue: ErrorOr[Int] = appErrorEither.raiseError("Something failed")

  import cats.syntax.applicativeError.* // imports raiseError extension method
  val failedValue_v2: ErrorOr[Int] = "Something failed".raiseError[ErrorOr, Int]

  //---------------------------------------------------------------------------
  // MonadError

  trait MyMonadError[F[_], E] extends MyApplicativeError[F, E] with Monad[F]

  import cats.MonadError
  val monadErrorEither = MonadError[ErrorOr, String]


  //---------------------------------------------------------------------------
  // Traverse

  trait MyTraverse[F[_]] extends Functor[F]:
    // traverse takes another wrapper type G, as it makes sense only for two wrapper types
    def traverse[G[_] : MyApplicative, A, B](container: F[A])(f: A => G[B]): G[F[B]]

  // Use case: Turn nested wrappers inside-out
  val listOfOptions: List[Option[Int]] = List(Some(1), Some(2), Some(3))

  // goal: convert it into an Option of List.
  import cats.Traverse

  // fetch the implicit traverse for list
  val listTraverse = Traverse[List]
  val optionList: Option[List[Int]] = listTraverse.traverse(List(1, 2, 3))(Option(_)) // equivalently, (x => Option(x))
  // In the above call to listTraverse.traverse, the types are:
  // List(1, 2, 3): List[Int] corresponds to (container: F[A]) in the method signature, while
  // (x => Option(x)) corresponds to f: A => G[B]. Here G == Option, and B == A

  // If we had used map on List(1, 2, 3) to map Option(_), it would have created
  // a list with each list element in an Option (i.e. a list of Options), as in
  // the listOfOptions above: List(Some(1), Some(2), Some(3)).
  // But 'traverse' converts it to an Option[List]

  import cats.syntax.traverse.*
  val optionList_v2: Option[List[Int]] = List(1, 2, 3) traverse (Option(_))

  //---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    println("Hello Cats type classes")
  }
}
