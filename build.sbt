import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

// Used Scala 3.2.2 instead of the latest 3.3.6 because the current latest
// cats-effect works with Scala 3.2. v3.2.2 seems to be the latest 3.2x
ThisBuild / scalaVersion := "3.2.2" // 3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "scala-cats-effect",

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.6.1", // latest as of 5/26/2025
    )
  )
