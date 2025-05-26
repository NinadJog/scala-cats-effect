import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

// Cats-Effect 3.6.x is built for Scala 3.3.x
ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "scala-cats-effect",

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.6.1", // latest as of 5/26/2025
    )
  )
