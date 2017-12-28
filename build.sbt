name := "traindelays"

version := "0.1"

scalaVersion := "2.12.4"

unmanagedJars in Compile += file(Path.userHome + "/Downloads/gozirra-client-0.4.1.jar")


val circeVersion = "0.9.0-M2"
val fs2Version = "0.10.0-M8"
val http4sVersion = "0.18.0-M5"


libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io" % fs2Version
)

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test, it",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "io.circe" %% "circe-fs2" % "0.9.0-M3")



scalacOptions += "-Ypartial-unification"

configs(IntegrationTest)

Defaults.itSettings

internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value)
parallelExecution in IntegrationTest := false