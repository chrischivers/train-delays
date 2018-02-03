name := "traindelays"

version := "0.1"

scalaVersion := "2.12.4"

unmanagedJars in Compile += file("lib/gozirra-client-0.4.1.jar")


val circeVersion = "0.9.0-M2"
val fs2Version = "0.10.0-M8"
val http4sVersion = "0.18.0-M5"
val doobieVersion  = "0.5.0-M9"
val scalacacheVersion  = "0.21.0"


libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-java8"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io" % fs2Version
)

libraryDependencies ++= Seq(
  "org.http4s"     %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)
libraryDependencies ++= Seq(
  "org.tpolecat"               %% "doobie-core"             % doobieVersion,
  "org.tpolecat"               %% "doobie-hikari"           % doobieVersion,
  "org.tpolecat"               %% "doobie-postgres"         % doobieVersion,
  "org.tpolecat"               %% "doobie-h2"               % doobieVersion % "test, it",
  "org.tpolecat"               %% "doobie-scalatest"        % doobieVersion % "test, it"
)

libraryDependencies ++= Seq(
  "com.github.cb372" % "scalacache-core_2.12" % scalacacheVersion,
  "com.github.cb372" %% "scalacache-guava" % scalacacheVersion,
  "com.github.cb372" %% "scalacache-cats-effect" % scalacacheVersion
)

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test, it",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "io.circe" %% "circe-fs2" % "0.9.0-M3",
  "org.flywaydb" % "flyway-core" % "4.2.0",
  "ch.qos.logback" % "logback-classic" % "1.1.9",
  "javax.mail" % "mail" % "1.5.0-b01",
  "com.github.etaty" %% "rediscala" % "1.8.0",
  "com.google.api-client" % "google-api-client" % "1.23.0"
)



scalacOptions += "-Ypartial-unification"

configs(IntegrationTest)

Defaults.itSettings

internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value)
parallelExecution in IntegrationTest := false