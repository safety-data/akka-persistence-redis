import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

val akkaVersion = "2.4.17"
val redisScalaVersion = "1.8.0"

lazy val dependencies = Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query-experimental" % akkaVersion,
  "com.github.etaty" %% "rediscala" % redisScalaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % "test")

lazy val root = project.in(file("."))
  .settings(
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    organization := "com.safetydata",
    name := "akka-persistence-redis",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.12.1",
    libraryDependencies ++= dependencies,
    parallelExecution in Test := false,
    scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits", "-implicits-show-all", "-diagrams", "-doc-title", "Akka Persistence Redis API Documentation", "-doc-version", version.value, "-doc-footer", "Copyright © 2017 Safety Data"),
    autoAPIMappings := true,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"))
  .settings(scalariformSettings)
  .settings(
    ScalariformKeys.preferences := {
    ScalariformKeys.preferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(MultilineScaladocCommentsStartOnFirstLine, true)
    })
