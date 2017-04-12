import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

val akkaVersion = "2.4.17"
val redisScalaVersion = "1.8.0"

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact in Test := false,
  // The Nexus repo we're publishing to.
  publishTo := (version { (v: String) =>
    val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
      else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }).value,
  pomIncludeRepository := { x => false },
  pomExtra := (
    <developers>
      <developer>
        <id>satabin</id>
        <name>Lucas Satabin</name>
        <email>satabin@safety-data.com</email>
      </developer>
    </developers>
    <ciManagement>
        <system>travis</system>
        <url>https://travis-ci.org/#!/safety-data/akka-persistence-redis</url>
      </ciManagement>
      <issueManagement>
        <system>github</system>
        <url>https://github.com/safety-data/akka-persistence-redis/issues</url>
      </issueManagement>
    )
  )

lazy val siteSettings = Seq(
  ghpagesNoJekyll := true,
  git.remoteRepo := scmInfo.value.get.connection)

lazy val dependencies = Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query-experimental" % akkaVersion,
  "com.github.etaty" %% "rediscala" % redisScalaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % "test",
  "com.github.pocketberserker" %% "scodec-msgpack" % "0.6.0" % "test")

lazy val root = project.in(file("."))
  .enablePlugins(SiteScaladocPlugin, GhpagesPlugin)
  .settings(publishSettings: _*)
  .settings(siteSettings: _*)
  .settings(
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    organization := "com.safety-data",
    name := "akka-persistence-redis",
    version := "0.1.0",
    licenses += ("The Apache Software License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage := Some(url("https://github.com/safety-data/akka-persistence-redis")),
    scmInfo := Some(ScmInfo(url("https://github.com/safety-data/akka-persistence-redis"), "git@github.com:safety-data/akka-persistence-redis.git")),
    scalaVersion := "2.12.1",
    crossScalaVersions := Seq("2.12.1", "2.11.8"),
    libraryDependencies ++= dependencies,
    parallelExecution in Test := false,
    scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits", "-implicits-show-all", "-diagrams", "-doc-title", "Akka Persistence Redis", "-doc-version", version.value, "-doc-footer", "Copyright © 2017 Safety Data"),
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
