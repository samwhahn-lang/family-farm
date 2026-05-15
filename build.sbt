ThisBuild / scalaVersion := "2.13.12"
ThisBuild / organization := "com.cornbelt"
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "corn-belt-weather",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % "3.5.0",
      "org.apache.spark" %% "spark-sql"   % "3.5.0",
      "io.delta"         %% "delta-spark" % "3.1.0",
      "com.softwaremill.sttp.client3" %% "core"  % "3.9.3",
      "com.softwaremill.sttp.client3" %% "circe" % "3.9.3",
      "io.circe" %% "circe-core"    % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser"  % "0.14.6",
      "com.typesafe"               %  "config"          % "1.4.3",
      "ch.qos.logback"             %  "logback-classic" % "1.4.14",
      "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.5",
      "org.scalatest"              %% "scalatest"        % "3.2.17" % Test
    ),

    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "-Dhadoop.home.dir=C:/hadoop",
      "-Djava.library.path=C:/hadoop/bin",
      "-Dlog4j2.configurationFile=log4j2.properties"
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )