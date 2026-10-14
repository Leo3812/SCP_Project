
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "EarthquakeCo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.3" % "provided"
    ),
    assembly / mainClass := Some("Main"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs*) =>
        MergeStrategy.discard

      case "module-info.class" =>
        MergeStrategy.discard

      case x if x.endsWith("Log.class") =>
        MergeStrategy.first

      case x if x.endsWith("NoOpLog.class") =>
        MergeStrategy.first

      case _ =>
        MergeStrategy.first
    }
  )
