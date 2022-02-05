ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

ThisBuild / Test / fork := true

lazy val root = (project in file("."))
  .settings(
    name := "sparky",
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.0.1",
      "org.postgresql" % "postgresql" % "42.3.2"
    )
  )
