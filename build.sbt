ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "Quantexa"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.14" % Test
)