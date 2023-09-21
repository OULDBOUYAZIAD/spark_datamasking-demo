ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

// spark core


lazy val root = (project in file("."))
  .settings(
    name := "spark_datamasking"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test


