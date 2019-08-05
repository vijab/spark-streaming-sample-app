organization := "com.vijai"

name := "spark-streaming-sample-app"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"

resolvers += "HDP Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

val sparkVersion = "2.3.2"
val circeVersion = "0.10.0"

// Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-sql"
).map(_ % sparkVersion % "provided")

// Circe
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "org.apache.ambari" % "ambari-metrics-common" % "2.7.0.0.0"
libraryDependencies += "org.apache.ambari" % "ambari-metrics-hadoop-sink" % "2.4.2.2.1"
