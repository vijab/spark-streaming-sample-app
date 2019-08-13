organization := "com.vijai"

name := "spark-streaming-sample-app"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"

resolvers += "HDP Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

val sparkVersion = "2.3.1.3.0.1.0-187"
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

libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "8.1.18.v20150929"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "4.1.0" % "provided"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.27" % "provided"


assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}