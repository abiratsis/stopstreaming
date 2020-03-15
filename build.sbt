name := "StopStreamingGracefully"

version := "0.1"

scalaVersion := "2.12.9"
val sparkVersion = "2.4.5"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core" % sparkVersion,
  "org.apache.spark"     %% "spark-sql"  % sparkVersion,
  "org.apache.spark"     %% "spark-hive" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "com.databricks" % "dbutils-api_2.11" % "0.0.4"
)

logBuffered in Test := false