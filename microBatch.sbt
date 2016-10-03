
// compile with sbt spDist
name := "micro-batch"

version := "1.0"

scalaVersion := "2.11.8"


resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"



libraryDependencies += "org.apache.spark" %% "spark-core_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"


