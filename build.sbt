name := "weather-data-analysis-spark"

version := "0.1"

scalaVersion := "2.11.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"