organization := "com.automation"

name := "recordFilter"

version := "0.2"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"


libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"