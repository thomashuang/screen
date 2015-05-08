// Project name (artifact name in Maven)
name := "screen"

// orgnization name (e.g., the package name of the project)
organization := "com.hana"

version := "1.0-SNAPSHOT"
scalaVersion := "2.10.4"
// project description
description := "Treasure Data Project"

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

//
exportJars := true

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
   "org.mongodb" % "mongo-java-driver" % "2.13.1",
   "org.mongodb" % "bson" % "2.13.1",
   "org.apache.spark" % "spark-core_2.10" % "1.3.0",
   "org.apache.spark" % "spark-sql_2.10" % "1.3.0",
   "org.apache.hadoop" % "hadoop-client" % "2.5.2"
)