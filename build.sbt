ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.1.0"
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.1.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.2.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"

lazy val root = (project in file("."))
  .settings(
    name := "snapshot-merger"
  )
