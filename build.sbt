organization := "couscous.asado"

scalaVersion := "2.11.8"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.apache.spark" %% "spark-mllib" % "2.2.0",
    "joda-time" % "joda-time" % "2.9.9"
) 