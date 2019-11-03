name := "Aditya_Sawant_MapReduce_HW_2"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"

libraryDependencies +="org.slf4j" % "slf4j-api" % "1.7.5"

libraryDependencies +="org.slf4j" % "slf4j-simple" % "1.7.5"

libraryDependencies +="com.typesafe" % "config" % "1.3.2"

libraryDependencies +="org.scala-lang.modules" %% "scala-xml" % "1.0.6"

libraryDependencies +="junit" % "junit" % "4.12"

libraryDependencies +="org.apache.hadoop" % "hadoop-client" % "2.4.0"

libraryDependencies +="sax" % "sax" % "2.0.1"

lazy val commonSettings = Seq(
  organization := "AdityaSawant.HW2",
  version := "0.1.0-SNAPSHOT"
)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "mapreduce"
  ).
  enablePlugins(AssemblyPlugin)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}