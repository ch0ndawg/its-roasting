name := "it-s-roasting"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0"
mergeStrategy in assembly := {

  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard

  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard

  case "log4j.properties"                                  => MergeStrategy.discard

  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines

  case "reference.conf"                                    => MergeStrategy.concat

  case _                                                   => MergeStrategy.first

}

