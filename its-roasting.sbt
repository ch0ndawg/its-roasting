name := "it-s-roasting"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.1"
mergeStrategy in assembly := {

  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard

  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard

  case "log4j.properties"                                  => MergeStrategy.discard

  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines

  case "reference.conf"                                    => MergeStrategy.concat

  case _                                                   => MergeStrategy.first

}

