name := "PersonDeduplicator"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.1" % "provided"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}