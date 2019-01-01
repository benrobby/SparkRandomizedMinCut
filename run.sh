sbt assembly
spark-submit --class "org.benhurdelhey.PersonDeduplicator" --master local[2] --jars target/scala-2.10/PersonDeduplicator-assembly-1.0.jar target/scala-2.10/PersonDeduplicator-assembly-1.0.jar checkpointdir
