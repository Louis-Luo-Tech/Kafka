name := "Kafka"

version := "0.1"

scalaVersion := "2.12.10"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4"
