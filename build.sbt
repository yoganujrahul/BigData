name := "scala sbt project"
version := "0.1"
scalaVersion := "2.11.8"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.confluent" % "kafka-avro-serializer" % "3.1.1",
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.2.0"
)

resolvers += "confluent" at "http://packages.confluent.io/maven/"
