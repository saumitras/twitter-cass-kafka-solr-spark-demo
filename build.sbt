name := "TwitterAnalysis"

version := "1.0"

scalaVersion := "2.11.8"

//scalacOptions += "-Ylog-classpath"

libraryDependencies ++= Seq(
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.17",
  "org.apache.kafka" % "kafka_2.11" % "0.10.0.0" withSources() exclude("org.slf4j","slf4j-log4j12") exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "org.apache.avro" % "avro" % "1.7.7" withSources(),
  "org.apache.solr" % "solr-solrj" % "6.4.1" withSources(),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.datastax.cassandra" % "cassandra-driver-core"  % "3.0.2",
  "org.apache.cassandra" % "cassandra-clientutil"  % "3.0.2",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0"
  //"com.datastax.spark" % "spark-cassandra-connector-unshaded_2.11" % "2.0.1"
)
