package cassandra

import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}
import commons.Logging

object CassReader extends Logging {

  val CASS_SEEDS = "127.0.0.1"
  val SPARK_MASTER = "spark://sam-ub:7077"

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CASS_SEEDS)
    .setJars(Seq("lib/spark-cassandra-connector-assembly-2.0.0.jar"))
    .setMaster(SPARK_MASTER)
    .setAppName("cass_query")


  lazy val sc = new SparkContext(conf)

  val data = sc.cassandraTable("twitter", "tweets")
               .select("topic", "creationdate", "retweetcount", "id", "isretweet")
               .where("topic = 'tweets' and creationdate = '2017-04-04 20:15:05+0000'")
               .groupBy(_.getLong("retweetcount"))
               .map(r => (r._1, r._2.size))
               .collect()
  //val data = sc.cassandraTable("zepp", "test").count()

  //val resp = data.sortBy(_.getLong("retweetcount"))

  logger.info("Count of rows ----------------- " + data)

  sc.stop()

}
