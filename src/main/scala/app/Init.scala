package app

import akka.actor.{Props, ActorSystem}
import cassandra.CassWriter
import commons.Types.StartRead
import kafka.KafkaTweetProducer
import kafka.KafkaTweetConsumer
import solr.SolrWriter
import tweetstream.TwitterWatcher
import twitter4j.conf.ConfigurationBuilder

object Init extends App {

  //twitter keywords to track
  val topics = List("#INDvPAK", "#CT17Final", "#indvpak", "#pakvind", "#CT17", "#ct2017final")

  //twitter auth
  val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true)
  cb.setOAuthConsumerKey("p5vABCjRWWSXNBkypnb8ZnSzk")
  cb.setOAuthConsumerSecret("wCVFIpwWxEyOcM9lrHa9TYExbNsLGvEUgJucePPjcTx83bD1Gt")
  cb.setOAuthAccessToken("487652626-kDOFZLu8bDjFyCKUOCDa7FtHsr22WC3PMH4iuNtn")
  cb.setOAuthAccessTokenSecret("4W3LaQTAgGoW5SsHUAgp6gK9b5AKgl8hRcFnNYgvPTylU")

  val zkHostKafka = "localhost:2181/kafka"
  val kafkaBrokers = "localhost:9092"
  val topic = "ct1"

  val zkHostSolr = "localhost:2181"
  val solrCollection = "ct1"

  val cassSeeds = "localhost"
  val cassKeyspace = "twitter"
  val cassCf = "ct1"

  val system = ActorSystem("TwitterAnalysis")

  val kafkaProducer = system.actorOf(Props(new KafkaTweetProducer(kafkaBrokers, topic)),
    name = "kafka_tweet_producer")

  val twitterStream = new TwitterWatcher(cb, topics, kafkaProducer)
  twitterStream.startTracking()

  val solrWriter = system.actorOf(Props(
    new SolrWriter(zkHostSolr, solrCollection, commitAfterBatch = true)), name = "solr_writer")

  val cassWriter = system.actorOf(Props(
    new CassWriter(cassSeeds, cassKeyspace, cassCf, topic)), name = "cass_writer")

/*  val cassConsumer = new KafkaTweetConsumer(zkHostKafka, "tweet-cass-consumer", topic, cassWriter)
  cassConsumer.read()


  val solrConsumer = new KafkaTweetConsumer(zkHostKafka, "tweet-solr-consumer", topic, solrWriter)
  solrConsumer.read()*/

  val cassConsumer = system.actorOf(Props(
    new KafkaTweetConsumer(zkHostKafka, "tweet-cass-consumer", topic, cassWriter)), name = "cass_consumer")

  val solrConsumer = system.actorOf(Props(
    new KafkaTweetConsumer(zkHostKafka, "tweet-solr-consumer", topic, solrWriter)), name = "solr_consumer")

  cassConsumer ! StartRead
  solrConsumer ! StartRead


}
