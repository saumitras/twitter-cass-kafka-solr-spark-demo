package kafka

import java.io.ByteArrayOutputStream
import java.util.{UUID, Properties}

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{EncoderFactory, BinaryEncoder}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import commons.Types.Tweet
import commons.Logging

import scala.io.Source

class KafkaTweetProducer(brokerList:String, topic:String) extends Actor with Logging {

  val props = new Properties()
  props.put("bootstrap.servers", brokerList)
  props.put("client.id", "KafkaTweetProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  val producer = new KafkaProducer[String, Array[Byte]](props)

  val schema = new Parser().parse(Source.fromURL(getClass.getResource("/tweet.avsc")).mkString)

  override def receive: Receive = {
    case t:Tweet =>
      writeToKafka(t)

    case _ =>
      logger.warn("Unknown message received")
  }

  def writeToKafka(tweet: Tweet) = {

    val row = new GenericData.Record(schema)
    row.put("id", tweet.id)
    row.put("username", tweet.username)
    row.put("userId", tweet.userId)
    row.put("userScreenName", tweet.userScreenName)
    row.put("userDesc", tweet.userDesc)
    row.put("userProfileImgUrl", tweet.userProfileImgUrl)
    row.put("favCount", tweet.favCount)
    row.put("retweetCount", tweet.retweetCount)
    row.put("lang", tweet.lang)
    row.put("place", tweet.place)
    row.put("message", tweet.message)
    row.put("isSensitive", tweet.isSensitive)
    row.put("isTruncated", tweet.isTruncated)
    row.put("isFavorited", tweet.isFavorited)
    row.put("isRetweeted", tweet.isRetweeted)
    row.put("isRetweet", tweet.isRetweet)
    row.put("createdAt", tweet.createdAt)

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(row, encoder)
    encoder.flush()

    //logger.info("Pushing to kafka. TweetId= " + tweet.id)

    val data = new ProducerRecord[String, Array[Byte]](topic, out.toByteArray)
    producer.send(data)

  }


}
