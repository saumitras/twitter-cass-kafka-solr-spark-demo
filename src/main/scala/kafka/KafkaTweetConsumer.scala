package kafka

import java.util.Properties

import akka.actor.{Actor, ActorRef}
import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException, Whitelist}
import kafka.serializer.DefaultDecoder
import commons.Types.{StartRead, Tweet}
import commons.Logging

import scala.io.Source

class KafkaTweetConsumer(zkHost:String, groupId:String, topic:String, destination:ActorRef) extends Actor with Logging {
  logger.info(s"Creating new consumer. zkHost: $zkHost, topic:$topic, group:$groupId, destination: ${destination.path.name}")

  private val props = new Properties()

  props.put("group.id", groupId)
  props.put("zookeeper.connect", zkHost)
  props.put("auto.offset.reset", "smallest")
  props.put("consumer.timeout.ms", "120000")
  props.put("auto.commit.interval.ms", "10000")

  private val consumerConfig = new ConsumerConfig(props)
  private val consumerConnector = Consumer.create(consumerConfig)
  private val filterSpec = new Whitelist(topic)

  val schemaString = Source.fromURL(getClass.getResource("/tweet.avsc")).mkString
  val schema = new Schema.Parser().parse(schemaString)

  def receive() = {
    case StartRead =>
      read()
  }

  private def getTweet(message: Array[Byte]): Tweet = {

    val reader = new SpecificDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(message, null)
    val record = reader.read(null, decoder)

    val tweet = Tweet(
      id = record.get("id").toString,
      username = record.get("username").toString,
      userId = record.get("userId").toString.toLong,
      userScreenName = record.get("userScreenName").toString,
      userDesc = record.get("userDesc").toString,
      userProfileImgUrl = record.get("userProfileImgUrl").toString,
      favCount = record.get("favCount").toString.toInt,
      retweetCount = record.get("retweetCount").toString.toInt,
      lang = record.get("lang").toString,
      place = record.get("place").toString,
      message = record.get("message").toString,
      isSensitive = record.get("isSensitive").toString.toBoolean,
      isTruncated = record.get("isTruncated").toString.toBoolean,
      isFavorited = record.get("isFavorited").toString.toBoolean,
      isRetweeted = record.get("isRetweeted").toString.toBoolean,
      isRetweet = record.get("isRetweet").toString.toBoolean,
      createdAt = record.get("createdAt").toString.toLong
    )
    tweet
  }

  def read() = try {
    val streams = consumerConnector.createMessageStreamsByFilter(filterSpec, 1,
      new DefaultDecoder(), new DefaultDecoder())(0)

    lazy val iterator = streams.iterator()

    while (iterator.hasNext()) {
      val tweet = getTweet(iterator.next().message())
      //logger.info("Consuming tweet: " + tweet.id)
      destination ! tweet
    }

  } catch {
    case ex: Exception =>
      ex.printStackTrace()
      None
  }

  def close() = consumerConnector.shutdown()

}

