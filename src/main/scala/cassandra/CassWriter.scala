package cassandra

import akka.actor.Actor
import com.datastax.driver.core._
import commons.Logging
import commons.Types.Tweet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.Date

class CassWriter(seeds:String, keyspace:String, cf:String, topic:String) extends Actor with Logging {

  lazy val cluster = Cluster.builder().addContactPoint(seeds).build()
  lazy val session = cluster.connect(keyspace)

  lazy val prepStmt = session.prepare(s"INSERT INTO $cf (" +
        "topic, id, username, userId, userScreenName, userDesc, userProfileImgUrl, favCount," +
        "retweetCount, lang, place, message, isSensitive, isTruncated, isFavorited, isRetweeted," +
        "isRetweet, createdAt, creationDate" +
        ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")


  def receive() = {

    case t:Tweet =>
      writeToCass(t)

    case _ =>
      logger.warn("Unknown message")
  }


  def writeToCass(t:Tweet) = {

    //logger.info("Write to cass " + t.message)
    try {
     /* val boundStmt = prepStmt.bind(topic, t.id, t.username, t.userId.toString, t.userScreenName, t.userDesc,
        t.userProfileImgUrl, t.favCount, t.retweetCount, t.lang, t.place, t.message, t.isSensitive,
        t.isTruncated, t.isFavorited, t.isRetweeted, t.isRetweet, t.createdAt, t.createdAt
      )*/
      val boundStmt = prepStmt.bind()
        .setString("topic", topic)
        .setString("id",t.id)
        .setString("username", t.username)
        .setString("userId", t.userId.toString)
        .setString("userScreenName",t.userScreenName)
        .setString("userDesc",t.userDesc)
        .setString("userProfileImgUrl",t.userProfileImgUrl)
        .setLong("favCount",t.favCount)
        .setLong("retweetCount",t.retweetCount)
        .setString("lang",t.lang)
        .setString("place",t.place)
        .setString("message",t.message)
        .setBool("isSensitive",t.isSensitive)
        .setBool("isTruncated",t.isTruncated)
        .setBool("isFavorited",t.isFavorited)
        .setBool("isRetweeted",t.isRetweeted)
        .setBool("isRetweet",t.isRetweet)
        .setTimestamp("createdAt", new Date(t.createdAt))
        .setTimestamp("creationDate", new Date(t.createdAt))



      //println(boundStmt.preparedStatement().getVariables)
      //println(boundStmt.getBool("isRetweet"))
      session.execute(boundStmt)
    } catch {
      case ex: Exception =>
        logger.error("C* insert exception. Message: " + ex.getMessage)
    }

  }


}
