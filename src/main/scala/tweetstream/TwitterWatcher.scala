package tweetstream

import akka.actor.{ActorRef, Props, ActorSystem}
import commons.Logging
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import commons.Types.Tweet


class TwitterWatcher(cb:ConfigurationBuilder, keywords:List[String], destination:ActorRef) extends Logging {


  logger.info("Starting TwitterWatcher")


  val stream = new TwitterStreamFactory(cb.build()).getInstance()

  val listener = new StatusListener {

    override def onTrackLimitationNotice(i: Int): Unit = logger.warn(s"Track limited $i tweets")

    override def onStallWarning(stallWarning: StallWarning): Unit = logger.error("Stream stalled")

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = logger.warn("Status ${statusDeletionNotice.getStatusId} deleted")

    override def onScrubGeo(l: Long, l1: Long): Unit = logger.warn(s"Geo info scrubbed. userId:$l, upToStatusId:$l1")

    override def onException(e: Exception): Unit = logger.error("Exception occurred. " + e.getMessage)

    override def onStatus(status: Status): Unit = {

      val retweetCount = if(status.getRetweetedStatus == null) 0 else status.getRetweetedStatus.getRetweetCount
      val userDesc = if(status.getUser.getDescription == null) "null" else status.getUser.getDescription
      val userProfileImgUrl = if(status.getUser.getProfileImageURL == null) "null" else status.getUser.getProfileImageURL
      val lang = if(status.getLang == null) "null" else status.getLang
      val place = if(status.getPlace == null) "null" else status.getPlace.getFullName

      val tweet = Tweet(
        id = status.getId.toString,
        username = status.getUser.getName,
        userId = status.getUser.getId,
        userScreenName = status.getUser.getScreenName,
        userDesc = userDesc,
        userProfileImgUrl = userProfileImgUrl,
        createdAt = status.getCreatedAt.getTime,
        favCount = status.getFavoriteCount,
        retweetCount = retweetCount,
        lang = lang,
        place = place,
        message = status.getText,
        isSensitive = status.isPossiblySensitive,
        isTruncated = status.isTruncated,
        isFavorited = status.isFavorited,
        isRetweeted = status.isRetweeted,
        isRetweet = status.isRetweet
      )
      logger.info("Msg: " + tweet.message + "\n\n")
      destination ! tweet
    }

  }


  def startTracking() = {
    stream.addListener(listener)
    val fq = new FilterQuery()
    fq.track(keywords.mkString(","))
    stream.filter(fq)
  }

  def stopTracking() = {
    stream.shutdown()
  }


}
