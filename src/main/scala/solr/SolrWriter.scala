package solr

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.Actor
import commons.Logging
import commons.Types.{Tweet, FlushBuffer}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._

class SolrWriter(zkHost: String, collection: String, commitAfterBatch: Boolean) extends Actor with Logging {

  val client = new CloudSolrClient.Builder().withZkHost(zkHost).build()
  client.setDefaultCollection(collection)

  val SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val solrDateFormat = new SimpleDateFormat(SOLR_DATE_FORMAT)

  var batch = List[SolrInputDocument]()

  val MAX_BATCH_SIZE = 100

  override def receive: Receive = {
    case doc: Tweet =>
      //logger.info("Got tweet in solr consumer: " + doc.id)
      val solrDoc = new SolrInputDocument()
      solrDoc.setField("id", doc.id)
      solrDoc.setField("username", doc.username)
      solrDoc.setField("userId", doc.userId)
      solrDoc.setField("userScreenName", doc.userScreenName)
      solrDoc.setField("userDesc", doc.userDesc)
      solrDoc.setField("userProfileImgUrl", doc.userProfileImgUrl)
      solrDoc.setField("favCount", doc.favCount)
      solrDoc.setField("retweetCount", doc.retweetCount)
      solrDoc.setField("lang", doc.lang)
      solrDoc.setField("place", doc.place)
      solrDoc.setField("message", doc.message)
      solrDoc.setField("isSensitive", doc.isSensitive)
      solrDoc.setField("isTruncated", doc.isTruncated)
      solrDoc.setField("isFavorited", doc.isFavorited)
      solrDoc.setField("isRetweeted", doc.isRetweeted)
      solrDoc.setField("isRetweet", doc.isRetweet)
      solrDoc.setField("createdAt", getSolrDate(doc.createdAt))

      batch = solrDoc :: batch

      if (batch.size > MAX_BATCH_SIZE) indexBatch()


    case FlushBuffer =>
      indexBatch()

    case _ =>
      logger.warn("Unknown message")

  }


  def indexBatch(): Boolean = {
    try {
      logger.info("Flushing batch")
      client.add(batch.asJavaCollection)
      batch = List[SolrInputDocument]()
      if (commitAfterBatch) client.commit()
      true
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to indexing solr batch. Exception is " + ex.getMessage)
        ex.printStackTrace()
        batch = List[SolrInputDocument]()
        false
    }
  }

  def getSolrDate(epoch: Long) = solrDateFormat.format(new Date(epoch))

}
