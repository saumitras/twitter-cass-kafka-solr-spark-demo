package commons

import java.util.Date

object Types {

  case class Tweet(id:String, username:String, userId:Long, userScreenName:String,
                   userDesc:String, userProfileImgUrl:String, favCount:Long, retweetCount:Long,
                   lang:String, place:String, message:String, isSensitive:Boolean,
                   isTruncated:Boolean, isFavorited:Boolean, isRetweeted:Boolean,
                   isRetweet:Boolean, createdAt:Long)

  case object FlushBuffer

  case object StartRead
}
