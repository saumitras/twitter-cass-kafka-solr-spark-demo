CREATE KEYSPACE twitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE twitter.ct1 (
  topic text,
  id text,
  username text,
  userId text,
  userScreenName text,
  userDesc text,
  userProfileImgUrl text,
  favCount bigint,
  retweetCount bigint,
  lang text,
  place text,
  message text,
  isSensitive boolean,
  isTruncated boolean,
  isFavorited boolean,
  isRetweeted boolean,
  isRetweet boolean,
  createdAt timestamp,
  creationDate timestamp,

  PRIMARY KEY ((topic, creationDate), username, id)
)