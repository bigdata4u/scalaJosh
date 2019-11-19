package com.base

import java.util
import java.util.concurrent.TimeUnit

import com.google.api.core.{ApiFuture, ApiFutures}
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import play.api.libs.json.Json

object HandlePubSub {


  def main(args: Array[String]): Unit = {
    publishMessages()
  }


  @throws[Exception]
  def publishMessages(): Unit = {
    val tmp = Json.obj(
      "project" -> "project_id",
      "jobId" -> 15,
      "tableName" -> "abcd",
      "tableToJoin" -> "",
      "startDate" -> "20190311",
      "endDate" -> "20190631"
    ).toString()

    val topicName = ProjectTopicName.of("project_id", "partner-zone")
    val messageIdFutures = new util.ArrayList[ApiFuture[String]]
      val publisher = Publisher.newBuilder(topicName).build()
      val messages = util.Arrays.asList("first message")
      // schedule publishing one message at a time : messages get automatically batched
      import scala.collection.JavaConversions._
      for (message <- messages) {
        val data = ByteString.copyFrom(tmp.getBytes())
        val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
        // Once published, returns a server-assigned message id (unique within the topic)
        val messageIdFuture = publisher.publish(pubsubMessage)
        messageIdFutures.add(messageIdFuture)
      }
      // wait on any pending publish requests.
      val messageIds = ApiFutures.allAsList(messageIdFutures).get
      import scala.collection.JavaConversions._
      for (messageId <- messageIds) {
        System.out.println("published with message ID: " + messageId)
      }
      if (publisher != null) { // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown()
        publisher.awaitTermination(1, TimeUnit.MINUTES)
      }
    // [END pubsub_publish]
  }

}


