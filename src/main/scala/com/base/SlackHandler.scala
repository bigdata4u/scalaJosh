package com.base

import scalaj.http.Http
import scala.util.parsing.json._

object SlackHandler {

  def main(args: Array[String]): Unit = {
    val SLACK_INTEGRATION_HOOK = "https://hooks.slack.com/services/xyz"
    val data = Map(
      "channel" -> "#project_id",
      "username" -> "tarun.garg",
      "text" -> "test Please Ignore",
      "icon_emoji" -> ":ghost:"
    )

    val requestBody = JSONObject(data).toString()

    Http(SLACK_INTEGRATION_HOOK)
      .postData(requestBody)
      .header("content-type", "application/json")
      .asString
  }
}