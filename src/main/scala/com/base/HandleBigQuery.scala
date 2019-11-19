package com.base

import java.io.File
import java.nio.channels.Channels
import java.nio.file.Files

import com.google.api.client.http.HttpResponseException
import com.google.api.services.bigquery.BigqueryScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.JobStatistics.LoadStatistics
import com.google.cloud.bigquery._
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.io.Source

object HandleBigQuery {
  val project_id = "project_id"
  val datset_name = "datset_name"
  val table_name = "table_name"
  val schemeLocation = "~/new schema"
  val dataLocation = "~/new data 100.csv"
  val bigquery: BigQuery =  BigQueryOptions.newBuilder()
    .setCredentials(GoogleCredentials.getApplicationDefault.createScoped(java.util.Collections.unmodifiableSet(
      Set(BigqueryScopes.BIGQUERY).asJava
    )))
    .setProjectId(project_id)
    .build
    .getService

  def main(args: Array[String]): Unit = {
    val schema: Schema = handleSchema(new File(schemeLocation))
//    println(schema.toString)
//    val abc = schema.getFields.asScala.filter(_.getName != "tracking_id")
//    val tmp = "select * from (" + abc.map(x=> s"select concat('${x.getName}', '.', cast(${x.getName} as String)), count(*) as count from `testing.data_02` group by 1").mkString(" union all ") + ") where count <100"
//    println(tmp)
//    handleQuery
//    HandleBigQuery.insertFile(datset_name,table_name, schema, new File(dataLocation))
    compareFileWithSchema(schema, new File(dataLocation))
  }

  def compareFileWithSchema(schema: Schema, content: File): Unit = {
    import scala.collection.JavaConverters._
    val fields: String = schema.getFields.asScala.map{ _.getName }.mkString(",")
    println(fields)
    val dataFields: String = Source.fromFile(content).getLines().next().replaceAll("[\uFEFF-\uFFFF]", "")
    println(dataFields)
    println(fields.sameElements(dataFields))
    //    println(fields.deep == dataFields.deep)
    fields == dataFields match {
      case true => println(true)
      case false => println(false)
    }
  }

  def handleQuery() ={
    val query = "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` ORDER BY word_count DESC limit 10";
    val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query).build()
    import scala.collection.JavaConversions._
    for (row <- bigquery.query(queryConfig).iterateAll) {
      println(row.get("word").getStringValue)
      println(row.get("word_count").getStringValue)
      System.out.printf("\n")
    }
  }

  def handleSchema(schemaFile: File) = {
    val fields = Source.fromFile(schemaFile).getLines().map(line => {
      val tmp = line.split(",")
      Field.of(tmp(0), getDataType(tmp(1)))
    })
    Schema.of(fields.toIterable.asJava)
  }

  def insertFile(datasetName: String, tableName: String, schema: Schema, data: File): Unit = {
    try {
    val tableId: TableId = TableId.of(datasetName, tableName)

//    bigQuery.delete(tableId)

    val tableDefinition = StandardTableDefinition.of(schema)
    val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).setExpirationTime(DateTime.now().plusHours(16).getMillis())
      .build
    bigquery.create(tableInfo)

    val writeChannelConfiguration = WriteChannelConfiguration
      .newBuilder(tableId)//.setTimePartitioning(TimePartitioning.of(Type.DAY))
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .setFormatOptions(
        CsvOptions
          .newBuilder
          .setSkipLeadingRows(1)
          .build)
      .build
    val writer = bigquery.writer(writeChannelConfiguration)
    // Write data to writer
    val stream = Channels.newOutputStream(writer)


    try
      Files.copy(data.toPath, stream)
    finally {

      writer.close()
      if (stream != null) stream.close()
    }
    // Get load job
    val job = writer.getJob.waitFor()
    //    job = job.waitFor()
    println(job.getStatistics[LoadStatistics].getOutputRows)
    println(job.getStatistics)
    } catch {
      case e: com.google.cloud.bigquery.BigQueryException => {
        println(e.getCause.asInstanceOf[HttpResponseException].getContent)
        println(e.getMessage)
        val imp = Json.parse(e.getCause.asInstanceOf[HttpResponseException].getContent)
        println((imp \ "error" \ "message").get.as[String])
      }
    }

  }


  private def getDataType(dataType:String): LegacySQLTypeName = {
    dataType.toLowerCase() match {
      case "string" => LegacySQLTypeName.STRING
      case "integer" | "int" => LegacySQLTypeName.INTEGER
      case "float" => LegacySQLTypeName.FLOAT
      case "boolean" => LegacySQLTypeName.BOOLEAN
      case "date" => LegacySQLTypeName.DATE
      case "datetime" => LegacySQLTypeName.DATETIME
    }
  }

}

