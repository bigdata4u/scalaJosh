package com.base

import java.lang
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.google.api.core.SettableApiFuture
import com.google.cloud.bigquery._
import com.google.cloud.dlp.v2.DlpServiceClient
import com.google.cloud.dlp.v2.DlpServiceClient._
import com.google.cloud.pubsub.v1.Subscriber
import com.google.privacy.dlp.v2.Action.PublishToPubSub
import com.google.privacy.dlp.v2.InspectConfig.FindingLimits
import com.google.privacy.dlp.v2.PrivacyMetric.{KAnonymityConfig, LDiversityConfig}
import com.google.privacy.dlp.v2.{BigQueryOptions => _, Table => _, _}
import com.google.pubsub.v1.ProjectSubscriptionName

import scala.collection.JavaConverters._

object HandleDLP {

  val notAlowedColumn = Array("tracking_id", "trackingEventId", "tradeline_owner_id", "anon_id", "model_name")

  def main(args: Array[String]): Unit = {
    val tableInfo: Array[String] = "project_id.datset_name.table_name".split('.')
//    calculateKAnonymity(tableInfo(0),tableInfo(1),tableInfo(2))
//    getList(tableInfo(0))
    inspectBigQuery(
      "project_id",
      "datset_name",
      "table_name",
      10,
      List(),
      false)
  }

  def delete(projectId:String) = {
    val parent = ProjectName.of(projectId)
    val dlpServiceClient = create
    println("RISK_ANALYSIS_JOB")
    val tmp: Seq[String] = dlpServiceClient.listDlpJobs(ListDlpJobsRequest.newBuilder().setParent(parent.toString).setType(DlpJobType.RISK_ANALYSIS_JOB).build()).iterateAll().asScala.map(_.getName).toList
        tmp.foreach(name => dlpServiceClient.deleteDlpJob(DeleteDlpJobRequest.newBuilder()
          .setName(name)
          .build()))
  }

  def getList(projectId: String): Unit = {
    val parent = ProjectName.of(projectId)
    val dlpServiceClient = create
    println("RISK_ANALYSIS_JOB")
//    val tmp: Seq[String] = dlpServiceClient.listDlpJobs(ListDlpJobsRequest.newBuilder().setParent(parent.toString).setType(DlpJobType.RISK_ANALYSIS_JOB).build()).iterateAll().asScala.map(_.getName).toList
//    tmp.foreach(name => dlpServiceClient.deleteDlpJob(DeleteDlpJobRequest.newBuilder()
//      .setName(name)
//      .build()))
    dlpServiceClient.listDlpJobs(ListDlpJobsRequest.newBuilder().setParent(parent.toString).setType(DlpJobType.RISK_ANALYSIS_JOB).build()).iterateAll().forEach(
      x=>println(s"${x.getState}, ${x.getName}, ${x.getStartTime.getSeconds}, ${x.getEndTime.getSeconds}")
    )
//    println("DLP_JOB_TYPE_UNSPECIFIED")
//    dlpServiceClient.listDlpJobs(ListDlpJobsRequest.newBuilder().setParent(parent.toString).setType(DlpJobType.DLP_JOB_TYPE_UNSPECIFIED).build()).iterateAll().forEach(
//      x=>println(s"${x.getState}, ${x.getName}, ${new org.joda.time.DateTime(x.getStartTime.getSeconds, zone).toDate}, ${new org.joda.time.DateTime(x.getEndTime.getSeconds, zone).toDate}"))
//    println("UNRECOGNIZED")
//    dlpServiceClient.listDlpJobs(ListDlpJobsRequest.newBuilder().setParent(parent.toString).setType(DlpJobType.UNRECOGNIZED).build()).iterateAll().forEach(
//      x=>println(s"${x.getState}, ${x.getName}, ${new org.joda.time.DateTime(x.getStartTime.getSeconds, zone).toDate}, ${new org.joda.time.DateTime(x.getEndTime.getSeconds, zone).toDate}"))
  }

  // [START dlp_numerical_stats]
  // [END dlp_numerical_stats]
  // [START dlp_categorical_stats]
  // [END dlp_categorical_stats]
  // [START dlp_k_anonymity]
  @throws[Exception]
  private def calculateKAnonymity(projectId: String, datasetId: String, tableId: String)  //, quasiIds: util.List[String], topicId: String, subscriptionId: String)
  : Unit = {
    println("started 1")// Instantiates a client
      val dlpServiceClient = create
      try {

        val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
        val tableRequest: Table = bigquery.getTable(TableId.of(projectId, datasetId, tableId))
        println("started 2")
        val quasiIdsFields: lang.Iterable[FieldId] =
          tableRequest.getDefinition[StandardTableDefinition].getSchema.getFields.asScala
            .filter{columnName => (!notAlowedColumn.contains(columnName.getName)) }
            .map{ columnName => FieldId.newBuilder.setName(columnName.getName).build }.toIterable.asJava
        println(quasiIdsFields)
        val id: EntityId.Builder = EntityId.newBuilder().setField(FieldId.newBuilder.setName("id"))
        val kanonymityConfig: KAnonymityConfig = KAnonymityConfig.newBuilder.addAllQuasiIds(quasiIdsFields).setEntityId(id).setEntityId(id).build
        println("started 3")
        val bigQueryTable = BigQueryTable.newBuilder.setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId).build
//        bigQueryTable.getAllFields
        val privacyMetric = PrivacyMetric.newBuilder.setKAnonymityConfig(kanonymityConfig).build
        println("started 4")
        val topicName: String = String.format("projects/%s/topics/%s", projectId, "anonymity")
        val publishToPubSub = PublishToPubSub.newBuilder.setTopic(topicName).build
        // Create action to publish job status notifications over Google Cloud Pub/Sub
        val action_1 = Action.newBuilder.setPubSub(publishToPubSub).build

        val destinationTable: BigQueryTable = BigQueryTable.newBuilder()
          .setProjectId(projectId)
          .setDatasetId(datasetId)
          .setTableId(tableId + "_kAnonynity")
          .build()

        val outputStorageConfig = OutputStorageConfig.newBuilder().setTable(destinationTable)
        println("started 5")
        val saveToBQAction = Action.SaveFindings.newBuilder().setOutputConfig(outputStorageConfig).build()
        val action_2 = Action.newBuilder().setSaveFindings(saveToBQAction).build()
        println("started 6")
        val riskAnalysisJobConfig = RiskAnalysisJobConfig.newBuilder.setSourceTable(bigQueryTable).setPrivacyMetric(privacyMetric).addActions(action_1).addActions(action_2).build
        val createDlpJobRequest = CreateDlpJobRequest.newBuilder.setParent(ProjectName.of(projectId).toString).setRiskJob(riskAnalysisJobConfig).build
        println("started 7")
        val dlpJob: DlpJob = dlpServiceClient.createDlpJob(createDlpJobRequest)

        val dlpJobName: String = dlpJob.getName
        println(s"started 8 $dlpJobName")
        val done: SettableApiFuture[Boolean] = SettableApiFuture.create[Boolean]
        // Set up a Pub/Sub subscriber to listen on the job completion status
        val subscriber = Subscriber.newBuilder(
          ProjectSubscriptionName.newBuilder.setProject(projectId).setSubscription("anonymity").build,
          (pubsubMessage, ackReplyConsumer) => {
          if (pubsubMessage.getAttributesCount > 0 && pubsubMessage.getAttributesMap.get("DlpJobName").equals(dlpJobName)) { // notify job completion
            done.set(true)
            ackReplyConsumer.ack
          }

        }).build
        subscriber.startAsync
        // Wait for job completion semi-synchronously
        // For long jobs, consider using a truly asynchronous execution model such as Cloud Functions
        try {
          done.get(1, TimeUnit.MINUTES)
          Thread.sleep(500) // Wait for the job to become available

        } catch {
          case e: Exception =>
            println(s"Unable to verify job completion. $e")
        }
//         Retrieve completed job status
        val completedJob = dlpServiceClient.getDlpJob(GetDlpJobRequest.newBuilder.setName(dlpJobName).build)
        println("Job status: " + completedJob.getState)
        println("started 9")
        val riskDetails = completedJob.getRiskDetails
        val kanonymityResult = riskDetails.getKAnonymityResult
        import scala.collection.JavaConverters._
        kanonymityResult.getEquivalenceClassHistogramBucketsList.forEach {
          _.getBucketValuesList.forEach {
            bucket => {
              val quasiIdValues = bucket.getQuasiIdsValuesList.asScala.map{_.toString}.mkString(",")
              println("\tQuasi-ID values: " + quasiIdValues)
              println("\tClass size: " + bucket.getEquivalenceClassSize)
            }
          }
        }
      } catch {
        case e: Exception =>
          println("Error in calculateKAnonymity: " + e)
      } finally if (dlpServiceClient != null) dlpServiceClient.close()
    }


  // [START dlp_numerical_stats]
  // [END dlp_numerical_stats]
  // [START dlp_categorical_stats]
  // [END dlp_categorical_stats]
  // [START dlp_k_anonymity]
  // [END dlp_k_anonymity]
  // [START dlp_l_diversity]
  @throws[Exception]
  private def calculateLDiversity(projectId: String, datasetId: String, tableId: String, sensitiveAttribute: String) //, quasiIds: util.List[String], topicId: String, subscriptionId: String)
  : Unit = { // Instantiates a client
    try {
      val dlpServiceClient = create
      try {
        val sensitiveAttributeField: FieldId = FieldId.newBuilder.setName(sensitiveAttribute).build
        val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
        val tableRequest: Table = bigquery.getTable(TableId.of(projectId, datasetId, tableId))
        val quasiIdFields: lang.Iterable[FieldId] =
          tableRequest.getDefinition[StandardTableDefinition].getSchema.getFields.asScala
            .filter{columnName => (!notAlowedColumn.contains(columnName.getName)) }
            .map{ columnName => FieldId.newBuilder.setName(columnName.getName).build }.toIterable.asJava
        val ldiversityConfig = LDiversityConfig.newBuilder.addAllQuasiIds(quasiIdFields).setSensitiveAttribute(sensitiveAttributeField).build
        val bigQueryTable: BigQueryTable = BigQueryTable.newBuilder.setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId).build
        val privacyMetric = PrivacyMetric.newBuilder.setLDiversityConfig(ldiversityConfig).build
        val topicName: String = String.format("projects/%s/topics/%s", projectId, "anonymity")
        val publishToPubSub = PublishToPubSub.newBuilder.setTopic(topicName).build
        // Create action to publish job status notifications over Google Cloud Pub/Sub
        val action = Action.newBuilder.setPubSub(publishToPubSub).build
        val riskAnalysisJobConfig = RiskAnalysisJobConfig.newBuilder.setSourceTable(bigQueryTable).setPrivacyMetric(privacyMetric).addActions(action).build
        val createDlpJobRequest = CreateDlpJobRequest.newBuilder.setParent(ProjectName.of(projectId).toString).setRiskJob(riskAnalysisJobConfig).build
        val dlpJob = dlpServiceClient.createDlpJob(createDlpJobRequest)
        val dlpJobName: String = dlpJob.getName
        val done: SettableApiFuture[Boolean] = SettableApiFuture.create[Boolean]
        // Set up a Pub/Sub subscriber to listen on the job completion status
        val subscriber = Subscriber.newBuilder(ProjectSubscriptionName.newBuilder.setProject(projectId).setSubscription("anonymity").build,
          (pubsubMessage, ackReplyConsumer) => {
            if (pubsubMessage.getAttributesCount > 0 && pubsubMessage.getAttributesMap.get("DlpJobName").equals(dlpJobName)) { // notify job completion
              done.set(true)
              ackReplyConsumer.ack
            }

          }
        ).build
        subscriber.startAsync
        // Wait for job completion semi-synchronously
        // For long jobs, consider using a truly asynchronous execution model such as Cloud Functions
        try {
          done.get(1, TimeUnit.MINUTES)
          Thread.sleep(500) // Wait for the job to become available

        } catch {
          case e: TimeoutException =>
            println("Unable to verify job completion.")
        }
        // retrieve completed job status
        val completedJob = dlpServiceClient.getDlpJob(GetDlpJobRequest.newBuilder.setName(dlpJobName).build)
        println("Job status: " + completedJob.getState)
        val riskDetails = completedJob.getRiskDetails
        val ldiversityResult = riskDetails.getLDiversityResult
        import scala.collection.JavaConverters._
        ldiversityResult.getSensitiveValueFrequencyHistogramBucketsList.forEach{
          _.getBucketValuesList.forEach{
            bucket => {
              val quasiIdValues = bucket.getQuasiIdsValuesList.asScala.map{_.toString}.mkString(",")
              println("\tQuasi-ID values: " + quasiIdValues)
              println("\tClass size: " + bucket.getEquivalenceClassSize)
              bucket.getTopSensitiveValuesList.forEach{ x =>
                print(s"\t\tSensitive value ${x.getValue.toString} occurs ${x.getCount} time(s).\n")
              }
            }
          }
        }
      } catch {
        case e: Exception =>
          System.out.println("Error in calculateLDiversity: " + e.getMessage)
      } finally if (dlpServiceClient != null) dlpServiceClient.close
    }
  }


  def inspectBigQuery(projectId: String,
                      datasetId: String,
                      tableId: String,
                      maxFindings: Integer,
                      customInfoTypes: List[CustomInfoType],
                      includeQuote: Boolean): Unit = {
    import com.google.privacy.dlp.v2._

    // Look for everything possible, likely, or very likely.
    val minLikelihood = Likelihood.LIKELY

    // Infotypes to inspect, US only.
    // https://cloud.google.com/dlp/docs/infotypes-reference
    val relevantInfoTypes = List(
      "AGE",
      "DATE_OF_BIRTH",
      "EMAIL_ADDRESS",
      "ETHNIC_GROUP",
      "GCP_CREDENTIALS",
      "GENDER",
      "IP_ADDRESS",
      "LOCATION",
      "MAC_ADDRESS",
      "MAC_ADDRESS_LOCAL",
      "PERSON_NAME",
      "PHONE_NUMBER",
      "URL",
      "US_DRIVERS_LICENSE_NUMBER",
      "US_EMPLOYER_IDENTIFICATION_NUMBER",
      "US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER",
      "US_PASSPORT",
      "US_SOCIAL_SECURITY_NUMBER",
      "US_STATE",
      "US_VEHICLE_IDENTIFICATION_NUMBER")

    // Build the list of infoTypes.
    val infoTypes = relevantInfoTypes.map(InfoType.newBuilder().setName(_).build())

    // Instantiate a DLP client
    val dlpServiceClient = DlpServiceClient.create()

    // Reference to the BQ dataset and table to query.
    // However, the specified project ID is just where it runs.
    // So to avoid trampling modular-shard's API limits, use
    // .setProjectId("ck-soc-dev")
    val tableReference = BigQueryTable.newBuilder()
      .setProjectId(projectId)
      .setDatasetId(datasetId)
      .setTableId(tableId)
      .build()
    // Limit to 10 rows to start. In this future, this should be configurable.
    val bigqueryOptions = BigQueryOptions.newBuilder().setTableReference(tableReference).setRowsLimitPercent(100).build()

    // Bigquery configuration to be inspected.
    val storageConfig = StorageConfig.newBuilder().setBigQueryOptions(bigqueryOptions).build()
    // Finding limits do not limit the number of rows queried! It limits the results returned!
    val findingLimits = FindingLimits.newBuilder().setMaxFindingsPerRequest(maxFindings).build()

    val inspectConfig = InspectConfig.newBuilder()
      .addAllInfoTypes(infoTypes.asJava)
      .addAllCustomInfoTypes(customInfoTypes.asJava)
      .setMinLikelihood(minLikelihood)
      .setLimits(findingLimits)
      .setIncludeQuote(includeQuote)
      .build()

    // BQ table to save the results to (Katie's SOC project).
    val destinationTable = BigQueryTable.newBuilder()
      .setProjectId(projectId)
      .setDatasetId(datasetId)
      .setTableId(tableId+"_finds")
      .build()

    val outputStorageConfig = OutputStorageConfig.newBuilder().setTable(destinationTable)
    val saveToBQAction = Action.SaveFindings.newBuilder().setOutputConfig(outputStorageConfig).build()
    val action = Action.newBuilder().setSaveFindings(saveToBQAction).build()

    val inspectJobConfig = InspectJobConfig.newBuilder()
      .setStorageConfig(storageConfig)
      .setInspectConfig(inspectConfig)
      .addActions(action)
      .build()

    // Asynchronously submit an inspect job, and wait on results
    val createDlpJobRequest = CreateDlpJobRequest.newBuilder()
      .setParent(ProjectName.of(projectId).toString)
      .setInspectJob(inspectJobConfig)
      .build()


    val dlpJob = dlpServiceClient.createDlpJob(createDlpJobRequest)

    println("Job created with ID:" + dlpJob.getName)

    val done = SettableApiFuture.create()

    try {
      done.get(1, TimeUnit.MINUTES)
      Thread.sleep(500)
    } catch {
      case _ => println("Unable to verify job completion")
    }

    val completedJob = dlpServiceClient.getDlpJob(
      GetDlpJobRequest.newBuilder().setName(dlpJob.getName()).build()
    )

    println("Job Status: " + completedJob.getState())
    val inspectDataSourceDetails = completedJob.getInspectDetails()
    val result = inspectDataSourceDetails.getResult()

    if (result.getInfoTypeStatsCount() > 0) {
      println("Findings: ")

      result.getInfoTypeStatsList().forEach(infoTypeStat => {
        print("\tInfo type: " + infoTypeStat.getInfoType().getName())
        println("\tCount: " + infoTypeStat.getCount())
      })
    } else {
      println("No findings.")
    }
  }
}
