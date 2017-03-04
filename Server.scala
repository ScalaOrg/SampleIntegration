package com.clara.clowncar

import java.util.concurrent.Executors

import com.clara.clowncar.processor._
import com.clara.kafka.KafkaTopics
import com.clara.kafka.consumer.PlatformEventListener
import com.expeditelabs.analytics.S3AccessFactory
import com.expeditelabs.analytics.services.S3BackedLoanArchiveLibrary
import com.expeditelabs.db.repositories.DatabaseTables
import com.expeditelabs.salesforce.PollingSalesforceTester
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.util.config.LoggingConfig
import com.expeditelabs.util.environment.ExpediteEnvironment
import com.expeditelabs.util.services.ExpediteServer
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.logging.{Level, LoggerFactory}
import com.twitter.util._

object ClownCarServer extends ExpediteServer  {

  private[this] val kafkaServers =
    flag(
      "kafka.servers",
      "localhost:9092",
      "comma separated list of kafka server:ports - kafka configuration value bootstrap.servers"
    )

  def main(): Unit = {
    log.info("VROOM VROOM...")
    val s3Access = S3AccessFactory(env, statsReceiver)
    val (logBucket, sfUserName, sfPassword) = getDynamoConfigVals()

    val archiveLibrary   = new S3BackedLoanArchiveLibrary(logBucket, s3Access)
    val salesforceClient = resolveSalesforceClient(env, sfUserName, sfPassword)
    val pollingSalesforceTester =
      new PollingSalesforceTester(sfUserName, salesforceClient, statsReceiver)

    val dbTables = DatabaseTables(databaseUrl, statsReceiver)

    val platformListener = PlatformEventListener.withProcessors(
      kafkaServerName  = kafkaServers(),
      kafkaGroupIdName = "clownCarListener",
      processors       = makeProcessors(dbTables, archiveLibrary, salesforceClient),
      topics           = Seq(KafkaTopics.platformEvents),
      statsReceiver    = statsReceiver
    )

    val executor       = Executors.newSingleThreadExecutor()
    val futurePool     = FuturePool(executor)
    val listenerFuture = futurePool(platformListener.run())

    onExit {
      log.info("onExit - the car's done!")
      Await.ready(salesforceClient.close())
      Await.ready(platformListener.close())
      Await.ready(dbTables.close())
      executor.shutdown()
    }

    pollingSalesforceTester.start()
    Await.ready(listenerFuture)
    ()
  }

  private[this] def makeProcessors(
    dbTables: DatabaseTables,
    archiveLibrary: S3BackedLoanArchiveLibrary,
    salesforceClient: SalesforceClient
  ) = {
    val loanSnapshotProcessor =
      SalesforceLoanSnapshotProcessor.asynchronously(salesforceClient, archiveLibrary,statsReceiver)

    val filteredLoanSpecialistProcessor =
      SkipPlaceholderAccountsFilteringProcessor.forProcessor(
        new UpdateLoanSpecialistProcessor(salesforceClient, dbTables, statsReceiver),
        dbTables,
        statsReceiver
      )

    val updateLeadOnInitializationProcessor =
      new UpdateLeadOnInitializationProcessor(salesforceClient, dbTables, statsReceiver)

    val updateLeadContactPrefsProcessor =
      new UpdateLeadContactPreferencesProcessor(salesforceClient, dbTables, statsReceiver)

    val updateLeadInPortalStatusProcessor =
      UpdateLeadInPortalStatusProcessor(salesforceClient,statsReceiver)

    val updateLeadCreditPulledStatusProcessor =
      UpdateLeadCreditPulledStatusProcessor(salesforceClient, statsReceiver)

    Seq(
      loanSnapshotProcessor,
      filteredLoanSpecialistProcessor,
      updateLeadOnInitializationProcessor,
      updateLeadContactPrefsProcessor,
      updateLeadInPortalStatusProcessor,
      updateLeadCreditPulledStatusProcessor
    )
  }

  private[this] def getDynamoConfigVals(): (String, String, String) = {
    (
      dynamoConfig.get[String]("LOG_DATA_BUCKET"),
      dynamoConfig.get[String]("SALESFORCE_USERNAME"),
      dynamoConfig.get[String]("SALESFORCE_PASSWORD")
    )
  }

  private[this] def resolveSalesforceClient(
    environment: ExpediteEnvironment.Value,
    sfUserName: String,
    sfPassword: String
  ): SalesforceClient = {
    val client = SalesforceClient(sfUserName, sfPassword, environment, statsReceiver)
    client.login()
    client
  }

  override def loggerFactories: List[LoggerFactory] = {
    val baseConfig = LoggingConfig(
      environment = env,
      baseLogFile = "clown-car.log",
      maxLogSize  = 100.megabytes,
      nodes       = Nil
    )

    val config = env match {
      case ExpediteEnvironment.Development =>
        baseConfig.copy(level = Level.DEBUG)

      case _ =>
        baseConfig.copy(
          logDir    = "/var/log/clara/clown-car"
        )
    }

    config.factories
  }
}
