package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.analytics.services.LoanArchiveLibrary
import com.expeditelabs.logs.labels.{Events, Topics}
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.thrift.events
import com.expeditelabs.util.AsynchronousWorkQueueFactory
import com.expeditelabs.util.rollbar.NullExceptionNotifier
import com.sforce.soap.enterprise.sobject
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

object SalesforceLoanSnapshotProcessor {
  def asynchronously(
    salesforceClient: SalesforceClient,
    archiveLibrary: LoanArchiveLibrary,
    statsReceiver: StatsReceiver
  ): FilteringProcessor = {
    val workQueue = AsynchronousWorkQueueFactory(statsReceiver, NullExceptionNotifier)

    new SalesforceLoanSnapshotProcessor(
      salesforceClient,
      archiveLibrary,
      statsReceiver
    )
  }

  protected def extractNotifications(
    messages: Seq[events.EventMessage]
  ): Seq[events.LoanArchiveNotification] =
    messages.flatMap { message =>
      message.payload match {
        case Some(events.LogData.LoanArchiveNotification(loanArchive)) =>
          Some(loanArchive)

        case _ =>
          None
      }
    }
}

/**
  * Takes a sequence of [[com.expeditelabs.thrift.events.EventMessage]]s, and pulls out
  * [[com.expeditelabs.thrift.events.LoanArchiveNotification]] payloads with which to query an
  * archiveLibrary for [[com.expeditelabs.thrift.events.LendingQbSnapshot]]s. These snapshots
  * the loan status that we want to update salesforce opportunities with.
  */
class SalesforceLoanSnapshotProcessor(
  salesforceClient: SalesforceClient,
  archiveLibrary: LoanArchiveLibrary,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {
  import SalesforceLoanSnapshotProcessor._

  protected[this] val scopedStats = statsReceiver.scope("loan_snapshot_processor")
  protected[this] val logger      = Logger("LoanSnapshotProcessor")

  private[this] val requestsCounter            = scopedStats.counter("notifications")
  private[this] val opportunityFoundCounter    = scopedStats.counter("opportunity_found")
  private[this] val opportunityNotFoundCounter = scopedStats.counter("opportunity_not_found")

  override def isDefinedAt(message: events.EventMessage): Future[Boolean] = {
    Future.value(message.event == Events.lendingQbBackup && message.topic == Topics.lendingQbBackup)
  }

  override def apply(messages: Seq[events.EventMessage]): Future[Unit] = {
    val notifications = extractNotifications(messages)
    requestsCounter.incr(notifications.size)

    Future.collect(
      notifications.map { notification =>
        fetchSnapshotAndMaybeOpportunity(notification).flatMap { case (lqbSnapshot, opportunityOpt) =>
          resolveOpportunityUpdate(lqbSnapshot, opportunityOpt) match {
            case Some(updatedOpportunity) =>
              opportunityFoundCounter.incr()
              salesforceClient.updateOpportunity(updatedOpportunity).unit

            case None =>
              opportunityNotFoundCounter.incr()
              Future.Done
          }
        }
      }
    ).unit
  }

  private[this] def fetchSnapshotAndMaybeOpportunity(
    notification: events.LoanArchiveNotification
  ): Future[(events.LendingQbSnapshot, Option[sobject.Opportunity])] =
    for {
      lqbSnapshot   <- archiveLibrary.retrieve(notification)
      sfOpportunity <- salesforceClient.getOpportunityByLoanId(lqbSnapshot.loanId.toInt)
    } yield {
      (lqbSnapshot, sfOpportunity)
    }

  // TODO: Update opportunity with more fields based on snapshot
  private[this] def resolveOpportunityUpdate(
    lqbSnapshot: events.LendingQbSnapshot,
    opportunityOpt: Option[sobject.Opportunity]
  ): Option[sobject.Opportunity] =
    opportunityOpt.map { opportunityToUpdate =>
      opportunityToUpdate.setStageName(lqbSnapshot.currentLoanStatus.name)

      opportunityToUpdate
    }
}
