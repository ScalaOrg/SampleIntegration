package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.db.models.BorrowerLoanDatum
import com.expeditelabs.db.repositories.DatabaseTables
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.thrift.{events, scala => thrift}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

/**
  * Updates the Lead object in Salesforce when a [[com.expeditelabs.thrift.events.UserInitialization]]
  * event is generated.
  */
class UpdateLeadOnInitializationProcessor(
  salesforceClient: SalesforceClient,
  databaseTables: DatabaseTables,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {
  protected[this] val scopedStats = statsReceiver.scope("update_lead_activation_token_processor")
  protected[this] val logger      = Logger("UpdateLeadOnInitializationProcessor")

  private[this] val salesforceScopedStats       = scopedStats.scope("salesforce")
  private[this] val updateSalesforceReqCounter  = salesforceScopedStats.counter("requests")
  private[this] val updateSalesforceSuccCounter = salesforceScopedStats.counter("success")
  private[this] val leadNotFoundCounter         = salesforceScopedStats.counter("not_found")

  private[this] val userScopedStats             = scopedStats.scope("user")
  private[this] val emptyActivationTokenCounter = userScopedStats.counter("empty_token")
  private[this] val userNotFoundCounter         = userScopedStats.counter("not_found")

  override def isDefinedAt(message: events.EventMessage): Future[Boolean] =
    Future.value(extractEvents(Seq(message)).nonEmpty)

  override def apply(messages: Seq[events.EventMessage]): Future[Unit] = {
    val userInitEvents = extractEvents(messages)

    Future.collect(userInitEvents.map(updateLead)).unit
  }

  /**
    * Currently updates activation token and refinance goal.
    */
  private[this] def updateLead(userInitEvent: events.UserInitialization): Future[Unit] = {
    Future.join(
      salesforceClient.getLeadByLoanId(userInitEvent.loanId),
      databaseTables.users.get(userInitEvent.userId),
      databaseTables.borrowerLoanData.getForLoan(userInitEvent.loanId)
    ).flatMap {
      case (Some(sfLead), Some(user), bldOpt) if user.activationToken.isDefined =>
        updateSalesforceReqCounter.incr()
        sfLead.setActivation_Token__c(user.activationToken.get)
        sfLead.setRefinance_Goal__c(getRefinanceString(bldOpt))

        salesforceClient.updateLead(sfLead).unit.onSuccess(_ => updateSalesforceSuccCounter.incr())

      case (_, Some(user), _) if user.activationToken.isEmpty =>
        emptyActivationTokenCounter.incr()
        logger.warning(s"No activation token defined for user: ${user}")
        Future.Done

      case (_, None, _) =>
        userNotFoundCounter.incr()
        logger.warning(s"No user found for id: ${userInitEvent.userId}")
        Future.Done

      case (None, _, _) =>
        leadNotFoundCounter.incr()
        logger.warning(s"No lead found for loan id: ${userInitEvent.loanId}")
        Future.Done
    }
  }

  private[this] def getRefinanceString(bldOpt: Option[BorrowerLoanDatum]): String = {
    bldOpt.flatMap(_.refinanceGoal).map(_.toString).getOrElse("")
  }

  private[this] def extractEvents(messages: Seq[events.EventMessage]): Seq[events.UserInitialization] = {
    messages.flatMap { message =>
      message.payload.flatMap {
        // Event that is emitted when lead is created for the first time. At this point
        // the lead's owner in Salesforce is set to the default and needs to be updated.
        case events.LogData.UserInitialization(userInit) =>
          Some(userInit)

        case _ => None
      }
    }
  }
}
