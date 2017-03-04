package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.db.{models => db}
import com.expeditelabs.db.repositories.DatabaseTables
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.thrift.{scala => thrift, events => logThrift, update => updateThrift}
import com.sforce.soap.enterprise.sobject.Lead
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

object UpdateLeadContactPreferencesProcessor {
  protected[processor] def extractUserIdAndTransactionContext(
    message: logThrift.EventMessage
  ): Option[(Int, thrift.TransactionContext)] = {
    message.payload.flatMap {
      // the externalSource field defines whether this write happened from salesforce or internally
      case logThrift.LogData.UpdateAttributesRequest(updateReq) if updateReq.externalSource.isEmpty =>
        updateReq.attributes match {
          case updateThrift.Attributes.UserPreferenceAttributes(userPrefUpdateThrift) =>
            Some((userPrefUpdateThrift.userId, updateReq.transactionContext))

          case _ => None
        }

      // Event that is emitted when lead is created for the first time. At this point
      // the lead's owner in Salesforce is set to the default and needs to be updated.
      case logThrift.LogData.UserInitialization(userInit) =>
        Some((userInit.userId, thrift.TransactionContext(Some(userInit.loanId))))

      case _ => None
    }
  }

  object Salesforce {
    val WorkingWithOtherLender = "Other Lender"
    val Nurture                = "Nurture"
  }
}

/**
  * UpdateLeadContactPreferencesProcessor
  *
  * This FilteringProcessor listens to the event stream for UpdateEntityRequest that are generated
  * by internal systems and updates leads in salesforce based on their UserPreferences.
  *
  * @param salesforceClient
  * @param databaseTables
  * @param statsReceiver
  */
class UpdateLeadContactPreferencesProcessor(
  salesforceClient: SalesforceClient,
  databaseTables: DatabaseTables,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {
  import UpdateLeadContactPreferencesProcessor._

  protected[this] val scopedStats = statsReceiver.scope("update_lead_contact_prefs_processor")
  protected[this] val logger      = Logger("UpdateLeadContactPreferencesProcessor")

  private[this] val loanScopedStats       = scopedStats.scope("user")
  private[this] val loanNotDefinedCounter = loanScopedStats.counter("missing")

  private[this] val salesforceScopedStats         = scopedStats.scope("salesforce")
  private[this] val updateSalesforceCounter       = salesforceScopedStats.counter("requests")
  private[this] val updateSalesforceSuccCounter   = salesforceScopedStats.counter("success")
  private[this] val salesforceLeadNotFoundCounter =
    salesforceScopedStats.scope("lead").counter("missing")

  override def isDefinedAt(message: logThrift.EventMessage): Future[Boolean] =
    Future.value(extractUserIdAndTransactionContext(message).nonEmpty)

  override def apply(messages: Seq[logThrift.EventMessage]): Future[Unit] = {
    Future.collect {
      messages.map { message =>
        extractUserIdAndTransactionContext(message).map { case (userId, txnContext) =>
          getSalesforceLead(txnContext).flatMap {
            case Some(sfLead) => updateLeadInSalesForce(sfLead, userId)
            case None         => Future.Done
          }
        }.getOrElse(Future.Done)
      }
    }.unit
  }

  private[this] def getSalesforceLead(txnContext: thrift.TransactionContext): Future[Option[Lead]] =
    txnContext.loanId match {
      case Some(loanId) =>
        salesforceClient.getLeadByLoanId(loanId).flatMap {
          case someSfLead @ Some(_) =>
            Future.value(someSfLead)

          case None =>
            salesforceLeadNotFoundCounter.incr()
            logger.warning(s"Lead with loanId: $loanId not found in Salesforce")
            Future.None
        }

      case _ =>
        loanNotDefinedCounter.incr()
        Future.None
    }

  private[this] def updateLeadInSalesForce(
    sfLead: Lead,
    userId: Int
  ): Future[Unit] = {
    databaseTables.userPreferences.getForUserId(userId).flatMap { userPref =>
      updateSalesforceCounter.incr()

      updateDncStatus(sfLead, userPref)
      updateDisqualifiedReason(sfLead, userPref)

      salesforceClient.updateLead(sfLead).onSuccess { _ =>
        updateSalesforceSuccCounter.incr()
      }.unit
    }
  }

  private[this] def updateDncStatus(lead: Lead, userPref: db.UserPreference): Unit = {
    val prefStrings = Seq(
      userPref.doNotCall.collect       { case true => "Do Not Call" },
      userPref.doNotEmail.collect      { case true => "Do Not Email" },
      userPref.doNotText.collect       { case true => "Do Not Text" },
      userPref.doNotTouch.collect      { case true => "Do Not Touch" },
      userPref.wrongEmail.collect      { case true => "Wrong Email" },
      userPref.wrongNumber.collect     { case true => "Wrong Number" },
      userPref.noConsentToCall.collect { case true => "No Consent To Call" },
      userPref.noConsentToText.collect { case true => "No Consent To Text" }
    ).flatten

    lead.setDNC_Status__c(
      if (prefStrings.isEmpty) "None" else prefStrings.mkString(";")
    )
  }

  private[this] def updateDisqualifiedReason(lead: Lead, userPref: db.UserPreference): Unit =
    userPref.disqualifiedReason.foreach {
      case thrift.DisqualifiedReason.OtherLender =>
        lead.setDisqualified_Status__c(Salesforce.WorkingWithOtherLender)
        lead.setStatus(Salesforce.Nurture)

      case _ =>
        Unit
    }
}
