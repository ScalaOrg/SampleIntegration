package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.db.repositories.DatabaseTables
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.thrift.{events, scala => thrift}
import com.sforce.soap.enterprise.sobject
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

/**
  * UpdateLoanSpecialistProcessor
  *
  * This FilteringProcessor listens to the event stream for updateEntity events that target the
  * borrower's leads. When one is detected, it syncs the owner in Salesforce with the borrower's
  * assigned loan specialist in the portal. Also listen to user initialization events to make sure
  * that the lead's owner is synchronized on lead creation.
  *
  * One use case of this FilteringProcessor is to handle the case in the portal impersonation
  * mode where the borrower's loan specialist is changed to a new loan specialist. That emits an event
  * which will be processed here, which will update the lead or opportunity's owner in Salesforce.
  *
  * Another use case is to set the Salesforce lead's owner when it is created. Moreoever, the counters
  * in this class will give us the ability to understand how frequently lead creation fails from velocify
  * by checking how often lead is missing from salesforce, which has been a problem historically.
  *
  * @param salesforceClient - a client for talking to salesforce
  * @param databaseTables - a DatabaseTables object to read the underlying info from
  * @param statsReceiver
  */
class UpdateLoanSpecialistProcessor(
  salesforceClient: SalesforceClient,
  databaseTables: DatabaseTables,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {
  protected[this] val scopedStats     = statsReceiver.scope("update_loan_specialist_processor")
  protected[this] val logger          = Logger("UpdateLoanSpecialistProcessor")

  private[this] val leadScopedStats      = scopedStats.scope("lead")
  private[this] val missingClaraLead     = leadScopedStats.counter("missing")
  private[this] val malformedClaraLead   = leadScopedStats.counter("malformed")

  private[this] val salesforceScopedStats         = scopedStats.scope("salesforce")
  private[this] val updateLeadCounter             = salesforceScopedStats.scope("lead").counter("requests")
  private[this] val updateLeadSuccCounter         = salesforceScopedStats.scope("lead").counter("success")
  private[this] val updateOpportunityCounter      = salesforceScopedStats.scope("opportunity").counter("requests")
  private[this] val updateOpportunitySuccCounter  = salesforceScopedStats.scope("opportunity").counter("success")
  private[this] val salesforceLONotFoundCounter   = salesforceScopedStats.scope("lo").counter("not_found")
  private[this] val salesforceLeadNotFoundCounter = salesforceScopedStats.scope("lead").counter("not_found")
  private[this] val salesforceOppNotFoundCounter  = salesforceScopedStats.scope("opp").counter("not_found")

  override def isDefinedAt(message: events.EventMessage): Future[Boolean] =
    Future.value(extractLeadUpdates(Seq(message)).nonEmpty)

  override def apply(messages: Seq[events.EventMessage]): Future[Unit] = {
    val leadIds = extractLeadUpdates(messages)

    Future.collect(leadIds.map(updateLeadOwner)).unit
  }

  private[this] def extractLeadUpdates(messages: Seq[events.EventMessage]): Seq[Int] = {
    messages.flatMap { message =>
      message.payload.flatMap {
        // Event that is emitted when a lead's owner is switched in impersonation mode
        case events.LogData.UpdateEntityRequest(updateReq) =>
          updateReq.entity match {
            case thrift.Entity.Lead(lead) if lead.createdByUserId.isDefined =>
              lead.id
            case _ =>
              None
          }

        // Event that is emitted when lead is created for the first time. At this point
        // the lead's owner in Salesforce is set to the default and needs to be updated.
        case events.LogData.UserInitialization(userInit) =>
          Some(userInit.leadId)

        case _ => None
      }
    }
  }

  private[this] def updateLeadOwner(leadId: Int): Future[Unit] = {
    getBorrowerAndLoanSpecialist(leadId).flatMap {
      case (Some(loanId), Some(loanSpecialistEmail)) =>
        updateSalesforce(loanId, loanSpecialistEmail)

      case _ =>
        logger.warning(s"Malformed details for lead: $leadId")
        Future.Done
    }
  }

  private[this] def getBorrowerAndLoanSpecialist(leadId: Int): Future[(Option[Int], Option[String])] = {
    databaseTables.leads.get(leadId).flatMap {
      case Some(lead) =>
        (lead.loanId, lead.createdByUserId) match {
          case (loanIdOpt @ Some(loanId), Some(loanSpecialistId)) =>
            databaseTables.users.get(loanSpecialistId).map { loanSpecialistOpt =>
              (loanIdOpt, loanSpecialistOpt.map(_.email))
            }

          case _ =>
            malformedClaraLead.incr()
            Future.value((None, None))
        }

      case None =>
        missingClaraLead.incr()
        Future.value((None, None))
    }
  }

  private[this] def updateSalesforce(loanId: Int, loanSpecialistEmail: String): Future[Unit] = {
    salesforceClient.getUserByEmail(loanSpecialistEmail).flatMap {
      case Some(loanSpecialist) =>
        Future.join(
          updateSalesforceLead(loanId, loanSpecialist),
          updateSalesforceOpportunity(loanId, loanSpecialist)
        ).unit

      case None =>
        salesforceLONotFoundCounter.incr()
        logger.warning(
          s"Loan Specialist $loanSpecialistEmail not found in Salesforce when updating Loan $loanId"
        )
        Future.Done
    }
  }

  private[this] def updateSalesforceLead(loanId: Int, loanSpecialist: sobject.User): Future[Unit] = {
    salesforceClient.getLeadByLoanId(loanId).flatMap {
      case Some(lead) =>
        updateLeadCounter.incr()
        lead.setOwnerId(loanSpecialist.getId)
        salesforceClient.updateLead(lead).unit.onSuccess(_ => updateLeadSuccCounter.incr())

      case None =>
        salesforceLeadNotFoundCounter.incr()
        logger.warning(
          s"Lead for loan $loanId not found in SFDC when associating loan specialist ID ${loanSpecialist.getId}"
        )
        Future.Done
    }
  }

  // N.B. This may not always be successful since a given Lead object may not have been converted to an
  // Opportunity in SFDC yet.
  private[this] def updateSalesforceOpportunity(loanId: Int, loanSpecialist: sobject.User): Future[Unit] = {
    salesforceClient.getOpportunityByLoanId(loanId).flatMap {
      case Some(opportunity) =>
        updateOpportunityCounter.incr()
        opportunity.setOwnerId(loanSpecialist.getId)
        salesforceClient.updateOpportunity(opportunity).unit.onSuccess(_ => updateOpportunitySuccCounter.incr())

      case None =>
        salesforceOppNotFoundCounter.incr()
        logger.info(
          s"Opportunity for loan $loanId not found in SFDC when associating loan specialist ID ${loanSpecialist.getId}"
        )
        Future.Done
    }
  }
}
