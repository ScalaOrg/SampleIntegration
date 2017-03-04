package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.salesforce.client.SalesforceClient
import com.expeditelabs.thrift.events
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

/**
  * UpdateLeadStatusProcessor
  *
  * This FilteringProcessor listens to the event stream for a specified event
  * and sets a lead's status to a specified value in salesforce in response.
  *
  * @param salesforceClient
  * @param statsReceiver
  */
abstract class UpdateLeadStatusProcessor(
  statusToSet: String,
  messageTopic: String,
  messageEvent: String,
  salesforceClient: SalesforceClient,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {

  val className        = getClass.getSimpleName
  val snakeClassName   = toSnakeCase(className)

  protected[this] val logger                 = Logger(className)
  protected[this] val scopedStats            = statsReceiver.scope("update_lead_status_processor").scope(snakeClassName)
  private[this] val salesforceScope          = scopedStats.scope("salesforce")
  private[this] val updatedActivationCounter = salesforceScope.counter("updated_lead")
  private[this] val leadNotFoundCounter      = salesforceScope.counter("lead_not_found")

  override def isDefinedAt(message: events.EventMessage): Future[Boolean] =
    Future.value(extractEvents(Seq(message)).nonEmpty)

  override def apply(messages: Seq[events.EventMessage]): Future[Unit] = {
    val relevantEvents = extractEvents(messages)
    val loanIds = relevantEvents.flatMap(_.transactionContext.flatMap(_.loanId))

    Future.collect(loanIds.map(updateLeadStatus)).unit
  }

  private[this] def updateLeadStatus(loanId: Int): Future[Unit] = {
    salesforceClient.getLeadByLoanId(loanId).flatMap {
      case Some(sfLead) =>
        updatedActivationCounter.incr()
        sfLead.setStatus(statusToSet)
        salesforceClient.updateLead(sfLead).unit

      case None =>
        leadNotFoundCounter.incr()
        Future.Done
    }
  }

  private[this] def extractEvents(messages: Seq[events.EventMessage]): Seq[events.EventMessage] = {
    messages.filter { message =>
      message.topic == messageTopic && message.event == messageEvent
    }
  }

  // from https://gist.github.com/sidharthkuruvila/3154845#gistcomment-1779018
  private[this] def toSnakeCase(name: String) = "[A-Z\\d]".r.replaceAllIn(name, {m =>
    if (m.end(0) == 1){
      m.group(0).toLowerCase()
    } else {
      "_" + m.group(0).toLowerCase()
    }
  })
}
