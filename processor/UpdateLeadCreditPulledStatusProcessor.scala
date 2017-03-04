package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.logs.labels
import com.expeditelabs.salesforce.client.SalesforceClient
import com.twitter.finagle.stats.StatsReceiver

/**
  * UpdateLeadCreditPulledStatusProcessor
  *
  * This FilteringProcessor listens to the event stream for credit pull events and sets
  * their status to `Credit Pulled` in salesforce.
  */
object UpdateLeadCreditPulledStatusProcessor {
  val CreditPulledStatus = "Credit Pulled"

  def apply(
    salesforceClient: SalesforceClient,
    statsReceiver: StatsReceiver
  ): UpdateLeadCreditPulledStatusProcessor =
    new UpdateLeadCreditPulledStatusProcessor(
      statusToSet       = CreditPulledStatus,
      messageTopic      = labels.Topics.platformThrift,
      messageEvent      = labels.Events.mrRobotoCreditReportReceived,
      salesforceClient  = salesforceClient,
      statsReceiver     = statsReceiver
    )
}

class UpdateLeadCreditPulledStatusProcessor(
  statusToSet: String,
  messageTopic: String,
  messageEvent: String,
  salesforceClient: SalesforceClient,
  statsReceiver: StatsReceiver
) extends UpdateLeadStatusProcessor(statusToSet, messageTopic, messageEvent, salesforceClient, statsReceiver)
