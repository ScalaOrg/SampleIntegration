package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.logs.labels
import com.expeditelabs.salesforce.client.SalesforceClient
import com.twitter.finagle.stats.StatsReceiver

/**
  * UpdateLeadInPortalStatusProcessor
  *
  * This FilteringProcessor listens to the event stream for user activated events and sets
  * their status to `In Portal` in salesforce.
  */
object UpdateLeadInPortalStatusProcessor {
  val InPortalStatus = "In Portal"

  def apply(
    salesforceClient: SalesforceClient,
    statsReceiver: StatsReceiver
  ): UpdateLeadInPortalStatusProcessor =
    new UpdateLeadInPortalStatusProcessor(
      statusToSet       = InPortalStatus,
      messageTopic      = labels.Topics.borrowerPortal,
      messageEvent      = labels.Events.userActivated,
      salesforceClient  = salesforceClient,
      statsReceiver     = statsReceiver
  )
}

class UpdateLeadInPortalStatusProcessor(
  statusToSet: String,
  messageTopic: String,
  messageEvent: String,
  salesforceClient: SalesforceClient,
  statsReceiver: StatsReceiver
) extends UpdateLeadStatusProcessor(statusToSet, messageTopic, messageEvent, salesforceClient, statsReceiver)
