package com.clara.clowncar.processor

import com.clara.kafka.consumer.FilteringProcessor
import com.expeditelabs.db.repositories.DatabaseTables
import com.expeditelabs.thrift.{events, scala => thrift}
import com.expeditelabs.util.PlaceholderEmail
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future

object SkipPlaceholderAccountsFilteringProcessor {
  object HasAUserId {
    def unapply(updateEntityRequest: thrift.UpdateEntityRequest): Option[Int] =
      updateEntityRequest.entity match {
        case thrift.Entity.Lead(lead) =>
          lead.invitedUserId

        case thrift.Entity.User(user) =>
          user.id

        case thrift.Entity.UserPreference(userPref) =>
          Some(userPref.userId)

        case _ =>
          None
      }
  }

  def forProcessor(
    underlying: FilteringProcessor,
    databaseTables: DatabaseTables,
    statsReceiver: StatsReceiver
  ): FilteringProcessor =
    new SkipPlaceholderAccountsFilteringProcessor(underlying, databaseTables, statsReceiver)
}

/**
  * A [[com.clara.kafka.consumer.FilteringProcessor]] implementation meant to wrap an underlying
  * [[com.clara.kafka.consumer.FilteringProcessor]] to short circuit any processing of updates that
  * are meant for placeholder accounts(as defined by [[com.expeditelabs.util.PlaceholderEmail]]).
  * This is due to the fact that the platform doesn't create leads in salesforce until after a user
  * sets their email. Until then, they will have a placeholder email and should not be looked up in
  * SFDC.
  *
  * @param underlying FilteringProcessor to call if we deem it necessary
  * @param databaseTables
  * @param statsReceiver
  */
class SkipPlaceholderAccountsFilteringProcessor(
  underlying: FilteringProcessor,
  databaseTables: DatabaseTables,
  statsReceiver: StatsReceiver
) extends FilteringProcessor {
  import SkipPlaceholderAccountsFilteringProcessor._

  protected[this] val logger = Logger("SkipPlaceholderAccountsFilteringProcessor")

  protected[this] val scopedStats =
    statsReceiver.scope("skip_placeholder_accounts_filtering_processor")

  private[this] val requestCounter = scopedStats.counter("requests")
  private[this] val handledCounter = scopedStats.counter("handled")

  override def isDefinedAt(message: events.EventMessage): Future[Boolean] = {
    requestCounter.incr()

    message.payload match {
      case Some(events.LogData.UpdateEntityRequest(HasAUserId(userId))) =>
        handledCounter.incr()
        val isPlaceholderFuture = databaseTables.users.get(userId).map {
          case Some(user) =>
            PlaceholderEmail.isPlaceholderEmail(user.email)

          case None =>
            false
        }

        isPlaceholderFuture.flatMap { isPlaceholder =>
          if (isPlaceholder) {
            Future.False
          } else {
            underlying.isDefinedAt(message)
          }
        }

      case _ =>
        Future.False
    }
  }

  override def apply(messages: Seq[events.EventMessage]): Future[Unit] = underlying(messages)
}
