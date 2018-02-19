package traindelays.networkrail.subscribers

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.{DateTimeFormatter, FormatStyle}

import cats.Functor
import cats.data.OptionT
import cats.effect.IO
import cats.instances.list._
import cats.syntax.traverse._
import traindelays.networkrail.{ServiceCode, StanoxCode}
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, StanoxTable, SubscriberTable}
import traindelays.networkrail.movementdata.{CancellationLog, DBLog, MovementLog, VariationStatus}
import traindelays.networkrail.scheduledata.{ScheduleTrainId, StanoxRecord}
import traindelays.networkrail.subscribers.Emailer.Email
import cats.instances.list._
import cats.syntax.traverse._
import com.typesafe.scalalogging.StrictLogging

trait SubscriberHandler {

  def movementNotifier: fs2.Sink[IO, MovementLog]

  def cancellationNotifier: fs2.Sink[IO, CancellationLog]
}

object SubscriberHandler extends StrictLogging {

  private val dateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
  private val timeZone          = ZoneId.of("Europe/London")

  def apply(movementLogTable: MovementLogTable,
            subscriberTable: SubscriberTable,
            scheduleTable: ScheduleTable,
            stanoxTable: StanoxTable,
            emailer: Emailer) =
    new SubscriberHandler {

      override def movementNotifier: fs2.Sink[IO, MovementLog] = fs2.Sink[IO, MovementLog] { log =>
        //TODO only notify in particular circumstances (e.g. late)

        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
          affected           <- filterSubscribersOnStanoxRange(subscribersOnRoute, log.stanoxCode, scheduleTable)
          _                  <- if (affected.nonEmpty) createEmailAction(log, affected) else IO.unit
        } yield ()
      }

      override def cancellationNotifier: fs2.Sink[IO, CancellationLog] = fs2.Sink[IO, CancellationLog] { log =>
        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
          //For cancellation on a route all subscribers are notified
          //TODO is this assumption correct
          _ <- if (subscribersOnRoute.nonEmpty) createEmailAction(log, subscribersOnRoute) else IO.unit
        } yield ()
      }

      private def filterSubscribersOnStanoxRange(subscribersOnRoute: List[SubscriberRecord],
                                                 affectedStanoxCode: StanoxCode,
                                                 scheduleTable: ScheduleTable): IO[List[SubscriberRecord]] =
        subscribersOnRoute
          .map { subscriber =>
            scheduleTable
              .retrieveScheduleLogRecordsFor(subscriber.fromStanoxCode,
                                             subscriber.scheduleTrainId,
                                             subscriber.serviceCode)
              .map(s =>
                subscriber -> s.exists(scheduleLog => {
                  val toStanoxCodeIdx       = scheduleLog.subsequentStanoxCodes.indexOf(subscriber.toStanoxCode)
                  val affectedStanoxCodeIdx = scheduleLog.subsequentStanoxCodes.indexOf(affectedStanoxCode)
                  if (subscriber.fromStanoxCode == affectedStanoxCode) true
                  else if (toStanoxCodeIdx == -1 || affectedStanoxCodeIdx == -1) false
                  else if (affectedStanoxCodeIdx > toStanoxCodeIdx) false
                  else true
                }))
          }
          .sequence[IO, (SubscriberRecord, Boolean)]
          .map(_.collect { case (subscriber, true) => subscriber })

      def createEmailAction(log: DBLog, affectedSubscribers: List[SubscriberRecord]): IO[Unit] = {
        import cats.implicits._
        for {
          _                    <- IO(logger.info(s"Creating email for $log and subscribers $affectedSubscribers"))
          originatingStanoxOpt <- stanoxTable.stanoxRecordFor(log.originStanoxCode)
          affectedStanoxOpt    <- stanoxTable.stanoxRecordFor(log.stanoxCode)
          _                    <- IO(logger.info(s"Originating stanox $originatingStanoxOpt and affected stanox $affectedStanoxOpt"))
          _ <- affectedSubscribers
            .traverse { subscriber =>
              val emailAction = for {
                originatingStanox <- originatingStanoxOpt
                affectedStanox    <- affectedStanoxOpt
              } yield {
                log match {
                  case l: MovementLog =>
                    emailSubscriberWithMovementUpdate(subscriber, l, originatingStanox, affectedStanox, emailer)
                  case l: CancellationLog =>
                    emailSubscriberWithCancellationUpdate(subscriber, l, originatingStanox, affectedStanox, emailer)
                }
              }
              logger.info(s"Email action created: ${emailAction.isDefined}")
              emailAction.getOrElse(IO.unit)
            }
        } yield ()
      }

      //TODO set proper notifcations
      private def emailSubscriberWithMovementUpdate(subscriberRecord: SubscriberRecord,
                                                    movementLog: MovementLog,
                                                    stanoxOriginated: StanoxRecord,
                                                    stanoxAffected: StanoxRecord,
                                                    emailer: Emailer): IO[Unit] = {

        val headline = "Train Delay Helper: Delay Update"
        val email = Email(
          headline,
          EmailTemplates.movementEmailTemplate(headline,
                                               movementLogToBody(movementLog, stanoxOriginated, stanoxAffected)),
          subscriberRecord.emailAddress
        )
        emailer.sendEmail(email)
      }

      private def emailSubscriberWithCancellationUpdate(subscriberRecord: SubscriberRecord,
                                                        cancellationLog: CancellationLog,
                                                        stanoxOriginating: StanoxRecord,
                                                        stanoxCancelled: StanoxRecord,
                                                        emailer: Emailer): IO[Unit] = {
        val headline = "Train Delay Helper: Cancel Update"
        val email = Email(
          headline,
          EmailTemplates.movementEmailTemplate(
            headline,
            cancellationLogToBody(cancellationLog, stanoxOriginating, stanoxCancelled)),
          subscriberRecord.emailAddress
        )
        emailer.sendEmail(email)
      }

    }

  def movementLogToBody(movementLog: MovementLog,
                        stanoxOriginated: StanoxRecord,
                        stanoxAffected: StanoxRecord): String =
    s"""
       |Train ID: ${movementLog.scheduleTrainId.value}<br/>
       |Train originated from: ${stationTextFrom(stanoxOriginated)}<br/>
       |Station affected: ${stationTextFrom(stanoxAffected)}<br/>
       |Operator: ${movementLog.toc.value}<br/>
       |<br/>
       |Event type: ${movementLog.eventType.string}<br/>
       |Expected time: ${timestampToFormattedDateTime(movementLog.plannedPassengerTimestamp)}<br/>
       |Actual time: ${timestampToFormattedDateTime(movementLog.actualTimestamp)}<br/>
       |Status: ${statusTextFrom(movementLog.variationStatus,
                                 movementLog.plannedPassengerTimestamp,
                                 movementLog.actualTimestamp)}<br/>
       |
     """.stripMargin

  def cancellationLogToBody(cancellationLog: CancellationLog,
                            stanoxOriginating: StanoxRecord,
                            stanoxCancelled: StanoxRecord): String =
    s"""
       |Train ID: ${cancellationLog.scheduleTrainId.value}<br/>
       |Train originated from: ${stationTextFrom(stanoxOriginating)}<br/>
       |Stop cancelled: ${stationTextFrom(stanoxCancelled)}<br/>
       |Operator: ${cancellationLog.toc.value}<br/>
       |<br/>
       |Cancellation type: ${cancellationLog.cancellationType.string}<br/>
       |Expected departure time: ${timestampToFormattedDateTime(cancellationLog.originDepartureTimestamp)}<br/>
       |
     """.stripMargin

  def timestampToFormattedDateTime(timestamp: Long): String = {
    val zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone)
    zonedDateTime.format(SubscriberHandler.dateTimeFormatter)
  }

  private def stationTextFrom(stanox: StanoxRecord): String =
    s"${stanox.crs.map(str => s"[${str.value}]").getOrElse("")} ${stanox.description.getOrElse("")}"

  def statusTextFrom(variationStatus: VariationStatus, expectedTime: Long, actualTime: Long): String =
    variationStatus match {
      case VariationStatus.OnTime   => "On Time"
      case VariationStatus.Early    => s"Early by ${(expectedTime - actualTime) / 1000 / 60} minutes"
      case VariationStatus.Late     => s"Late by ${(actualTime - expectedTime) / 1000 / 60} minutes"
      case VariationStatus.OffRoute => s"Train is off route"
    }

}
