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
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog, VariationStatus}
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
          _ = if (subscribersOnRoute.nonEmpty) println("Subscribers on route: " + subscribersOnRoute) else IO.unit
          affected <- filterSubscribersOnStanoxRange(subscribersOnRoute, log.stanoxCode, scheduleTable)
          _ = if (affected.nonEmpty) println("Affected subscribers " + affected) else IO.unit
          _ <- if (affected.nonEmpty) createEmailAction(log, affected).value else IO.unit
        } yield ()
      }

      override def cancellationNotifier: fs2.Sink[IO, CancellationLog] = fs2.Sink[IO, CancellationLog] { log =>
        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
          //For cancellation on a route all subscribers are notified
          //TODO is this assumption correct
          _ <- subscribersOnRoute.traverse(subscriber =>
            emailSubscriberWithCancellationUpdate(subscriber, log, emailer))
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

      def createEmailAction(movementLog: MovementLog,
                            affectedSubscribers: List[SubscriberRecord]): OptionT[IO, Unit] = {
        println(s"Creating email for $movementLog and subscribers $affectedSubscribers")
        import cats.implicits._
        for {
          originatingStanoxOpt <- OptionT.liftF(stanoxTable.stanoxRecordFor(movementLog.originStanoxCode))
          affectedStanoxOpt    <- OptionT.liftF(stanoxTable.stanoxRecordFor(movementLog.stanoxCode))
          originatingStanox    <- OptionT.fromOption[IO](originatingStanoxOpt)
          affectedStanox       <- OptionT.fromOption[IO](affectedStanoxOpt)
          _ = println("affected: " + affectedStanox)
          _ = println("originating: " + originatingStanox)
          _ <- OptionT.liftF(affectedSubscribers.traverse(subscriber =>
            emailSubscriberWithMovementUpdate(subscriber, movementLog, originatingStanox, affectedStanox, emailer)))
        } yield ()
      }

      //TODO set proper notifcations
      private def emailSubscriberWithMovementUpdate(subscriberRecord: SubscriberRecord,
                                                    movementLog: MovementLog,
                                                    stanoxOriginated: StanoxRecord,
                                                    stanoxAffected: StanoxRecord,
                                                    emailer: Emailer): IO[Unit] = {
        println("HERE")
        val email = Email("TRAIN MOVEMENT UPDATE",
                          movementLogToBody(movementLog, stanoxOriginated, stanoxAffected),
                          subscriberRecord.emailAddress)
        println("email: " + email)
        emailer.sendEmail(email)
      }

      private def emailSubscriberWithCancellationUpdate(subscriberRecord: SubscriberRecord,
                                                        cancellationLog: CancellationLog,
                                                        emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN CANCELLATION UPDATE", cancellationLog.toString, subscriberRecord.emailAddress)
        emailer.sendEmail(email)
      }

    }

  def movementLogToBody(movementLog: MovementLog, stanoxOriginated: StanoxRecord, stanoxAffected: StanoxRecord) =
    s"""
       |TRAIN MOVEMENT UPDATE
       |Train ID: ${movementLog.scheduleTrainId.value}
       |Train originated from: ${stationTextFrom(stanoxOriginated)}
       |Station affected: ${stationTextFrom(stanoxAffected)}
       |Operator: ${movementLog.toc.value}
       |
       |Event type: ${movementLog.eventType.string}
       |Expected time: ${timestampToFormattedDateTime(movementLog.plannedPassengerTimestamp)}
       |Actual time: ${timestampToFormattedDateTime(movementLog.actualTimestamp)}
       |Status: ${statusTextFrom(movementLog.variationStatus,
                                 movementLog.plannedPassengerTimestamp,
                                 movementLog.actualTimestamp)}
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
