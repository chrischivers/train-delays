package traindelays.networkrail.subscribers

import java.time._
import java.time.format.{DateTimeFormatter, FormatStyle}

import cats.effect.IO
import cats.instances.list._
import cats.syntax.traverse._
import com.typesafe.scalalogging.StrictLogging
import fs2.Sink
import traindelays.SubscribersConfig
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordPrimary, ScheduleRecordSecondary}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, StanoxTable, SubscriberTable}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.subscribers.Emailer.Email

trait SubscriberHandler {

  def movementNotifier: fs2.Sink[IO, MovementLog]

  def cancellationNotifier: fs2.Sink[IO, CancellationLog]

  def changeOfOriginNotifier: fs2.Sink[IO, ChangeOfOriginLog]
}

object SubscriberHandler extends StrictLogging {

  def apply(movementLogTable: MovementLogTable,
            subscriberTable: SubscriberTable,
            primaryScheduleTable: ScheduleTable[ScheduleRecordPrimary],
            secondaryScheduleTable: ScheduleTable[ScheduleRecordSecondary],
            stanoxTable: StanoxTable,
            emailer: Emailer,
            subscribersConfig: SubscribersConfig) =
    new SubscriberHandler {

      override def movementNotifier: fs2.Sink[IO, MovementLog] = fs2.Sink[IO, MovementLog] { log =>
        def email =
          for {
            subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
            affected <- filterSubscribersOnStanoxRange(subscribersOnRoute,
                                                       log.stanoxCode,
                                                       primaryScheduleTable,
                                                       secondaryScheduleTable)
            _ <- if (affected.nonEmpty) createEmailAction(log, affected) else IO.unit
          } yield ()

        if (isNotifiable(log)) email else IO.unit
      }

      private def isNotifiable(log: MovementLog): Boolean =
        log.variationStatus match {
          case VariationStatus.Late
              if log.actualTimestamp - log.plannedPassengerTimestamp > subscribersConfig.lateNotifyAfter.toMillis =>
            true
          case VariationStatus.OffRoute => true
          case _                        => false
        }

      override def cancellationNotifier: fs2.Sink[IO, CancellationLog] = fs2.Sink[IO, CancellationLog] { log =>
        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
          //For cancellation on a route all subscribers are notified
          //TODO is this assumption correct
          _ <- if (subscribersOnRoute.nonEmpty) createEmailAction(log, subscribersOnRoute) else IO.unit
        } yield ()
      }

      override def changeOfOriginNotifier: Sink[IO, ChangeOfOriginLog] = fs2.Sink[IO, ChangeOfOriginLog] { log =>
        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(log.scheduleTrainId, log.serviceCode)
          //For cancellation on a route all subscribers are notified
          //TODO is this assumption correct. Probably not.
          _ <- if (subscribersOnRoute.nonEmpty) createEmailAction(log, subscribersOnRoute) else IO.unit
        } yield ()
      }

      private def filterSubscribersOnStanoxRange(
          subscribersOnRoute: List[SubscriberRecord],
          affectedStanoxCode: StanoxCode,
          primaryScheduleTable: ScheduleTable[ScheduleRecordPrimary],
          secondaryScheduleTable: ScheduleTable[ScheduleRecordSecondary]
      ): IO[List[SubscriberRecord]] =
        subscribersOnRoute
          .map { subscriber =>
            for {
              primRecs <- primaryScheduleTable.retrieveScheduleRecordsFor(subscriber.scheduleTrainId,
                                                                          subscriber.fromStanoxCode)
              secRecs <- secondaryScheduleTable.retrieveScheduleRecordsFor(subscriber.scheduleTrainId,
                                                                           subscriber.fromStanoxCode)
            } yield {
              subscriber -> (primRecs ++ secRecs).exists(scheduleLog => {
                val toStanoxCodeIdx       = scheduleLog.subsequentStanoxCodes.indexOf(subscriber.toStanoxCode)
                val affectedStanoxCodeIdx = scheduleLog.subsequentStanoxCodes.indexOf(affectedStanoxCode)
                if (subscriber.fromStanoxCode == affectedStanoxCode) true
                else if (toStanoxCodeIdx == -1 || affectedStanoxCodeIdx == -1) false
                else if (affectedStanoxCodeIdx > toStanoxCodeIdx) false
                else true
              })
            }
          }
          .sequence[IO, (SubscriberRecord, Boolean)]
          .map(_.collect { case (subscriber, true) => subscriber })

      def createEmailAction(log: DBLog, affectedSubscribers: List[SubscriberRecord]): IO[Unit] = {
        import cats.implicits._
        for {
          _                     <- IO(logger.info(s"Creating email for $log and subscribers $affectedSubscribers"))
          originatingStanoxList <- stanoxTable.stanoxRecordsFor(log.originStanoxCode)
          affectedStanoxList    <- stanoxTable.stanoxRecordsFor(log.stanoxCode)
          _ <- affectedSubscribers
            .traverse[IO, Unit] { subscriber =>
              val emailAction = for {
                originatingStanox <- getMainStanox(originatingStanoxList)
                affectedStanox    <- getMainStanox(affectedStanoxList)
              } yield {
                log match {
                  case l: MovementLog =>
                    emailSubscriberWithMovementUpdate(subscriber, l, originatingStanox, affectedStanox, emailer)
                  case l: CancellationLog =>
                    emailSubscriberWithCancellationUpdate(subscriber, l, originatingStanox, affectedStanox, emailer)
                  case l: ChangeOfOriginLog =>
                    emailSubscriberWithChangeOfOriginUpdate(subscriber, l, originatingStanox, affectedStanox, emailer)
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

      private def emailSubscriberWithChangeOfOriginUpdate(subscriberRecord: SubscriberRecord,
                                                          changeOfOriginLog: ChangeOfOriginLog,
                                                          stanoxOriginating: StanoxRecord,
                                                          originChangedTo: StanoxRecord,
                                                          emailer: Emailer): IO[Unit] = {
        val headline = "Train Delay Helper: Change of Origin Update"
        val email = Email(
          headline,
          EmailTemplates
            .movementEmailTemplate(headline,
                                   changeOfOriginLogToBody(changeOfOriginLog, stanoxOriginating, originChangedTo)),
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
       |Date: ${dateFormatter(movementLog.originDepartureDate)}
       |Train originated from: ${stationTextFrom(stanoxOriginated)} at ${timeFormatter(movementLog.originDepartureTime)}<br/>
       |Station affected: ${stationTextFrom(stanoxAffected)}<br/>
       |Operator: ${movementLog.toc.value}<br/>
       |<br/>
       |Event type: ${movementLog.eventType.string}<br/>
       |Expected time: ${timeFormatter(movementLog.plannedPassengerTime)}<br/>
       |Actual time: ${timeFormatter(movementLog.actualTime)}<br/>
       |Status: ${statusTextFrom(movementLog.variationStatus,
                                 movementLog.plannedPassengerTimestamp,
                                 movementLog.actualTimestamp)}<br/>
       |
     """.stripMargin

  //TODO need to include subscribers stop in this
  def cancellationLogToBody(cancellationLog: CancellationLog,
                            stanoxOriginating: StanoxRecord,
                            stanoxCancelled: StanoxRecord): String =
    s"""
     |Train ID: ${cancellationLog.scheduleTrainId.value}<br/>
     |Date: ${dateFormatter(cancellationLog.originDepartureDate)}
     |Train originating from: ${stationTextFrom(stanoxOriginating)}<br/>
     |Expected departure time: ${timeFormatter(cancellationLog.originDepartureTime)}<br/>
     |Cancelled at: ${stationTextFrom(stanoxCancelled)}<br/>
     |Operator: ${cancellationLog.toc.value}<br/>
     |<br/>
     |Cancellation type: ${cancellationLog.cancellationType.string}<br/>

     |
     """.stripMargin

  def changeOfOriginLogToBody(changeOfOriginLog: ChangeOfOriginLog,
                              stanoxOriginating: StanoxRecord,
                              originChangedTo: StanoxRecord): String =
    s"""
       |Train ID: ${changeOfOriginLog.scheduleTrainId.value}<br/>
       |Date: ${dateFormatter(changeOfOriginLog.originDepartureDate)}
       |Train originating from: ${stationTextFrom(stanoxOriginating)}<br/>
       |Expected departure time: ${timeFormatter(changeOfOriginLog.originDepartureTime)}<br/>
       |Origin Changed To: ${stationTextFrom(originChangedTo)}<br/>
       |Operator: ${changeOfOriginLog.toc.value}<br/>
       |
     """.stripMargin

  private def stationTextFrom(stanox: StanoxRecord): String =
    s"${stanox.crs.map(str => s"[${str.value}]").getOrElse("")} ${stanox.description.getOrElse("")}"

  def statusTextFrom(variationStatus: VariationStatus, expectedTime: Long, actualTime: Long): String =
    variationStatus match {
      case VariationStatus.OnTime   => "On Time"
      case VariationStatus.Early    => s"Early by ${(expectedTime - actualTime) / 1000 / 60} minutes"
      case VariationStatus.Late     => s"Late by ${(actualTime - expectedTime) / 1000 / 60} minutes"
      case VariationStatus.OffRoute => s"Train is off route"
    }

  private def getMainStanox(stanoxRecords: List[StanoxRecord]): Option[StanoxRecord] =
    stanoxRecords.filter(_.crs.isDefined).find(_.primary.contains(true)).orElse(stanoxRecords.headOption)

  def timeFormatter(time: LocalTime): String =
    time.format(DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT))

  def dateFormatter(date: LocalDate): String =
    date.format(DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT))
}
