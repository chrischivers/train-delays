package traindelays.networkrail.subscribers

import cats.effect.IO
import cats.instances.list._
import cats.syntax.traverse._
import traindelays.networkrail.{ServiceCode, Stanox}
import traindelays.networkrail.db.{MovementLogTable, SubscriberTable}
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.Emailer.Email

trait SubscriberHandler {

  def generateSubscriberReports: IO[List[SubscriberReport]]

  def movementNotifier: fs2.Sink[IO, MovementLog]

  def cancellationNotifier: fs2.Sink[IO, CancellationLog]
}

object SubscriberHandler {
  def apply(movementLogTable: MovementLogTable, subscriberTable: SubscriberTable, emailer: Emailer) =
    new SubscriberHandler {
      override def generateSubscriberReports: IO[List[SubscriberReport]] =
        for {
          subscriberRecords <- subscriberTable.retrieveAllRecords()
          movementLogs      <- movementLogTable.retrieveAllRecords() //TODO make scalable
        } yield {
          subscriberRecords.map { watchingRecord =>
            val filteredLogs =
              movementLogs.filter(
                log =>
                  log.scheduleTrainId == watchingRecord.scheduleTrainId &&
                    log.serviceCode == watchingRecord.serviceCode &&
                    log.stanox == watchingRecord.stanox
              ) //TODO do we care about all predicates?
            SubscriberReport(watchingRecord, filteredLogs)
          }
        }

      override def movementNotifier: fs2.Sink[IO, MovementLog] = fs2.Sink[IO, MovementLog] { log =>
        //TODO only notify in particular circumstances (e.g. late)

        for {
          allSubscribers <- subscriberTable.retrieveAllRecords()
//            .subscriberRecordsFor(log.scheduleTrainId, log.serviceCode, log.stanox)
          affected = filterSubscribersBy(allSubscribers, log.serviceCode, log.scheduleTrainId, log.stanox)
          _ <- affected.traverse(subscriber => emailSubscriberWithMovementUpdate(subscriber, log, emailer))
        } yield ()
      }

      override def cancellationNotifier: fs2.Sink[IO, CancellationLog] = fs2.Sink[IO, CancellationLog] { log =>
        for {
          subscriberList <- subscriberTable
            .subscriberRecordsFor(log.scheduleTrainId, log.serviceCode, log.stanox)
          affected = filterSubscribersBy(subscriberList, log.serviceCode, log.scheduleTrainId, log.stanox)
          _ <- affected.traverse(subscriber => emailSubscriberWithCancellationUpdate(subscriber, log, emailer))
        } yield ()
      }

      private def filterSubscribersBy(allSubscribers: List[SubscriberRecord],
                                      serviceCode: ServiceCode,
                                      scheduleTrainId: ScheduleTrainId,
                                      stanox: Stanox): List[SubscriberRecord] =
        allSubscribers.filter(
          subscriber =>
            subscriber.serviceCode == serviceCode &&
              subscriber.scheduleTrainId == scheduleTrainId &&
              subscriber.stanox == stanox) //TODO do we care about all predicates?

      //TODO set proper notifcations
      private def emailSubscriberWithMovementUpdate(subscriberRecord: SubscriberRecord,
                                                    movementLog: MovementLog,
                                                    emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN DELAY UPDATE", movementLog.toString, subscriberRecord.email)
        emailer.sendEmail(email)
      }

      private def emailSubscriberWithCancellationUpdate(subscriberRecord: SubscriberRecord,
                                                        cancellationLog: CancellationLog,
                                                        emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN CANCELLATION UPDATE", cancellationLog.toString, subscriberRecord.email)
        emailer.sendEmail(email)
      }

    }
}
