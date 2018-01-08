package traindelays.networkrail.subscribers

import cats.effect.IO
import cats.instances.list._
import cats.syntax.traverse._
import traindelays.networkrail.db.{MovementLogTable, SubscriberTable}
import traindelays.networkrail.movementdata.MovementLog
import traindelays.networkrail.subscribers.Emailer.Email

trait SubscriberHandler {

  def generateSubscriberReports: IO[List[SubscriberReport]]

  def notifySubscribersSink: fs2.Sink[IO, MovementLog]
}

object SubscriberHandler {
  def apply(movementLogTable: MovementLogTable, subscriberTable: SubscriberTable, emailer: Emailer) =
    new SubscriberHandler {
      override def generateSubscriberReports: IO[List[SubscriberReport]] =
        for {
          watchingRecords <- subscriberTable.retrieveAllRecords()
          movementLogs    <- movementLogTable.retrieveAllRecords() //TODO make scalable
        } yield {
          watchingRecords.map { watchingRecord =>
            val filteredLogs =
              movementLogs.filter(
                log =>
                  log.trainId == watchingRecord.trainId
                    && log.serviceCode == watchingRecord.serviceCode)
            SubscriberReport(watchingRecord, filteredLogs)
          }
        }

      override def notifySubscribersSink: fs2.Sink[IO, MovementLog] = fs2.Sink[IO, MovementLog] { log =>
        //TODO only notify in particular circumstances (e.g. late)

        for {
          subscriberList <- subscriberTable
            .subscriberRecordsFor(log.trainId, log.serviceCode, log.stanox) //todo memoize
          affected = getSubscribersForLog(subscriberList, log)
          _ <- affected.traverse(subscriber => emailSubscriber(subscriber, log, emailer))
        } yield ()

      }

      private def getSubscribersForLog(allSubscribers: List[SubscriberRecord],
                                       movementLog: MovementLog): List[SubscriberRecord] =
        allSubscribers.filter(
          subscriber =>
            subscriber.serviceCode == movementLog.serviceCode &&
              subscriber.trainId == movementLog.trainId &&
              subscriber.stanox == movementLog.stanox)

      private def emailSubscriber(subscriberRecord: SubscriberRecord,
                                  movementLog: MovementLog,
                                  emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN DELAY UPDATE", movementLog.toString, subscriberRecord.email)
        emailer.sendEmail(email)
      }

    }
}
