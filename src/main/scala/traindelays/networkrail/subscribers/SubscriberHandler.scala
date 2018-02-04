package traindelays.networkrail.subscribers

import cats.effect.IO
import cats.instances.list._
import cats.syntax.traverse._
import traindelays.networkrail.{ServiceCode, StanoxCode}
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, SubscriberTable}
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.Emailer.Email
import cats.instances.list._
import cats.syntax.traverse._

trait SubscriberHandler {

  def movementNotifier: fs2.Sink[IO, MovementLog]

  def cancellationNotifier: fs2.Sink[IO, CancellationLog]
}

object SubscriberHandler {
  def apply(movementLogTable: MovementLogTable,
            subscriberTable: SubscriberTable,
            scheduleTable: ScheduleTable,
            emailer: Emailer) =
    new SubscriberHandler {

      override def movementNotifier: fs2.Sink[IO, MovementLog] = fs2.Sink[IO, MovementLog] { log =>
        //TODO only notify in particular circumstances (e.g. late)

        for {
          affected <- affectedSubscribersFor(log.scheduleTrainId, log.serviceCode, log.stanoxCode)
          _        <- affected.traverse(subscriber => emailSubscriberWithMovementUpdate(subscriber, log, emailer))
        } yield ()
      }

      override def cancellationNotifier: fs2.Sink[IO, CancellationLog] = fs2.Sink[IO, CancellationLog] { log =>
        for {
          affected <- affectedSubscribersFor(log.scheduleTrainId, log.serviceCode, log.stanoxCode)
          _        <- affected.traverse(subscriber => emailSubscriberWithCancellationUpdate(subscriber, log, emailer))
        } yield ()
      }

      private def affectedSubscribersFor(scheduleTrainId: ScheduleTrainId,
                                         serviceCode: ServiceCode,
                                         stanoxCode: StanoxCode): IO[List[SubscriberRecord]] =
        for {
          subscribersOnRoute <- subscriberTable.subscriberRecordsFor(scheduleTrainId, serviceCode)
          _ = println("subs on route: " + subscribersOnRoute)
          affected <- affectedSubscribers(subscribersOnRoute, stanoxCode, scheduleTable)
          _ = println("affected: " + affected)
        } yield affected

      private def affectedSubscribers(subscribersOnRoute: List[SubscriberRecord],
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

      //TODO set proper notifcations
      private def emailSubscriberWithMovementUpdate(subscriberRecord: SubscriberRecord,
                                                    movementLog: MovementLog,
                                                    emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN MOVEMENT UPDATE", movementLog.toString, subscriberRecord.emailAddress)
        emailer.sendEmail(email)
      }

      private def emailSubscriberWithCancellationUpdate(subscriberRecord: SubscriberRecord,
                                                        cancellationLog: CancellationLog,
                                                        emailer: Emailer): IO[Unit] = {
        val email = Email("TRAIN CANCELLATION UPDATE", cancellationLog.toString, subscriberRecord.emailAddress)
        emailer.sendEmail(email)
      }

    }
}
