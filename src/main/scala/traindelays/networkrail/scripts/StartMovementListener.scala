package traindelays.networkrail.scripts

import cats.effect.IO
import cats.instances.queue
import fs2.async
import fs2.async.mutable.Queue
import traindelays.ConfigLoader
import traindelays.networkrail.db._
import traindelays.networkrail.movementdata.{
  MovementMessageHandler,
  TrainActivationRecord,
  TrainMovementProcessor,
  TrainMovementRecord
}
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global //TODO something better with this

object StartMovementListener extends App {

  val config      = ConfigLoader.defaultConfig
  val stompClient = StompClient(config.networkRailConfig)

  val app = for {
    trainMovementQueue   <- async.unboundedQueue[IO, TrainMovementRecord]
    trainActivationQueue <- async.unboundedQueue[IO, TrainActivationRecord]
    movementMessageHandler = MovementMessageHandler(trainMovementQueue, trainActivationQueue)
    _ <- stompClient.subscribe(config.networkRailConfig.movements.topic, movementMessageHandler)
    _ <- createMovementProcessor(trainMovementQueue, trainActivationQueue).run
  } yield ()

  private def createMovementProcessor(trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
                                      trainActivationMessageQueue: Queue[IO, TrainActivationRecord]) =
    usingTransactor(config.databaseConfig)() { db =>
      val movementLogTable  = MovementLogTable(db)
      val subscriberTable   = MemoizedSubscriberTable(db, config.networkRailConfig.subscribersConfig)
      val emailer           = Emailer(config.emailerConfig)
      val subscriberFetcher = SubscriberHandler(movementLogTable, subscriberTable, emailer)
      TrainMovementProcessor(trainMovementMessageQueue, movementLogTable, subscriberFetcher).stream
      ???
    }

  app.unsafeRunSync()

}
