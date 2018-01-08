package traindelays.networkrail.scripts

import cats.effect.IO
import fs2.async
import fs2.async.mutable.Queue
import traindelays.ConfigLoader
import traindelays.networkrail.db._
import traindelays.networkrail.movementdata.{MovementMessageHandler, MovementProcessor, MovementRecord}
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global //TODO something better with this

object StartMovementListener extends App {

  val config      = ConfigLoader.defaultConfig
  val stompClient = StompClient(config.networkRailConfig)

  val app = for {
    queue <- async.unboundedQueue[IO, MovementRecord]
    movementMessageHandler = MovementMessageHandler(queue)
    _ <- stompClient.subscribe(config.networkRailConfig.movements.topic, movementMessageHandler)
    _ <- createMovementProcessor(queue).run
  } yield ()

  private def createMovementProcessor(movementMessageQueue: Queue[IO, MovementRecord]) =
    usingTransactor(config.databaseConfig)() { db =>
      val movementLogTable  = MovementLogTable(db)
      val subscriberTable   = MemoizedSubscriberTable(db, config.networkRailConfig.subscribersConfig)
      val emailer           = Emailer(config.emailerConfig)
      val subscriberFetcher = SubscriberHandler(movementLogTable, subscriberTable, emailer)
      MovementProcessor(movementMessageQueue, movementLogTable, subscriberFetcher).stream
    }

  app.unsafeRunSync()

}
