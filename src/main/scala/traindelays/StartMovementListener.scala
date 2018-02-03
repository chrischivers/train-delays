package traindelays

import akka.actor.ActorSystem
import cats.effect.IO
import fs2.async
import fs2.async.mutable.Queue
import redis.RedisClient
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global

object StartMovementListener extends App {

  val config               = ConfigLoader.defaultConfig
  val stompClient          = StompClient(config.networkRailConfig)
  implicit val actorSystem = ActorSystem()

  val app = for {
    trainMovementQueue     <- async.unboundedQueue[IO, TrainMovementRecord]
    trainActivationQueue   <- async.unboundedQueue[IO, TrainActivationRecord]
    trainCancellationQueue <- async.unboundedQueue[IO, TrainCancellationRecord]
    movementMessageHandler = MovementMessageHandler(trainMovementQueue, trainActivationQueue, trainCancellationQueue)
    _ <- stompClient.subscribe(config.networkRailConfig.movements.topic, movementMessageHandler)
    _ <- createMovementMessageProcessor(trainMovementQueue, trainActivationQueue, trainCancellationQueue).run
  } yield ()

  private def createMovementMessageProcessor(trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
                                             trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
                                             trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord]) =
    withTransactor(config.databaseConfig)() { db =>
      val movementLogTable     = MovementLogTable(db)
      val cancellationLogTable = CancellationLogTable(db)
      val subscriberTable      = SubscriberTable(db, config.networkRailConfig.subscribersConfig.memoizeFor)
      val emailer              = Emailer(config.emailerConfig)
      val subscriberHandler    = SubscriberHandler(movementLogTable, subscriberTable, emailer)

      val redisClient =
        RedisClient(config.redisConfig.host, config.redisConfig.port, password = None, Some(config.redisConfig.dbIndex))
      val trainActivationCache = TrainActivationCache(redisClient, config.networkRailConfig.movements.activationExpiry)

      val trainActivationProcessor = TrainActivationProcessor(trainActivationMessageQueue, trainActivationCache)
      val trainMovementProcessor =
        TrainMovementProcessor(trainMovementMessageQueue, movementLogTable, subscriberHandler, trainActivationCache)
      val trainCancellationProcessor =
        TrainCancellationProcessor(trainCancellationMessageQueue,
                                   subscriberHandler,
                                   cancellationLogTable,
                                   trainActivationCache)

      trainActivationProcessor.stream
        .concurrently(trainMovementProcessor.stream)
        .concurrently(trainCancellationProcessor.stream)
    }

  app.unsafeRunSync()

}
