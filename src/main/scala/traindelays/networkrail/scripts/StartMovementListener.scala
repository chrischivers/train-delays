package traindelays.networkrail.scripts

import akka.actor.ActorSystem
import cats.effect.IO
import cats.instances.queue
import fs2.async
import fs2.async.mutable.Queue
import redis.RedisClient
import traindelays.ConfigLoader
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global //TODO something better with this

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
    _ <- createMovementMessageProcessor(trainMovementQueue, trainActivationQueue).run
  } yield ()

  private def createMovementMessageProcessor(trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
                                             trainActivationMessageQueue: Queue[IO, TrainActivationRecord]) =
    usingTransactor(config.databaseConfig)() { db =>
      val movementLogTable  = MovementLogTable(db)
      val subscriberTable   = MemoizedSubscriberTable(db, config.networkRailConfig.subscribersConfig)
      val emailer           = Emailer(config.emailerConfig)
      val subscriberFetcher = SubscriberHandler(movementLogTable, subscriberTable, emailer)

      val redisClient =
        RedisClient(config.redisConfig.host, config.redisConfig.port, password = None, Some(config.redisConfig.dbIndex))
      val trainActivationCache = TrainActivationCache(redisClient, config.networkRailConfig.movements.activationExpiry)

      val trainActivationProcessor = TrainActivationProcessor(trainActivationMessageQueue, trainActivationCache)
      val trainMovementProcessor =
        TrainMovementProcessor(trainMovementMessageQueue, movementLogTable, subscriberFetcher, trainActivationCache)

      trainActivationProcessor.stream.concurrently {
        trainMovementProcessor.stream
      }
    }

  app.unsafeRunSync()

}
