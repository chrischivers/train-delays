package traindelays

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.async
import fs2.async.mutable.Queue
import redis.RedisClient
import traindelays.metrics.MetricsLogging
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global

object StartMovementListener extends App with StrictLogging {

  val config               = TrainDelaysConfig()
  implicit val actorSystem = ActorSystem()

  val app = for {
    trainMovementQueue       <- async.unboundedQueue[IO, TrainMovementRecord]
    trainActivationQueue     <- async.unboundedQueue[IO, TrainActivationRecord]
    trainCancellationQueue   <- async.unboundedQueue[IO, TrainCancellationRecord]
    trainChangeOfOriginQueue <- async.unboundedQueue[IO, TrainChangeOfOriginRecord]
    incomingMessageQueue     <- async.unboundedQueue[IO, String]
    metricsLogging = MetricsLogging(config.metricsConfig)
    _ <- MovementMessageHandler(
      config.networkRailConfig,
      incomingMessageQueue,
      trainMovementQueue,
      trainActivationQueue,
      trainCancellationQueue,
      trainChangeOfOriginQueue,
      newStompClient(config.networkRailConfig)
    ).concurrently(
        createMovementMessageProcessor(trainMovementQueue,
                                       trainActivationQueue,
                                       trainCancellationQueue,
                                       trainChangeOfOriginQueue,
                                       metricsLogging))
      .compile
      .drain
  } yield ()

  private def createMovementMessageProcessor(trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
                                             trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
                                             trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord],
                                             trainChangeOfOriginMessageQueue: Queue[IO, TrainChangeOfOriginRecord],
                                             metricsLogging: MetricsLogging) =
    withTransactor(config.databaseConfig)() { db =>
      logger.info(s"creating movement message processor with db config ${config.databaseConfig}")
      val movementLogTable       = MovementLogTable(db)
      val cancellationLogTable   = CancellationLogTable(db)
      val changeOfOriginLogTable = ChangeOfOriginLogTable(db)
      val subscriberTable        = SubscriberTable(db, config.networkRailConfig.subscribersConfig.memoizeFor)
      val primaryScheduleTable   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val secondaryScheduleTable = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val emailer                = Emailer(config.emailerConfig, metricsLogging)
      val subscriberHandler =
        SubscriberHandler(movementLogTable,
                          subscriberTable,
                          primaryScheduleTable,
                          secondaryScheduleTable,
                          stanoxTable,
                          emailer,
                          config.networkRailConfig.subscribersConfig)

      val redisClient =
        RedisClient(config.redisConfig.host, config.redisConfig.port, password = None, Some(config.redisConfig.dbIndex))
      val trainActivationCache = TrainActivationCache(redisClient, config.networkRailConfig.movements.activationExpiry)

      val trainActivationProcessor = TrainActivationProcessor(trainActivationMessageQueue,
                                                              trainActivationCache,
                                                              metricsLogging.incrActivationRecordsReceived)
      val trainMovementProcessor =
        TrainMovementProcessor(trainMovementMessageQueue,
                               movementLogTable,
                               subscriberHandler,
                               trainActivationCache,
                               metricsLogging.incrMovementRecordsReceived)
      val trainCancellationProcessor =
        TrainCancellationProcessor(trainCancellationMessageQueue,
                                   subscriberHandler,
                                   cancellationLogTable,
                                   trainActivationCache,
                                   metricsLogging.incrCancellationRecordsReceived)

      val trainChangeOfOriginProcessor =
        TrainChangeOfOriginProcessor(trainChangeOfOriginMessageQueue,
                                     subscriberHandler,
                                     changeOfOriginLogTable,
                                     trainActivationCache,
                                     metricsLogging.incrChangeOfOriginRecordsReceived)

      trainActivationProcessor.stream
        .concurrently(trainMovementProcessor.stream)
        .concurrently(trainCancellationProcessor.stream)
        .concurrently(trainChangeOfOriginProcessor.stream)
    }

  app.unsafeRunSync()

  def newStompClient(networkRailConfig: NetworkRailConfig) = StompClient(networkRailConfig)

}
