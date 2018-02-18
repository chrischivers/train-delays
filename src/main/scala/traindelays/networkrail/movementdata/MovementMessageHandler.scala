package traindelays.networkrail.movementdata

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.async.mutable.Queue
import io.circe.parser._
import traindelays.NetworkRailConfig
import traindelays.metrics.MetricsLogging
import traindelays.stomp.{StompClient, StompStreamListener}

import scala.concurrent.ExecutionContext

object MovementMessageHandler extends StrictLogging {

  import cats.instances.list._
  import cats.syntax.traverse._

  def apply(networkRailConfig: NetworkRailConfig,
            incomingMessageQueue: Queue[IO, String],
            trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
            trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
            trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord],
            metricsLogging: MetricsLogging,
            f: => StompClient)(implicit executionContext: ExecutionContext): fs2.Stream[IO, Unit] =
    fs2.Stream.bracket(IO(f))(
      { client =>
        for {
          _ <- fs2.Stream.eval(IO(logger.info("Incoming message queue created")))
          stompListener = StompStreamListener(incomingMessageQueue)
          _ <- fs2.Stream.eval(activateClient(client, stompListener, networkRailConfig.movements.topic))
          _ <- fs2.Stream.eval(
            IO(logger.info(s"Streaming Client activated with topic ${networkRailConfig.movements.topic}")))
          _ <- handleMessages(stompListener,
                              trainMovementMessageQueue,
                              trainActivationMessageQueue,
                              trainCancellationMessageQueue,
                              metricsLogging)
        } yield ()
      },
      client => client.disconnect.map(_ => logger.info("Client disconnected"))
    )

  private def activateClient(stompClient: StompClient, stompListener: StompStreamListener, topicName: String) =
    stompClient.subscribe(topicName, stompListener)

  private def handleMessages(stompListener: StompStreamListener,
                             trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
                             trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
                             trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord],
                             metricsLogging: MetricsLogging): fs2.Stream[IO, Unit] = {

    def handleMessage(msg: String): IO[Unit] =
      (for {
        parsedMsg      <- parse(msg)
        trainMovements <- parsedMsg.as[List[TrainMovements]]
      } yield trainMovements)
        .fold[IO[Unit]](
          err => IO(logger.error(s"Error parsing movement message [$msg]", err)),
          _.map {
            case tar: TrainActivationRecord =>
              metricsLogging.incrActivationRecordsReceived
              trainActivationMessageQueue.enqueue1(tar)
            case tmr: TrainMovementRecord =>
              metricsLogging.incrMovementRecordsReceived
              trainMovementMessageQueue.enqueue1(tmr)
            case tcr: TrainCancellationRecord =>
              metricsLogging.incrCancellationRecordsReceived
              trainCancellationMessageQueue.enqueue1(tcr)
            case utr: UnhandledTrainRecord =>
              IO {
                logger.info(s"Unhandled train message of type: ${utr.unhandledType}")
                metricsLogging.incrUnhandledRecordsReceived
              }
          }.sequence[IO, Unit].map(_ => ())
        )

    logger.info("Starting message handling stream")
    stompListener.messageQueue.dequeue.flatMap(msg => fs2.Stream.eval(handleMessage(msg)))
  }
}
