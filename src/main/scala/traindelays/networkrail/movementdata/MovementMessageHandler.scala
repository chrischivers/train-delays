package traindelays.networkrail.movementdata

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.async.mutable.Queue
import io.circe.parser._
import traindelays.NetworkRailConfig
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
            trainChangeOfOriginMessageQueue: Queue[IO, TrainChangeOfOriginRecord],
            f: => StompClient)(implicit executionContext: ExecutionContext): fs2.Stream[IO, Unit] =
    fs2.Stream.bracket(IO(f))(
      { client =>
        for {
          _ <- fs2.Stream.eval(IO(logger.info("Incoming message queue created")))
          stompListener = StompStreamListener(incomingMessageQueue)
          _ <- fs2.Stream.eval {
            networkRailConfig.movements.topics.traverse[IO, Unit](topic =>
              activateClient(client, stompListener, topic).flatMap(_ =>
                IO(logger.info(s"Streaming Client activated with topic $topic"))))
          }
          _ <- handleMessages(stompListener,
                              trainMovementMessageQueue,
                              trainActivationMessageQueue,
                              trainCancellationMessageQueue,
                              trainChangeOfOriginMessageQueue)
        } yield ()
      },
      client => client.disconnect.map(_ => logger.info("Client disconnected"))
    )

  private def activateClient(stompClient: StompClient, stompListener: StompStreamListener, topicName: String) =
    stompClient.subscribe(topicName, stompListener)

  private def handleMessages(
      stompListener: StompStreamListener,
      trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
      trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
      trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord],
      trainChangeOfOriginMessageQueue: Queue[IO, TrainChangeOfOriginRecord]): fs2.Stream[IO, Unit] = {

    def handleMessage(msg: String): IO[Unit] =
      (for {
        parsedMsg      <- parse(msg)
        trainMovements <- parsedMsg.as[List[TrainMovements]]
      } yield trainMovements)
        .fold[IO[Unit]](
          err => IO(logger.error(s"Error parsing movement message [$msg]", err)),
          _.map {
            case tar: TrainActivationRecord      => trainActivationMessageQueue.enqueue1(tar)
            case tmr: TrainMovementRecord        => trainMovementMessageQueue.enqueue1(tmr)
            case tcr: TrainCancellationRecord    => trainCancellationMessageQueue.enqueue1(tcr)
            case tcor: TrainChangeOfOriginRecord => trainChangeOfOriginMessageQueue.enqueue1(tcor)
            case utr: UnhandledTrainRecord =>
              IO(logger.info(s"Unhandled train message of type: ${utr.unhandledType}"))
          }.sequence[IO, Unit].map(_ => ())
        )

    logger.info("Starting message handling stream")
    stompListener.messageQueue.dequeue.flatMap(msg => fs2.Stream.eval(handleMessage(msg)))
  }
}
