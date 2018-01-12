package traindelays.networkrail.movementdata

import java.util

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.async.mutable.Queue
import io.circe.DecodingFailure
import io.circe.parser._
import traindelays.stomp.StompHandler

object MovementMessageHandler extends StrictLogging {

  import cats.instances.list._
  import cats.syntax.traverse._

  def apply(trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
            trainActivationMessageQueue: Queue[IO, TrainActivationRecord]) = new StompHandler {
    override def message(headers: util.Map[_, _], body: String): Unit = {
      handleMessage(body).unsafeRunSync()

      def handleMessage(msg: String): IO[Unit] = {
        logger.debug(s"Handling message [$msg]")
        (for {
          parsedMsg <- parse(msg)
          _ = println(parsedMsg)
          messageType <- parsedMsg.hcursor.downField("header").downField("msg_type").as[String]
          decodedMsg <- messageType match {
            case "0001"  => parsedMsg.as[TrainActivationRecord]
            case "0003"  => parsedMsg.as[TrainMovementRecord]
            case unknown => Left(DecodingFailure(s"Unknown message type: $unknown", List.empty))
          }
        } yield decodedMsg)
          .fold(
            err => IO(logger.error(s"Error parsing movement message [$msg]", err)), {
              case tar: TrainActivationRecord => trainActivationMessageQueue.enqueue1(tar)
              case tmr: TrainMovementRecord   => trainMovementMessageQueue.enqueue1(tmr)
            }
          )
      }
    }
  }
}
