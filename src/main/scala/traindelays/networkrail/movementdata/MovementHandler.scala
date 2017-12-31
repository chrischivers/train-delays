package traindelays.networkrail.movementdata

import java.util

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.async.mutable.Queue
import io.circe.parser._
import traindelays.stomp.StompHandler

object MovementHandler extends StrictLogging {

  import cats.instances.list._
  import cats.syntax.traverse._

  def apply(movementMessageQueue: Queue[IO, MovementRecord]) = new StompHandler {
    override def message(headers: util.Map[_, _], body: String): Unit = {
      handleMessage(body).unsafeRunSync()

      def handleMessage(msg: String): IO[Unit] = {
        logger.debug(s"Handling message [$msg]")
        (for {
          parsedMsg <- parse(msg)
          decoded   <- parsedMsg.as[List[MovementRecord]]
        } yield decoded)
          .fold(
            err => IO(logger.error(s"Error parsing movement message [$msg]", err)),
            movementDataList => movementDataList.map(movementMessageQueue.enqueue1).sequence[IO, Unit].map(_ => ())
          )
      }
    }
  }
}
