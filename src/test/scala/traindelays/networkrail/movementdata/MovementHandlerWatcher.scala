package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async
import fs2.async.mutable.Queue
import io.circe.parser._
import traindelays.stomp.StompHandler

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class MovementHandlerWatcher(queue: Queue[IO, MovementRecord])(implicit executionContext: ExecutionContext)
    extends StompHandler {

  var rawMessagesReceived      = ListBuffer[String]()
  private val movementsHandler = MovementMessageHandler(queue)

  override def message(headers: java.util.Map[_, _], body: String): Unit = {
    println(s"Received message with headers [$headers] and body [$body]")
    movementsHandler.message(headers, body)
    rawMessagesReceived += body

  }
}
