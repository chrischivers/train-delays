package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import traindelays.NetworkRailConfig
import traindelays.stomp.StompStreamListener

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class MovementMessageHandlerWatcher(
    networkRailConfig: NetworkRailConfig,
    trainMovementMessageQueue: Queue[IO, TrainMovementRecord],
    trainActivationMessageQueue: Queue[IO, TrainActivationRecord],
    trainCancellationMessageQueue: Queue[IO, TrainCancellationRecord])(implicit executionContext: ExecutionContext)
    extends StompStreamListener {

  var rawMessagesReceived = ListBuffer[String]()

  override val messageQueue: Queue[IO, String] = fs2.async.mutable.Queue.unbounded[IO, String].unsafeRunSync()

  override def message(headers: java.util.Map[_, _], body: String): Unit = {
    println(s"Received message with headers [$headers] and body [$body]")
    messageQueue.enqueue1(body).unsafeRunSync()
    rawMessagesReceived += body

  }

}
