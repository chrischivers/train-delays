package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async
import org.scalatest.FlatSpec
import traindelays.networkrail.MockStompClient
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

class MovementMessageHandlerTest extends FlatSpec {

  it should "receive movements message and parse/decode" in {

    withQueues
      .map {
        case (trainMovementQueue, trainActivationQueue) =>
          val mockStompClient = MockStompClient()
          val listener        = new MovementMessageHandlerWatcher(trainMovementQueue, trainActivationQueue)
          mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()

          mockStompClient.sendMessage("test/topic", sampleRawMovementMessage)
          listener.rawMessagesReceived should have size 1
          trainMovementQueue.dequeueBatch1(100).unsafeRunSync().toList should have size 32
      }
      .unsafeRunSync()
  }

  def sampleRawMovementMessage = Source.fromResource("sample-movement-message.json").getLines().mkString

  def withQueues =
    for {
      trainMovementQueue   <- fs2.async.unboundedQueue[IO, TrainMovementRecord]
      trainActivationQueue <- fs2.async.unboundedQueue[IO, TrainActivationRecord]
    } yield (trainMovementQueue, trainActivationQueue)
}
