package traindelays.networkrail.movementdata

import org.scalatest.FlatSpec
import traindelays.networkrail.MockStompClient
import org.scalatest.Matchers._
import traindelays.TestFeatures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

class MovementMessageHandlerTest extends FlatSpec with TestFeatures {

  it should "receive movements message and parse/decode, putting them on the correct queues based on message type" in {

    withQueues
      .map {
        case (trainMovementQueue, trainActivationQueue) =>
          val mockStompClient = MockStompClient()
          val listener        = new MovementMessageHandlerWatcher(trainMovementQueue, trainActivationQueue)
          mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()

          mockStompClient.sendMessage("test/topic", sampleRawMovementMessage)
          listener.rawMessagesReceived should have size 1
          trainMovementQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList should have size 32
          trainActivationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList should have size 1
      }
      .unsafeRunSync()
  }

  def sampleRawMovementMessage = Source.fromResource("sample-movement-message.json").getLines().mkString
}
