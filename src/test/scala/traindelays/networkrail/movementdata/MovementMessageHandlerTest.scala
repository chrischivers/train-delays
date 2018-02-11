package traindelays.networkrail.movementdata

import cats.effect.IO
import org.scalatest.FlatSpec
import traindelays.networkrail.MockStompClient
import org.scalatest.Matchers._
import traindelays.TestFeatures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

class MovementMessageHandlerTest extends FlatSpec with TestFeatures {

  it should "receive activation message and put onto activation queue" in {

    withQueues { queues =>
      val (mockStompClient, listener) =
        setUpClient(queues)
      mockStompClient.sendMessage("test/topic", sampleActivationMovementMessage)
      listener.rawMessagesReceived should have size 1
      queues.trainActivationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList should have size 1
    }
  }

  it should "receive movement message and put onto movement queue" in {

    withQueues { queues =>
      val (mockStompClient, listener) =
        setUpClient(queues)
      mockStompClient.sendMessage("test/topic", sampleMovementMessage)
      listener.rawMessagesReceived should have size 1
      queues.trainMovementQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList should have size 1
    }
  }

  it should "receive cancellation message and put onto cancellation queue" in {

    withQueues { queues =>
      val (mockStompClient, listener) =
        setUpClient(queues)
      mockStompClient.sendMessage("test/topic", sampleCancellationMovementMessage)
      listener.rawMessagesReceived should have size 1
      queues.trainCancellationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList should have size 1
    }
  }

  def sampleActivationMovementMessage =
    Source.fromResource("sample-movement-activation-message.json").getLines().mkString

  def sampleCancellationMovementMessage =
    Source.fromResource("sample-movement-cancellation-message.json").getLines().mkString

  def sampleMovementMessage =
    Source.fromResource("sample-movement-message.json").getLines().mkString

  def setUpClient(queues: Queues) = {
    val mockStompClient = MockStompClient()
    val listener =
      new MovementMessageHandlerWatcher(queues.trainMovementQueue,
                                        queues.trainActivationQueue,
                                        queues.trainCancellationQueue)
    mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()
    (mockStompClient, listener)
  }
}
