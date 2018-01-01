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

    async
      .unboundedQueue[IO, MovementRecord]
      .map { queue =>
        val mockStompClient = MockStompClient()
        val listener        = new MovementHandlerWatcher(queue)
        mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()

        mockStompClient.sendMessage("test/topic", sampleRawMovementMessage)
        listener.rawMessagesReceived should have size 1
        queue.dequeueBatch1(100).unsafeRunSync().toList should have size 32
      }
      .unsafeRunSync()
  }

  def sampleRawMovementMessage = Source.fromResource("sample-movement-message.json").getLines().mkString
}
