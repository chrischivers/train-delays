package traindelays.networkrail.movementdata

import cats.effect.IO
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class MovementMessageHandlerTest extends FlatSpec with TestFeatures {

  it should "receive activation message and put onto activation queue" in {

    withQueues { queues =>
      val fixture = setUpClientAndHandler(queues)
      fixture.mockStompClient.sendMessage("test/topic", sampleActivationMovementMessage)
      fixture.messageHandlerStream.compile.drain.unsafeRunTimed(2 seconds)
      fixture.movementMessageHandlerWatcher.rawMessagesReceived should have size 1
      val queueMessages = queues.trainActivationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList
      queueMessages should have size 1
      queueMessages.head shouldBe TrainActivationRecord(ScheduleTrainId("G73773"),
                                                        ServiceCode("24747000"),
                                                        TrainId("872A47MT14"),
                                                        StanoxCode("87980"),
                                                        1515949140000L)
    }
  }

  it should "receive multiple movement messages and put onto movement queue" in {

    withQueues { queues =>
      val fixture = setUpClientAndHandler(queues)
      fixture.mockStompClient.sendMessage("test/topic", sampleMovementMessage)
      fixture.messageHandlerStream.compile.drain.unsafeRunTimed(2 seconds)
      fixture.movementMessageHandlerWatcher.rawMessagesReceived should have size 1
      val queueMessages = queues.trainMovementQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList
      queueMessages should have size 2

      queueMessages.head shouldBe TrainMovementRecord(
        TrainId("172N37MX30"),
        ServiceCode("11810920"),
        EventType.Departure,
        TOC("23"),
        1514663100000L,
        Some(1514663100000L),
        Some(1514663100000L),
        Some(StanoxCode("24799")),
        Some(VariationStatus.OnTime)
      )

      queueMessages(1) shouldBe TrainMovementRecord(
        TrainId("172N37MX30"),
        ServiceCode("11800920"),
        EventType.Departure,
        TOC("23"),
        1514661100000L,
        Some(1514661100000L),
        Some(1514663100000L),
        Some(StanoxCode("24780")),
        Some(VariationStatus.OnTime)
      )
    }
  }

  it should "receive cancellation message and put onto cancellation queue" in {

    withQueues { queues =>
      val fixture = setUpClientAndHandler(queues)
      fixture.mockStompClient.sendMessage("test/topic", sampleCancellationMovementMessage)
      fixture.messageHandlerStream.compile.drain.unsafeRunTimed(2 seconds)
      fixture.movementMessageHandlerWatcher.rawMessagesReceived should have size 1
      val queueMessages = queues.trainCancellationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList
      queueMessages should have size 1
      queueMessages.head shouldBe TrainCancellationRecord(TrainId("871B26MK24"),
                                                          ServiceCode("22721000"),
                                                          TOC("88"),
                                                          StanoxCode("87701"),
                                                          CancellationType.EnRoute,
                                                          "YI")
    }
  }

  def sampleActivationMovementMessage =
    Source.fromResource("sample-movement-activation-message.json").getLines().mkString

  def sampleCancellationMovementMessage =
    Source.fromResource("sample-movement-cancellation-message.json").getLines().mkString

  def sampleMovementMessage =
    Source.fromResource("sample-movement-message.json").getLines().mkString

  def setUpClientAndHandler(queues: Queues): Fixture = {
    val mockStompClient = MockStompClient()
    val listener =
      new MovementMessageHandlerWatcher(config.networkRailConfig,
                                        queues.trainMovementQueue,
                                        queues.trainActivationQueue,
                                        queues.trainCancellationQueue)

    val handler =
      MovementMessageHandler(
        config.networkRailConfig,
        listener.messageQueue,
        queues.trainMovementQueue,
        queues.trainActivationQueue,
        queues.trainCancellationQueue,
        mockStompClient.client
      )

    mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()
    Fixture(mockStompClient, listener, handler)
  }

  case class Fixture(mockStompClient: MockStompClient,
                     movementMessageHandlerWatcher: MovementMessageHandlerWatcher,
                     messageHandlerStream: fs2.Stream[IO, Unit])
}
