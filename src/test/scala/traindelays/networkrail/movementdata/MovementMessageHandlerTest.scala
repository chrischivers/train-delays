package traindelays.networkrail.movementdata

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.TestFeatures
import traindelays.metrics.MetricsLogging
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.{MockStompClient, ServiceCode, StanoxCode, TOC}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

class MovementMessageHandlerTest extends FlatSpec with TestFeatures {

  it should "receive activation message and put onto activation queue" in {

    withQueues { queues =>
      val (mockStompClient, listener, handler) = setUpClientAndHandler(queues)
      mockStompClient.sendMessage("test/topic", sampleActivationMovementMessage)
      handler.run.unsafeRunTimed(2 seconds)
      listener.rawMessagesReceived should have size 1
      val queueMessages = queues.trainActivationQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList
      queueMessages should have size 1
      queueMessages.head shouldBe TrainActivationRecord(ScheduleTrainId("G73773"),
                                                        ServiceCode("24747000"),
                                                        TrainId("872A47MT14"),
                                                        StanoxCode("87980"),
                                                        1515949140000L)
    }
  }

  it should "receive movement message and put onto movement queue" in {

    withQueues { queues =>
      val (mockStompClient, listener, handler) =
        setUpClientAndHandler(queues)
      mockStompClient.sendMessage("test/topic", sampleMovementMessage)
      handler.run.unsafeRunTimed(2 seconds)
      listener.rawMessagesReceived should have size 1
      val queueMessages = queues.trainMovementQueue.dequeueBatch1(Integer.MAX_VALUE).unsafeRunSync().toList
      queueMessages should have size 1

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
    }
  }

  it should "receive cancellation message and put onto cancellation queue" in {

    withQueues { queues =>
      val (mockStompClient, listener, handler) =
        setUpClientAndHandler(queues)
      mockStompClient.sendMessage("test/topic", sampleCancellationMovementMessage)
      handler.run.unsafeRunTimed(2 seconds)
      listener.rawMessagesReceived should have size 1
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

  def setUpClientAndHandler(queues: Queues) = {
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
        MetricsLogging(config.metricsConfig),
        mockStompClient.client
      )

    mockStompClient.client.subscribe("test/topic", listener).unsafeRunSync()
    (mockStompClient, listener, handler)
  }
}
