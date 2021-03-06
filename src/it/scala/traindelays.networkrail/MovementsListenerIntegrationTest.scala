//package traindelays.networkrail
//
//import java.nio.file.Paths
//
//import org.scalactic.TripleEqualsSupport
//import org.scalatest.concurrent.Eventually
//import org.scalatest.{BeforeAndAfterEach, FlatSpec}
//import traindelays.stomp.{StompClient, StompStreamListener}
//
//import scala.concurrent.duration._
//import scala.language.postfixOps
//
//class MovementsListenerIntegrationTest
//    extends FlatSpec
//    with IntegrationTest
//    with TripleEqualsSupport
//    with BeforeAndAfterEach
//    with Eventually
//    with TestFeatures {
//
//  override implicit val patienceConfig: PatienceConfig =
//    PatienceConfig(timeout = scaled(30 seconds), interval = scaled(2 seconds))
//
//  //TODO fix
////  it should "subscribe to a topic and receive updates" in {
////
////    withQueues { queues =>
////      val movementWatcher =
////        new MovementMessageHandlerWatcher(queues.trainMovementQueue,
////                                          queues.trainActivationQueue,
////                                          queues.trainCancellationQueue)
////      subscribeToMovementsTopic(movementWatcher)
////
////      eventually {
////        movementWatcher.rawMessagesReceived.size shouldBe >(0)
////        parse(movementWatcher.rawMessagesReceived.head).right.get
////          .as[List[TrainMovementRecord]]
////          .right
////          .get
////          .size shouldBe >(0)
////        queues.trainMovementQueue.dequeueBatch1(3).unsafeRunSync().toList should have size 3
////      }
////    }
////  }
//
//  //TODO fix
////  it should "persist movement records where all details exist to DB" in {
////
////    withInitialState(testconfig.databaseConfig, scheduleDataConfig = testconfig.networkRailConfig.scheduleData)(
////      AppInitialState.empty) { fixture =>
////      withQueues { queues =>
////        val movementWatcher =
////          new MovementMessageHandlerWatcher(queues.trainMovementQueue,
////                                            queues.trainActivationQueue,
////                                            queues.trainCancellationQueue)
////        val emailer = Emailer(testconfig.emailerConfig)
////        val subscriberFetcher =
////          SubscriberHandler(fixture.movementLogTable,
////                            fixture.subscriberTable,
////                            fixture.scheduleTable,
////                            fixture.stanoxTable,
////                            emailer)
////        subscribeToMovementsTopic(movementWatcher)
////
////        TrainMovementProcessor(queues.trainMovementQueue,
////                               fixture.movementLogTable,
////                               subscriberFetcher,
////                               fixture.trainActivationCache).stream.run
////          .unsafeRunTimed(10 seconds)
////
////        fixture.movementLogTable
////          .retrieveAllRecords()
////          .map { retrievedRecords =>
////            retrievedRecords.size shouldBe >(0)
////          }
////          .unsafeRunSync()
////      }
////    }
////  }
//
////  it should "persist movement to db and surface them in watching report" in {
////
////    withInitialState(testconfig.databaseConfig)(AppInitialState.empty) { fixture =>
////      withQueues
////        .map {
////          case (trainMovementQueue, trainActivationQueue) =>
////            val movementWatcher   = new MovementMessageHandlerWatcher(trainMovementQueue, trainActivationQueue)
////            val emailer           = Emailer(testconfig.emailerConfig)
////            val subscriberFetcher = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
////            subscribeToMovementsTopic(movementWatcher)
////
////            TrainMovementProcessor(trainMovementQueue,
////                                   fixture.movementLogTable,
////                                   subscriberFetcher,
////                                   fixture.trainActivationCache).stream.run.unsafeRunTimed(20 seconds)
////
////            val movementsRecords =
////              parse(movementWatcher.rawMessagesReceived.head).right.get.as[List[TrainMovements]].right.get
////            //TODO this needs to be mocked
////            val (trainMovementRecords, _) = movementsRecords.partition(_.isInstanceOf[TrainMovementRecord])
////            val movementLog = movementRecordToMovementLog(Random.shuffle(trainMovementRecords).head.asInstanceOf[TrainMovementRecord], None, )
////
////            val userId = UserId(UUID.randomUUID().toString)
////            val email  = "test@test.com"
////            val watchingRecord =
////              SubscriberRecord(None, userId, email, movementLog.trainId, movementLog.serviceCode, movementLog.stanox)
////            fixture.subscriberTable.addRecord(watchingRecord).unsafeRunSync()
////
////            val reportsRetrieved = subscriberFetcher.generateSubscriberReports.unsafeRunSync()
////            val reportsForUsers  = reportsRetrieved.filter(_.subscriberRecord.userId == userId)
////            reportsForUsers should have size 1
////            reportsForUsers.head.subscriberRecord.copy(id = None) shouldBe watchingRecord
////            reportsForUsers.head.movementLogs.size shouldBe >(0)
////            reportsForUsers.head.movementLogs.map(_.copy(id = None)) should contain(movementLog)
////        }
////    }
////  }
//
//  override protected def afterEach(): Unit =
//    cleanUpFile(testconfig.networkRailConfig.scheduleData.tmpUnzipLocation.toString)
//
//  private def cleanUpFile(location: String) =
//    Paths.get(location).toFile.delete()
//
//  private def subscribeToMovementsTopic(movementWatcher: StompStreamListener) = {
//    val stompClient = StompClient(testconfig.networkRailConfig)
//    stompClient
//      .subscribe(testconfig.networkRailConfig.movements.topic, movementWatcher)
//      .unsafeRunSync()
//
//  }
//}
