package traindelays.networkrail.watching

import java.util.UUID

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
import scala.concurrent.ExecutionContext.Implicits.global

class WatchingCheckerTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "produce report for a single watched route and single movement record" in {

    val movementRecord = createMovementRecord()
    val userID         = UUID.randomUUID().toString
    val trainID        = movementRecord.trainId
    val serviceCode    = movementRecord.trainServiceCode
    val stanox         = movementRecord.stanox.get
    val watchedRecord  = WatchingRecord(None, userID, trainID, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(watchingRecords = List(watchedRecord), movementLogs = List(movementRecord.toMovementLog.get))) {
      fixture =>
        val watchingChecker = WatchingChecker(fixture.movementLogTable, fixture.watchingTable)
        watchingChecker.generateWatchingReports.map { retrieved =>
          retrieved should have size 1
          retrieved.head.watchingRecord shouldBe watchedRecord.copy(id = Some(1))
          retrieved.head.movementLogs should have size 1
          retrieved.head.movementLogs.head shouldBe movementRecord.toMovementLog.get.copy(id = Some(1))

        }
    }
  }

  it should "produce report for a single watched route and multiple movement records" in {

    val movementRecord1 = createMovementRecord()
    val movementRecord2 = createMovementRecord(eventType = Some("DEPARTURE"))
    val movementRecord3 = createMovementRecord(trainId = "ANOTHER_ID")
    val userID          = UUID.randomUUID().toString
    val trainID         = movementRecord1.trainId
    val serviceCode     = movementRecord1.trainServiceCode
    val stanox          = movementRecord1.stanox.get
    val watchedRecord   = WatchingRecord(None, userID, trainID, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(
        watchingRecords = List(watchedRecord),
        movementLogs =
          List(movementRecord1.toMovementLog.get, movementRecord3.toMovementLog.get, movementRecord2.toMovementLog.get)
      )) { fixture =>
      val watchingChecker = WatchingChecker(fixture.movementLogTable, fixture.watchingTable)
      watchingChecker.generateWatchingReports.map { retrieved =>
        retrieved should have size 1
        retrieved.head.watchingRecord shouldBe watchedRecord.copy(id = Some(1))
        retrieved.head.movementLogs should have size 2
        retrieved.head.movementLogs.head shouldBe movementRecord1.toMovementLog.get.copy(id = Some(1))
        retrieved.head.movementLogs(1) shouldBe movementRecord2.toMovementLog.get.copy(id = Some(3))

      }
    }
  }
}
