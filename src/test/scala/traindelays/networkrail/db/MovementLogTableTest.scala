package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.{StanoxCode, TestFeatures}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.ExecutionContext.Implicits.global

class MovementLogTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve an inserted movement log record from the database" in {

    val movementLog = createMovementLog()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.movementLogTable.addRecord(movementLog).unsafeRunSync()
      val retrievedRecords = fixture.movementLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe movementLog.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted movement log records from the database" in {

    val movementLogRecord1 = createMovementLog()
    val movementLogRecord2 = createMovementLog().copy(trainId = TrainId("862F60MY31"))

    withInitialState(testDatabaseConfig)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) {
      fixture =>
        val retrievedRecords = fixture.movementLogTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords should have size 2
        retrievedRecords.head shouldBe movementLogRecord1.copy(id = Some(1))
        retrievedRecords(1) shouldBe movementLogRecord2.copy(id = Some(2))
    }

  }

  it should "retrieve inserted movement log records from the database for a schedule train ID" in {

    val scheduleTrainId    = ScheduleTrainId("ABC123")
    val movementLogRecord1 = createMovementLog().copy(scheduleTrainId = scheduleTrainId)
    val movementLogRecord2 = createMovementLog()

    withInitialState(testDatabaseConfig)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) {
      fixture =>
        val retrievedRecords =
          fixture.movementLogTable.retrieveRecordsFor(scheduleTrainId, None, None, None).unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe movementLogRecord1.copy(id = Some(1))
    }
  }

  it should "retrieve inserted movement log records from the database for a schedule train ID and stanox" in {

    val scheduleTrainId    = ScheduleTrainId("ABC123")
    val stanoxCode         = StanoxCode("67232")
    val movementLogRecord1 = createMovementLog().copy(scheduleTrainId = scheduleTrainId, stanoxCode = stanoxCode)
    val movementLogRecord2 = createMovementLog().copy(scheduleTrainId = scheduleTrainId)

    withInitialState(testDatabaseConfig)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) {
      fixture =>
        val retrievedRecords =
          fixture.movementLogTable
            .retrieveRecordsFor(scheduleTrainId, Some(List(stanoxCode)), None, None)
            .unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe movementLogRecord1.copy(id = Some(1))
    }
  }

  it should "retrieve inserted movement log records from the database for a given origin time period" in {

    val scheduleTrainId = ScheduleTrainId("ABC123")
    val movementLogRecord1 =
      createMovementLog().copy(scheduleTrainId = scheduleTrainId, originDepartureTimestamp = 1000L)
    val movementLogRecord2 =
      createMovementLog().copy(scheduleTrainId = scheduleTrainId, originDepartureTimestamp = 2000L)

    withInitialState(testDatabaseConfig)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) {
      fixture =>
        val retrievedRecords =
          fixture.movementLogTable.retrieveRecordsFor(scheduleTrainId, None, Some(1001L), Some(2001L)).unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe movementLogRecord2.copy(id = Some(2))
    }
  }
}
