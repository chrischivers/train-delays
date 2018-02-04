package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.{ServiceCode, StanoxCode, TOC}
import traindelays.{DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class MovementLogTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert and retrieve an inserted movement log record from the database" in {

    val movementLog = createMovementLog()

    withInitialState(config)() { fixture =>
      fixture.movementLogTable.addRecord(movementLog).unsafeRunSync()
      val retrievedRecords = fixture.movementLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe movementLog.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted movement log records from the database" in {

    val movementLogRecord1 = createMovementLog()
    val movementLogRecord2 = createMovementLog().copy(trainId = TrainId("862F60MY31"))

    withInitialState(config)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) { fixture =>
      val retrievedRecords = fixture.movementLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe movementLogRecord1.copy(id = Some(1))
      retrievedRecords(1) shouldBe movementLogRecord2.copy(id = Some(2))
    }

  }
}
