package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
import traindelays.networkrail.movementdata.MovementLog
import scala.concurrent.ExecutionContext.Implicits.global

class MovementLogTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a movement log record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.movementLogTable.addRecord(getMovementLog())
    }
  }

  it should "retrieve an inserted movement log record from the database" in {

    val movementLog = getMovementLog()

    val retrievedRecords = withInitialState(config)(AppInitialState(movementLogs = List(movementLog))) { fixture =>
      fixture.movementLogTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe movementLog.copy(id = Some(1))
  }

  it should "retrieve multiple inserted movement log records from the database" in {

    val movementLogRecord1 = getMovementLog()
    val movementLogRecord2 = getMovementLog().copy(trainId = "862F60MY31")

    val retrievedRecords =
      withInitialState(config)(AppInitialState(movementLogs = List(movementLogRecord1, movementLogRecord2))) {
        fixture =>
          fixture.movementLogTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe movementLogRecord1.copy(id = Some(1))
    retrievedRecords(1) shouldBe movementLogRecord2.copy(id = Some(2))
  }

  def getMovementLog(trainId: String = "862F60MY30",
                     serviceCode: String = "24673605",
                     eventType: String = "ARRIVAL",
                     toc: String = "SN",
                     stanox: String = "87214",
                     plannedPassengerTimestamp: Long = 1514663220000L,
                     actualTimestamp: Long = 1514663160000L,
                     variationStatus: String = "EARLY") =
    MovementLog(
      None,
      trainId,
      serviceCode,
      eventType,
      toc,
      stanox,
      plannedPassengerTimestamp,
      actualTimestamp,
      actualTimestamp - plannedPassengerTimestamp,
      variationStatus
    )

}