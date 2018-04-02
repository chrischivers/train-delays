package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.TestFeatures
import traindelays.networkrail.movementdata._

import scala.concurrent.ExecutionContext.Implicits.global

class CancellationLogTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve a cancellation movement log record from the database" in {

    val cancellationLog = createCancellationLog()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.cancellationLogTable.safeAddRecord(cancellationLog).unsafeRunSync()
      val retrievedRecords = fixture.cancellationLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe cancellationLog.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted cancellation log records from the database" in {

    val cancellationLogRecord1 = createCancellationLog()
    val cancellationLogRecord2 = createCancellationLog().copy(trainId = TrainId("862F60MY31"))

    withInitialState(testDatabaseConfig)(
      AppInitialState(cancellationLogs = List(cancellationLogRecord1, cancellationLogRecord2))) { fixture =>
      val retrievedRecords = fixture.cancellationLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe cancellationLogRecord1.copy(id = Some(1))
      retrievedRecords(1) shouldBe cancellationLogRecord2.copy(id = Some(2))
    }

  }

}
