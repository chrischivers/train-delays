package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.{ServiceCode, StanoxCode, TOC}
import traindelays.{DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class CancellationLogTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a cancellation log record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.cancellationLogTable.addRecord(getCancellationLog())
    }
  }

  it should "retrieve an cancellation movement log record from the database" in {

    val cancellationLog = getCancellationLog()

    withInitialState(config)(AppInitialState(cancellationLogs = List(cancellationLog))) { fixture =>
      val retrievedRecords = fixture.cancellationLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe cancellationLog.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted cancellation log records from the database" in {

    val cancellationLogRecord1 = getCancellationLog()
    val cancellationLogRecord2 = getCancellationLog().copy(trainId = TrainId("862F60MY31"))

    withInitialState(config)(AppInitialState(cancellationLogs = List(cancellationLogRecord1, cancellationLogRecord2))) {
      fixture =>
        val retrievedRecords = fixture.cancellationLogTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords should have size 2
        retrievedRecords.head shouldBe cancellationLogRecord1.copy(id = Some(1))
        retrievedRecords(1) shouldBe cancellationLogRecord2.copy(id = Some(2))
    }

  }

  def getCancellationLog(trainId: TrainId = TrainId("862F60MY30"),
                         scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G12345"),
                         serviceCode: ServiceCode = ServiceCode("24673605"),
                         toc: TOC = TOC("SN"),
                         stanoxCode: StanoxCode = StanoxCode("87214"),
                         cancellationType: CancellationType = EnRoute,
                         cancellationReasonCode: String = "YI") =
    CancellationLog(
      None,
      trainId,
      scheduleTrainId,
      serviceCode,
      toc,
      stanoxCode,
      cancellationType,
      cancellationReasonCode
    )

}
