package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.DatabaseConfig
import traindelays.networkrail.db.common._
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{ScheduleRecord, _}

import scala.concurrent.ExecutionContext.Implicits.global

class TipLocTableTest extends FlatSpec {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a tiploc record into the database" in {

    withTiplocTable(config)(List.empty: _*) { tipLocTable =>
      tipLocTable.addRecord(getTipLocRecord())
    }
  }

  it should "retrieve an inserted tiploc record from the database" in {

    val tipLocRecord = getTipLocRecord()

    val retrievedRecords = withTiplocTable(config)(tipLocRecord) { tipLocTable =>
      tipLocTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe tipLocRecord
  }

  it should "retrieve multiple inserted tiploc records from the database" in {

    val tipLocRecord1 = getTipLocRecord()
    val tipLocRecord2 = getTipLocRecord().copy(tipLocCode = "REIGATE", description = "REIGATE_DESC")

    val retrievedRecords = withTiplocTable(config)(tipLocRecord1, tipLocRecord2) { tipLocTable =>
      tipLocTable.retrieveAllRecords()
    }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe tipLocRecord1
    retrievedRecords(1) shouldBe tipLocRecord2
  }

  def getTipLocRecord(tipLocCode: String = "REDHILL", description: String = "REDHILL") =
    TipLocRecord(tipLocCode, description)

}
