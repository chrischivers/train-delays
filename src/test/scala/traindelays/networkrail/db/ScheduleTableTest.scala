package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.DatabaseConfig
import traindelays.networkrail.db.common._
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{ScheduleRecord, _}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableTest extends FlatSpec {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a schedule record into the database" in {

    withScheduleTable(config)(List.empty: _*) { scheduleTable =>
      scheduleTable.addRecord(getTestScheduleRecord())
    }
  }

  it should "retrieve an inserted record from the database" in {

    val scheduleRecord = getTestScheduleRecord()

    val retrievedRecords = withScheduleTable(config)(scheduleRecord) { scheduleTable =>
      scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleRecord
  }

  it should "retrieve multiple inserted records from the database" in {

    val scheduleRecord1 = getTestScheduleRecord()
    val scheduleRecord2 = getTestScheduleRecord().copy(trainUid = "123456")

    val retrievedRecords = withScheduleTable(config)(scheduleRecord1, scheduleRecord2) { scheduleTable =>
      scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe scheduleRecord1
    retrievedRecords(1) shouldBe scheduleRecord2
  }

  def getTestScheduleRecord(trainUid: String = "G76481",
                            trainServiceCode: String = "24745000",
                            daysRun: DaysRun = DaysRun(monday = true,
                                                       tuesday = true,
                                                       wednesday = true,
                                                       thursday = true,
                                                       friday = true,
                                                       saturday = false,
                                                       sunday = false),
                            scheduleStartDate: LocalDate = LocalDate.parse("2017-12-11"),
                            scheduleEndDate: LocalDate = LocalDate.parse("2017-12-29"),
                            locationRecords: List[ScheduleLocationRecord] = List(
                              ScheduleLocationRecord("LO",
                                                     "REIGATE",
                                                     None,
                                                     Some(LocalTime.parse("0649", timeFormatter))),
                              ScheduleLocationRecord("LT",
                                                     "REDHILL",
                                                     Some(LocalTime.parse("0653", timeFormatter)),
                                                     None)
                            )) =
    ScheduleRecord(
      trainUid,
      trainServiceCode,
      daysRun,
      scheduleStartDate,
      scheduleEndDate,
      locationRecords
    )

}
