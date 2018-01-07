package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{ScheduleRecord, _}
import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a schedule record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.scheduleTable.addRecord(getTestScheduleRecord())
    }
  }

  it should "retrieve an inserted record from the database" in {

    val scheduleRecord = getTestScheduleRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(scheduleRecords = List(scheduleRecord))) {
      fixture =>
        fixture.scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleRecord
  }

  it should "delete all records from the database" in {

    val scheduleRecord = getTestScheduleRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(scheduleRecords = List(scheduleRecord))) {
      fixture =>
        for {
          _         <- fixture.scheduleTable.deleteAllRecords()
          retrieved <- fixture.scheduleTable.retrieveAllRecords()
        } yield retrieved
    }

    retrievedRecords should have size 0
  }

  it should "retrieve multiple inserted records from the database" in {

    val scheduleRecord1 = getTestScheduleRecord()
    val scheduleRecord2 = getTestScheduleRecord().copy(trainUid = "123456")

    val retrievedRecords =
      withInitialState(config)(AppInitialState(scheduleRecords = List(scheduleRecord1, scheduleRecord2))) { fixture =>
        fixture.scheduleTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe scheduleRecord1
    retrievedRecords(1) shouldBe scheduleRecord2
  }

  it should "preserve order of location records when retrieved from DB" in {

    val slr1 = ScheduleLocationRecord("LO", "REIGATE", None, Some(LocalTime.parse("0649", timeFormatter)))
    val slr2 = ScheduleLocationRecord("LT", "REDHILL", Some(LocalTime.parse("0653", timeFormatter)), None)
    val slr3 = ScheduleLocationRecord("LT", "MERSTHAM", Some(LocalTime.parse("0659", timeFormatter)), None)

    val scheduleRecord = getTestScheduleRecord(locationRecords = List(slr1, slr2, slr3))

    val retrievedRecords =
      withInitialState(config)(AppInitialState(scheduleRecords = List(scheduleRecord))) { fixture =>
        fixture.scheduleTable.retrieveAllRecords()
      }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleRecord

  }

  def getTestScheduleRecord(trainUid: String = "G76481",
                            trainServiceCode: String = "24745000",
                            atocCode: String = "SN",
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
      atocCode,
      daysRun,
      scheduleStartDate,
      scheduleEndDate,
      locationRecords
    )

}
