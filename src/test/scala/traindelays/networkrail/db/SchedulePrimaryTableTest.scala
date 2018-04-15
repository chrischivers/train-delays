package traindelays.networkrail.db

import java.nio.file.Paths
import java.time.LocalTime

import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType._
import traindelays.networkrail.scheduledata._
import traindelays.networkrail.{CRS, StanoxCode, TestFeatures, TipLocCode}
import traindelays.ScheduleDataConfig
import traindelays.networkrail.scheduledata.DaysRunPattern.{Saturdays, Weekdays}

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

class SchedulePrimaryTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve an inserted schedule log record from the database (single insertion)" in {

    val scheduleLogRecord = createScheduleRecordPrimary()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.schedulePrimaryTable.safeAddRecord(scheduleLogRecord).unsafeRunSync()
      val retrievedRecords = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe scheduleLogRecord.copy(id = Some(1))
    }

  }

  it should "insert a schedule record and retrieve a schedule log record from the database" in {

    val scheduleRecord = createDecodedScheduleCreateRecord()

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecords = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
      retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
    }
  }

  it should "delete all schedule log records from the database" in {

    val scheduleLogRecord1 = createScheduleRecordPrimary()
    val scheduleLogRecord2 = createScheduleRecordPrimary(scheduleTrainId = ScheduleTrainId("A87532"))

    withInitialState(testDatabaseConfig)(
      AppInitialState(schedulePrimaryRecords = List(scheduleLogRecord1, scheduleLogRecord2))
    ) { fixture =>
      val retrievedRecord1 = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 2
      fixture.schedulePrimaryTable.deleteAllRecords().unsafeRunSync()
      val retrievedRecord2 = fixture.schedulePrimaryTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "delete a schedule log record from the database by id, start date and stpIndicator" in {

    val scheduleLogRecord = createScheduleRecordPrimary()

    withInitialState(testDatabaseConfig)(
      AppInitialState(schedulePrimaryRecords = List(scheduleLogRecord))
    ) { fixture =>
      val retrievedRecord1 = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 1
      fixture.schedulePrimaryTable
        .deleteRecord(scheduleLogRecord.scheduleTrainId,
                      scheduleLogRecord.scheduleStart,
                      scheduleLogRecord.stpIndicator)
        .unsafeRunSync()
      val retrievedRecord2 = fixture.schedulePrimaryTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "retrieve inserted records from the database by id" in {

    val scheduleRecord = createDecodedScheduleCreateRecord()

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecord = fixture.schedulePrimaryTable.retrieveRecordBy(2).unsafeRunSync()
      retrievedRecord.get.stanoxCode shouldBe StanoxCode("23456")
    }

  }

  it should "retrieve inserted records from the database by from, to and pattern" in {

    val slr1 = ScheduleLocationRecord(OriginatingLocation,
                                      TipLocCode("REIGATE"),
                                      None,
                                      Some(LocalTime.parse("0649", timeFormatter)))
    val slr2 = ScheduleLocationRecord(TerminatingLocation,
                                      TipLocCode("REDHILL"),
                                      Some(LocalTime.parse("0653", timeFormatter)),
                                      None)
    val slr3 = ScheduleLocationRecord(TerminatingLocation,
                                      TipLocCode("MERSTHAM"),
                                      Some(LocalTime.parse("0659", timeFormatter)),
                                      None)

    val scheduleRecord = createDecodedScheduleCreateRecord(locationRecords = List(slr1, slr2, slr3))

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None),
      StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode("34567")), Some(CRS("MER")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrieved1 = fixture.schedulePrimaryTable
        .retrieveScheduleRecordsFor(StanoxCode("23456"), StanoxCode("34567"), Weekdays, StpIndicator.P)
        .unsafeRunSync()
      retrieved1 should have size 1
      retrieved1.head.stanoxCode shouldBe StanoxCode("23456")
      retrieved1.head.subsequentStanoxCodes shouldBe List(StanoxCode("34567"))

      val retrieved2 = fixture.schedulePrimaryTable
        .retrieveScheduleRecordsFor(StanoxCode("12345"), StanoxCode("34567"), Weekdays, StpIndicator.P)
        .unsafeRunSync()
      retrieved2 should have size 1
      retrieved2.head.stanoxCode shouldBe StanoxCode("12345")
      retrieved2.head.subsequentStanoxCodes shouldBe List(StanoxCode("23456"), StanoxCode("34567"))

      val retrieved3 = fixture.schedulePrimaryTable
        .retrieveScheduleRecordsFor(StanoxCode("12345"), StanoxCode("34567"), Saturdays, StpIndicator.P)
        .unsafeRunSync()
      retrieved3 should have size 0
    }
  }

  it should "retrieve distinct stanox codes from the DB" in {

    val scheduleRecord1 = createDecodedScheduleCreateRecord()
    val scheduleRecord2 = createDecodedScheduleCreateRecord(scheduleTrainId = ScheduleTrainId("5653864"))

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord1, stanoxRecordsToMap(stanoxRecords)) ++ toScheduleLogs(
          scheduleRecord2,
          stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val distinctStanoxCodes = fixture.schedulePrimaryTable.retrieveAllDistinctStanoxCodes.unsafeRunSync()
      distinctStanoxCodes should have size 2
      distinctStanoxCodes should contain theSameElementsAs List(StanoxCode("12345"), StanoxCode("23456"))
    }

  }

  it should "retrieve multiple inserted records from the database" in {

    val scheduleRecord1 = createDecodedScheduleCreateRecord()
    val scheduleRecord2 = createDecodedScheduleCreateRecord().copy(scheduleTrainId = ScheduleTrainId("123456"))

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord1, stanoxRecordsToMap(stanoxRecords)) ++ toScheduleLogs(
          scheduleRecord2,
          stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecords = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 4
      retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
      retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
      retrievedRecords(2).stanoxCode shouldBe StanoxCode("12345")
      retrievedRecords(3).stanoxCode shouldBe StanoxCode("23456")
    }

  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val scheduleRecord = createDecodedScheduleCreateRecord()

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("1234")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("4567")), Some(CRS("RED")), None)
    )

    withInitialState(
      testDatabaseConfig,
      scheduleDataConfig = ScheduleDataConfig(List(Uri.unsafeFromString("")),
                                              List(Uri.unsafeFromString("")),
                                              Paths.get(""),
                                              Paths.get(""),
                                              2 seconds)
    )(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecords1 = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()

      retrievedRecords1 should have size 2
      retrievedRecords1.head.stanoxCode shouldBe StanoxCode("1234")
      retrievedRecords1(1).stanoxCode shouldBe StanoxCode("4567")

      fixture.schedulePrimaryTable.deleteAllRecords().unsafeRunSync()

      val retrievedRecords2 = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords2 should have size 2

      Thread.sleep(2000)

      val retrievedRecords3 = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords3 should have size 0
    }
  }

  it should "delete all records from the database" in {

    val scheduleRecord = createDecodedScheduleCreateRecord()

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecords = (for {
        _         <- fixture.schedulePrimaryTable.deleteAllRecords()
        retrieved <- fixture.schedulePrimaryTable.retrieveAllRecords()
      } yield retrieved).unsafeRunSync()

      retrievedRecords should have size 0
    }

  }

  it should "preserve order of location records when retrieved from DB" in {

    val slr1 = ScheduleLocationRecord(OriginatingLocation,
                                      TipLocCode("REIGATE"),
                                      None,
                                      Some(LocalTime.parse("0649", timeFormatter)))
    val slr2 = ScheduleLocationRecord(TerminatingLocation,
                                      TipLocCode("REDHILL"),
                                      Some(LocalTime.parse("0653", timeFormatter)),
                                      None)
    val slr3 = ScheduleLocationRecord(TerminatingLocation,
                                      TipLocCode("MERSTHAM"),
                                      Some(LocalTime.parse("0659", timeFormatter)),
                                      None)

    val scheduleRecord = createDecodedScheduleCreateRecord(locationRecords = List(slr1, slr2, slr3))

    val stanoxRecords = List(
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None),
      StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode("3456")), Some(CRS("MER")), None),
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = stanoxRecords,
        schedulePrimaryRecords = toScheduleLogs(scheduleRecord, stanoxRecordsToMap(stanoxRecords))
      )) { fixture =>
      val retrievedRecords = fixture.schedulePrimaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 3
      retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
      retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
      retrievedRecords(2).stanoxCode shouldBe StanoxCode("3456")
    }

  }
}
