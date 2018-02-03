package traindelays.networkrail.db

import java.nio.file.Paths
import java.time.LocalTime

import cats.effect.IO
import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern.{Saturdays, Weekdays}
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType._
import traindelays.networkrail.scheduledata._
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}
import traindelays.{DatabaseConfig, ScheduleDataConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert and retrieve an inserted schedule log record from the database (single insertion)" in {

    val scheduleLogRecord = createScheduleLogRecord()

    val retrievedRecords = withInitialState(config)() { fixture =>
      fixture.scheduleTable.addRecord(scheduleLogRecord).unsafeRunSync()
      fixture.scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleLogRecord.copy(id = Some(1))
  }

  it should "insert and retrieve an inserted schedule log record from the database (multiple insertion)" in {

    val log1 = createScheduleLogRecord()
    val log2 = createScheduleLogRecord(scheduleTrainId = ScheduleTrainId("98742"))

    val retrievedRecords = withInitialState(config)() { fixture =>
      fixture.scheduleTable.addRecords(List(log1, log2)).unsafeRunSync()
      fixture.scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe log1.copy(id = Some(1))
    retrievedRecords(1) shouldBe log2.copy(id = Some(2))
  }

  it should "insert a schedule record and retrieve a schedule log record from the database" in {

    val scheduleRecord = createScheduleRecord()

    val retrievedRecords = withInitialState(config)(
      AppInitialState(
        stanoxRecords = List(
          StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
          StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
        ),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      fixture.scheduleTable.retrieveAllRecords()
    }

    retrievedRecords should have size 2
    retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
    retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
  }

  it should "retrieve inserted records from the database by id" in {

    val scheduleRecord = createScheduleRecord()

    val retrievedRecord =
      withInitialState(config)(
        AppInitialState(
          stanoxRecords = List(
            StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
            StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
          ),
          scheduleRecords = List(scheduleRecord)
        )) { fixture =>
        fixture.scheduleTable.retrieveRecordBy(2)
      }

    retrievedRecord.get.stanoxCode shouldBe StanoxCode("23456")
  }

  ignore should "retrieve inserted records from the database by from, to and pattern" in {

    //TODO sql statement does not work on H2. Ignored.

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

    val scheduleRecord = createScheduleRecord(locationRecords = List(slr1, slr2, slr3))

    withInitialState(config)(
      AppInitialState(
        stanoxRecords = List(
          StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
          StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None),
          StanoxRecord(StanoxCode("34567"), TipLocCode("MERSTHAM"), Some(CRS("MER")), None)
        ),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      IO {
        val retrieved1 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(StanoxCode("23456"), StanoxCode("34567"), Weekdays)
          .unsafeRunSync()
        retrieved1 should have size 1
        retrieved1.head.stanoxCode shouldBe StanoxCode("23456")
        retrieved1.head.subsequentStanoxCodes shouldBe List(StanoxCode("34567"))

        val retrieved2 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(StanoxCode("12345"), StanoxCode("34567"), Weekdays)
          .unsafeRunSync()
        retrieved2 should have size 1
        retrieved2.head.stanoxCode shouldBe StanoxCode("12345")
        retrieved2.head.subsequentStanoxCodes shouldBe List(StanoxCode("23456"), StanoxCode("34567"))

        val retrieved3 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(StanoxCode("12345"), StanoxCode("34567"), Saturdays)
          .unsafeRunSync()
        retrieved3 should have size 0
      }
    }
  }

  it should "retrieve distinct stanox codes from the DB" in {

    val scheduleRecord1 = createScheduleRecord()
    val scheduleRecord2 = createScheduleRecord(scheduleTrainId = ScheduleTrainId("5653864"))

    val distinctStanoxCodes =
      withInitialState(config)(
        AppInitialState(
          stanoxRecords = List(
            StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
            StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
          ),
          scheduleRecords = List(scheduleRecord1, scheduleRecord2)
        )) { fixture =>
        fixture.scheduleTable.retrieveAllDistinctStanoxCodes
      }

    distinctStanoxCodes should have size 2
    distinctStanoxCodes should contain theSameElementsAs List(StanoxCode("12345"), StanoxCode("23456"))
  }

  it should "retrieve multiple inserted records from the database" in {

    val scheduleRecord1 = createScheduleRecord()
    val scheduleRecord2 = createScheduleRecord().copy(scheduleTrainId = ScheduleTrainId("123456"))

    val retrievedRecords =
      withInitialState(config)(
        AppInitialState(
          stanoxRecords = List(
            StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
            StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
          ),
          scheduleRecords = List(scheduleRecord1, scheduleRecord2)
        )) { fixture =>
        fixture.scheduleTable.retrieveAllRecords()
      }

    retrievedRecords should have size 4
    retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
    retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
    retrievedRecords(2).stanoxCode shouldBe StanoxCode("12345")
    retrievedRecords(3).stanoxCode shouldBe StanoxCode("23456")
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val scheduleRecord = createScheduleRecord()

    withInitialState(config,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 2 seconds))(
      AppInitialState(
        stanoxRecords = List(
          StanoxRecord(StanoxCode("1234"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
          StanoxRecord(StanoxCode("4567"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
        ),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      IO {
        val retrievedRecords1 = fixture.scheduleTable.retrieveAllRecords().unsafeRunSync()

        retrievedRecords1 should have size 2
        retrievedRecords1.head.stanoxCode shouldBe StanoxCode("1234")
        retrievedRecords1(1).stanoxCode shouldBe StanoxCode("4567")

        fixture.scheduleTable.deleteAllRecords().unsafeRunSync()

        val retrievedRecords2 = fixture.scheduleTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords2 should have size 2

        Thread.sleep(2000)

        val retrievedRecords3 = fixture.scheduleTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords3 should have size 0
      }
    }
  }

  it should "delete all records from the database" in {

    val scheduleRecord = createScheduleRecord()

    val retrievedRecords = withInitialState(config)(
      AppInitialState(
        stanoxRecords = List(
          StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
          StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
        ),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      for {
        _         <- fixture.scheduleTable.deleteAllRecords()
        retrieved <- fixture.scheduleTable.retrieveAllRecords()
      } yield retrieved
    }

    retrievedRecords should have size 0
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

    val scheduleRecord = createScheduleRecord(locationRecords = List(slr1, slr2, slr3))

    val retrievedRecords =
      withInitialState(config)(
        AppInitialState(
          stanoxRecords = List(
            StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None),
            StanoxRecord(StanoxCode("3456"), TipLocCode("MERSTHAM"), Some(CRS("MER")), None),
            StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
          ),
          scheduleRecords = List(scheduleRecord)
        )) { fixture =>
        fixture.scheduleTable.retrieveAllRecords()
      }

    retrievedRecords should have size 3
    retrievedRecords.head.stanoxCode shouldBe StanoxCode("12345")
    retrievedRecords(1).stanoxCode shouldBe StanoxCode("23456")
    retrievedRecords(2).stanoxCode shouldBe StanoxCode("3456")

  }

}
