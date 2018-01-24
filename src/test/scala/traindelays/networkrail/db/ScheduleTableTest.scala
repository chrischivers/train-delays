package traindelays.networkrail.db

import java.nio.file.Paths
import java.time.LocalTime

import cats.effect.IO
import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.Stanox
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType._
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata._
import traindelays.{DatabaseConfig, ScheduleDataConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a schedule log record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.scheduleTable.addRecord(createScheduleLogRecord())
    }
  }

  it should "insert multiple schedule log records into the database" in {

    withInitialState(config)() { fixture =>
      val log1 = createScheduleLogRecord()
      val log2 = createScheduleLogRecord(scheduleTrainId = ScheduleTrainId("98742"))
      fixture.scheduleTable.addRecords(List(log1, log2))
    }
  }

  it should "retrieve an inserted record from the database" in {

    val scheduleRecord = createScheduleRecord()

    val retrievedRecords = withInitialState(config)(
      AppInitialState(
        tiplocRecords = List(TipLocRecord(TipLocCode("REIGATE"), Stanox("REI"), None),
                             TipLocRecord(TipLocCode("REDHILL"), Stanox("RED"), None)),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      fixture.scheduleTable.retrieveAllScheduleRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleRecord
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val scheduleRecord = createScheduleRecord()

    withInitialState(config,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 2 seconds))(
      AppInitialState(
        tiplocRecords = List(TipLocRecord(TipLocCode("REIGATE"), Stanox("REI"), None),
                             TipLocRecord(TipLocCode("REDHILL"), Stanox("RED"), None)),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      IO {
        val retrievedRecords1 = fixture.scheduleTable.retrieveAllScheduleRecords().unsafeRunSync()

        retrievedRecords1 should have size 1
        retrievedRecords1.head shouldBe scheduleRecord

        fixture.scheduleTable.deleteAllRecords().unsafeRunSync()

        val retrievedRecords2 = fixture.scheduleTable.retrieveAllScheduleRecords().unsafeRunSync()
        retrievedRecords2 should have size 1
        retrievedRecords2.head shouldBe scheduleRecord

        Thread.sleep(2000)

        val retrievedRecords3 = fixture.scheduleTable.retrieveAllScheduleRecords().unsafeRunSync()
        retrievedRecords3 should have size 0
      }
    }
  }

  it should "delete all records from the database" in {

    val scheduleRecord = createScheduleRecord()

    val retrievedRecords = withInitialState(config)(
      AppInitialState(
        tiplocRecords = List(TipLocRecord(TipLocCode("REIGATE"), Stanox("REI"), None),
                             TipLocRecord(TipLocCode("REDHILL"), Stanox("RED"), None)),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      for {
        _         <- fixture.scheduleTable.deleteAllRecords()
        retrieved <- fixture.scheduleTable.retrieveAllRecords()
      } yield retrieved
    }

    retrievedRecords should have size 0
  }

  it should "retrieve multiple inserted records from the database" in {

    val scheduleRecord1 = createScheduleRecord()
    val scheduleRecord2 = createScheduleRecord().copy(scheduleTrainId = ScheduleTrainId("123456"))

    val retrievedRecords =
      withInitialState(config)(
        AppInitialState(
          tiplocRecords = List(TipLocRecord(TipLocCode("REIGATE"), Stanox("REI"), None),
                               TipLocRecord(TipLocCode("REDHILL"), Stanox("RED"), None)),
          scheduleRecords = List(scheduleRecord1, scheduleRecord2)
        )) { fixture =>
        fixture.scheduleTable.retrieveAllScheduleRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe scheduleRecord1
    retrievedRecords(1) shouldBe scheduleRecord2
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
          tiplocRecords = List(
            TipLocRecord(TipLocCode("REIGATE"), Stanox("REI"), None),
            TipLocRecord(TipLocCode("MERSTHAM"), Stanox("MER"), None),
            TipLocRecord(TipLocCode("REDHILL"), Stanox("RED"), None)
          ),
          scheduleRecords = List(scheduleRecord)
        )) { fixture =>
        fixture.scheduleTable.retrieveAllScheduleRecords()
      }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe scheduleRecord

  }

}
