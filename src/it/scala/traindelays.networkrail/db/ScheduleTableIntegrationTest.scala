package traindelays.networkrail.db
import java.time.LocalTime

import cats.effect.IO
import org.scalatest.Matchers._
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord.DaysRunPattern
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.{StpIndicator, timeFormatter}
import traindelays.networkrail.{CRS, IntegrationTest, StanoxCode, TipLocCode}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableIntegrationTest extends ScheduleTableTest with IntegrationTest {

  it should "retrieve schedule records by starting station, ending station and daysRunPattern" in {

    val locationRecord1 = ScheduleLocationRecord(LocationType.OriginatingLocation,
                                                 TipLocCode("REIGATE"),
                                                 None,
                                                 Some(LocalTime.parse("0653", timeFormatter)))
    val locationRecord2 = ScheduleLocationRecord(LocationType.IntermediateLocation,
                                                 TipLocCode("REDHILL"),
                                                 Some(LocalTime.parse("0658", timeFormatter)),
                                                 Some(LocalTime.parse("0659", timeFormatter)))
    val locationRecord3 = ScheduleLocationRecord(LocationType.IntermediateLocation,
                                                 TipLocCode("MERSTHAM"),
                                                 Some(LocalTime.parse("0705", timeFormatter)),
                                                 Some(LocalTime.parse("0706", timeFormatter)))
    val locationRecord4 = ScheduleLocationRecord(LocationType.TerminatingLocation,
                                                 TipLocCode("EASTCROYDN"),
                                                 Some(LocalTime.parse("0715", timeFormatter)),
                                                 None)

    val scheduleRecord =
      createDecodedScheduleCreateRecord(
        locationRecords = List(locationRecord1, locationRecord2, locationRecord3, locationRecord4))

    val stanoxRecord1 = StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode("12345")), Some(CRS("REI")), None)
    val stanoxRecord2 = StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode("23456")), Some(CRS("RED")), None)
    val stanoxRecord3 = StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode("34567")), Some(CRS("MER")), None)
    val stanoxRecord4 = StanoxRecord(TipLocCode("EASTCROYDN"), Some(StanoxCode("45678")), Some(CRS("ECD")), None)

    val _stanoxRecords = List(
      stanoxRecord1,
      stanoxRecord2,
      stanoxRecord3,
      stanoxRecord4
    )

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        stanoxRecords = _stanoxRecords,
        scheduleLogRecords = scheduleRecord.toScheduleLogs(
          _stanoxRecords.flatMap(x => x.stanoxCode.map(stanox => x.tipLocCode -> stanox)).toMap)
      )) { fixture =>
      IO {
        val retrieved1 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord1.stanoxCode.get,
                                         stanoxRecord4.stanoxCode.get,
                                         DaysRunPattern.Weekdays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved1 should have size 1
        retrieved1.head.stanoxCode shouldBe stanoxRecord1.stanoxCode.get

        val retrieved2 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode.get,
                                         stanoxRecord4.stanoxCode.get,
                                         DaysRunPattern.Weekdays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved2 should have size 1
        retrieved2.head.stanoxCode shouldBe stanoxRecord2.stanoxCode.get

        val retrieved3 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode.get,
                                         stanoxRecord3.stanoxCode.get,
                                         DaysRunPattern.Weekdays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved3 should have size 1
        retrieved3.head.stanoxCode shouldBe stanoxRecord2.stanoxCode.get

        val retrieved4 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord3.stanoxCode.get,
                                         stanoxRecord2.stanoxCode.get,
                                         DaysRunPattern.Weekdays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved4 should have size 0

        val retrieved5 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord3.stanoxCode.get,
                                         stanoxRecord3.stanoxCode.get,
                                         DaysRunPattern.Weekdays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved5 should have size 0

        val retrieved6 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode.get,
                                         stanoxRecord3.stanoxCode.get,
                                         DaysRunPattern.Sundays,
                                         StpIndicator.P)
          .unsafeRunSync()
        retrieved6 should have size 0

      }
    }

  }
}
