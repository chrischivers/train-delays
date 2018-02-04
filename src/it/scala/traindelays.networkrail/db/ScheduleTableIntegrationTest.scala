package traindelays.networkrail.db
import java.time.LocalTime

import cats.effect.IO
import org.scalatest.Matchers._
import traindelays.DatabaseConfig
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.{StanoxRecord, timeFormatter}
import traindelays.networkrail.{CRS, IntegrationTest, StanoxCode, TipLocCode}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleTableIntegrationTest extends ScheduleTableTest with IntegrationTest {
  override protected def config: DatabaseConfig = testconfig.databaseConfig

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
      createScheduleRecord(locationRecords = List(locationRecord1, locationRecord2, locationRecord3, locationRecord4))

    val stanoxRecord1 = StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), Some(CRS("REI")), None)
    val stanoxRecord2 = StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), Some(CRS("RED")), None)
    val stanoxRecord3 = StanoxRecord(StanoxCode("34567"), TipLocCode("MERSTHAM"), Some(CRS("MER")), None)
    val stanoxRecord4 = StanoxRecord(StanoxCode("45678"), TipLocCode("EASTCROYDN"), Some(CRS("ECD")), None)

    val _stanoxRecords = List(
      stanoxRecord1,
      stanoxRecord2,
      stanoxRecord3,
      stanoxRecord4
    )

    withInitialState(config)(
      AppInitialState(
        stanoxRecords = _stanoxRecords,
        scheduleLogRecords = scheduleRecord.toScheduleLogs(_stanoxRecords.map(x => x.tipLocCode -> x.stanoxCode).toMap)
      )) { fixture =>
      IO {
        val retrieved1 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord1.stanoxCode, stanoxRecord4.stanoxCode, DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved1 should have size 1
        retrieved1.head.stanoxCode shouldBe stanoxRecord1.stanoxCode

        val retrieved2 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode, stanoxRecord4.stanoxCode, DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved2 should have size 1
        retrieved2.head.stanoxCode shouldBe stanoxRecord2.stanoxCode

        val retrieved3 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode, stanoxRecord3.stanoxCode, DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved3 should have size 1
        retrieved3.head.stanoxCode shouldBe stanoxRecord2.stanoxCode

        val retrieved4 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord3.stanoxCode, stanoxRecord2.stanoxCode, DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved4 should have size 0

        val retrieved5 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord3.stanoxCode, stanoxRecord3.stanoxCode, DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved5 should have size 0

        val retrieved6 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(stanoxRecord2.stanoxCode, stanoxRecord3.stanoxCode, DaysRunPattern.Sundays)
          .unsafeRunSync()
        retrieved6 should have size 0

      }
    }

  }
}
