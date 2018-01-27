package traindelays.networkrail.db
import java.time.LocalTime

import cats.effect.IO
import traindelays.DatabaseConfig
import traindelays.networkrail.{CRS, IntegrationTest, Stanox}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata.{TipLocRecord, timeFormatter}
import org.scalatest.Matchers._

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

    val tiplocRecord1 = TipLocRecord(TipLocCode("REIGATE"), Stanox("12345"), CRS("REI"), None)
    val tiplocRecord2 = TipLocRecord(TipLocCode("REDHILL"), Stanox("23456"), CRS("RED"), None)
    val tiplocRecord3 = TipLocRecord(TipLocCode("MERSTHAM"), Stanox("34567"), CRS("MER"), None)
    val tiplocRecord4 = TipLocRecord(TipLocCode("EASTCROYDN"), Stanox("45678"), CRS("ECD"), None)

    withInitialState(config)(
      AppInitialState(
        tiplocRecords = List(
          tiplocRecord1,
          tiplocRecord2,
          tiplocRecord3,
          tiplocRecord4
        ),
        scheduleRecords = List(scheduleRecord)
      )) { fixture =>
      IO {
        val retrieved1 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord1.tiplocCode,
                                         locationRecord4.tiplocCode,
                                         DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved1 should have size 1
        retrieved1.head.tiplocCode shouldBe locationRecord1.tiplocCode

        val retrieved2 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord2.tiplocCode,
                                         locationRecord4.tiplocCode,
                                         DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved2 should have size 1
        retrieved2.head.tiplocCode shouldBe locationRecord2.tiplocCode

        val retrieved3 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord2.tiplocCode,
                                         locationRecord3.tiplocCode,
                                         DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved3 should have size 1
        retrieved3.head.tiplocCode shouldBe locationRecord2.tiplocCode

        val retrieved4 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord3.tiplocCode,
                                         locationRecord2.tiplocCode,
                                         DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved4 should have size 0

        val retrieved5 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord3.tiplocCode,
                                         locationRecord3.tiplocCode,
                                         DaysRunPattern.Weekdays)
          .unsafeRunSync()
        retrieved5 should have size 0

        val retrieved6 = fixture.scheduleTable
          .retrieveScheduleLogRecordsFor(locationRecord2.tiplocCode, locationRecord3.tiplocCode, DaysRunPattern.Sundays)
          .unsafeRunSync()
        retrieved6 should have size 0

      }
    }

  }
}
