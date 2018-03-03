package traindelays.networkrail.scheduledata

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType.{
  OriginatingLocation,
  TerminatingLocation
}
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail._

class ScheduleDataReaderTest extends FlatSpec {

  it should "read data from schedule source records " in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[DecodedScheduleRecord].runLog.unsafeRunSync().toList

    result should have size 3
    result.head shouldBe DecodedScheduleRecord.Create(
      ScheduleTrainId("G76481"),
      ServiceCode("24745000"),
      TrainCategory("OO"),
      TrainStatus("P"),
      Some(AtocCode("SN")),
      DaysRun(monday = true,
              tuesday = true,
              wednesday = true,
              thursday = true,
              friday = true,
              saturday = false,
              sunday = false),
      LocalDate.parse("2017-12-11"),
      LocalDate.parse("2017-12-29"),
      StpIndicator.P,
      List(
        ScheduleLocationRecord(OriginatingLocation,
                               TipLocCode("REIGATE"),
                               None,
                               Some(LocalTime.parse("0649", timeFormatter))),
        ScheduleLocationRecord(TerminatingLocation,
                               TipLocCode("REDHILL"),
                               Some(LocalTime.parse("0653", timeFormatter)),
                               None)
      )
    )
  }

  it should "read data from stanox source records, ignoring those without crs code" in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[DecodedStanoxRecord].runLog.unsafeRunSync().toList
    result should have size 14
    result should contain(
      DecodedStanoxRecord.Create(TipLocCode("REDHILL"), Some(StanoxCode("87722")), Some(CRS("RDH")), Some("REDHILL")))
    result should contain(
      DecodedStanoxRecord.Create(TipLocCode("REIGATE"), Some(StanoxCode("87089")), Some(CRS("REI")), Some("REIGATE")))

  }

  it should "read remove location records where departure time and arrival time are both None" in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid-with-interim-stop.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[DecodedScheduleRecord].runLog.unsafeRunSync().toList

    result should have size 1
    result.head shouldBe DecodedScheduleRecord.Create(
      ScheduleTrainId("G76481"),
      ServiceCode("24745000"),
      TrainCategory("OO"),
      TrainStatus("P"),
      Some(AtocCode("SN")),
      DaysRun(monday = true,
              tuesday = true,
              wednesday = true,
              thursday = true,
              friday = true,
              saturday = false,
              sunday = false),
      LocalDate.parse("2018-01-01"),
      LocalDate.parse("2018-01-26"),
      StpIndicator.P,
      List(
        ScheduleLocationRecord(OriginatingLocation,
                               TipLocCode("REIGATE"),
                               None,
                               Some(LocalTime.parse("0649", timeFormatter))),
        ScheduleLocationRecord(TerminatingLocation,
                               TipLocCode("REDHILL"),
                               Some(LocalTime.parse("0653", timeFormatter)),
                               None)
      )
    )
  }
}
