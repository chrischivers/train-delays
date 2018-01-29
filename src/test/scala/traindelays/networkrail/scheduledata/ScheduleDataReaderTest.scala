package traindelays.networkrail.scheduledata

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType.{
  OriginatingLocation,
  TerminatingLocation
}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.{CRS, ServiceCode, StanoxCode, TipLocCode}

class ScheduleDataReaderTest extends FlatSpec {

  it should "read data from schedule source records " in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[ScheduleRecord].runLog.unsafeRunSync().toList

    result should have size 3
    result.head shouldBe ScheduleRecord(
      ScheduleTrainId("G76481"),
      ServiceCode("24745000"),
      AtocCode("SN"),
      DaysRun(monday = true,
              tuesday = true,
              wednesday = true,
              thursday = true,
              friday = true,
              saturday = false,
              sunday = false),
      LocalDate.parse("2017-12-11"),
      LocalDate.parse("2017-12-29"),
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

    val result = reader.readData[StanoxRecord].runLog.unsafeRunSync().toList
    result should have size 2
    result.head shouldBe StanoxRecord(StanoxCode("87722"), TipLocCode("REDHILL"), Some(CRS("RDH")), Some("REDHILL"))
    result(1) shouldBe StanoxRecord(StanoxCode("87089"), TipLocCode("REIGATE"), Some(CRS("REI")), Some("REIGATE"))

  }

  it should "read remove location records where departure time and arrival time are both None" in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid-with-interim-stop.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[ScheduleRecord].runLog.unsafeRunSync().toList

    result should have size 1
    result.head shouldBe ScheduleRecord(
      ScheduleTrainId("G76481"),
      ServiceCode("24745000"),
      AtocCode("SN"),
      DaysRun(monday = true,
              tuesday = true,
              wednesday = true,
              thursday = true,
              friday = true,
              saturday = false,
              sunday = false),
      LocalDate.parse("2018-01-01"),
      LocalDate.parse("2018-01-26"),
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
