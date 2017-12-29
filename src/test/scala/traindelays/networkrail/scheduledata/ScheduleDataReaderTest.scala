package traindelays.networkrail.scheduledata

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}

class ScheduleDataReaderTest extends FlatSpec {

  it should "read data from schedule source records " in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[ScheduleRecord].runLog.unsafeRunSync().toList

    //TODO more here once test data has been trimmed
    result should have size 3
    result.head shouldBe ScheduleRecord(
      "G76481",
      "24745000",
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
        ScheduleLocationRecord("LO", "REIGATE", None, Some(LocalTime.parse("0649", timeFormatter))),
        ScheduleLocationRecord("LT", "REDHILL", Some(LocalTime.parse("0653", timeFormatter)), None)
      )
    )
  }

  it should "read data from tiploc source records" in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[TipLocRecord].runLog.unsafeRunSync().toList
    result should have size 14
    result.head shouldBe TipLocRecord("REDH316", "REDHILL SIGNAL T1316")

  }
}
