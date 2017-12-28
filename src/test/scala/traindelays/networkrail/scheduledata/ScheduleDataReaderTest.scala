package traindelays.networkrail.scheduledata

import java.nio.file.Paths
import java.time.LocalDate

import org.scalatest.Matchers._
import org.scalatest.FlatSpec
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord

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
      "1111100",
      LocalDate.parse("2017-12-11"),
      LocalDate.parse("2017-12-29"),
      List(ScheduleLocationRecord("LO", "REIGATE", None, Some("0649")),
           ScheduleLocationRecord("LT", "REDHILL", Some("0653"), None))
    )
  }

  it should "read data from tiploc source records" in {

    val source = Paths.get(getClass.getResource("/test-schedule-single-train-uid.json").getPath)
    val reader = ScheduleDataReader(source)

    val result = reader.readData[TipLocRecord].runLog.unsafeRunSync().toList
    println(result.size)
    result should have size 14
    result.head shouldBe TipLocRecord("REDH316", "REDHILL SIGNAL T1316")

  }
}
