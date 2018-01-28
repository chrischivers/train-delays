package traindelays.networkrail.scheduledata

import java.time.LocalTime

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.TestFeatures
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}

class ScheduleDataTest extends FlatSpec with TestFeatures {

  it should "convert daysRun string into DaysRun class" in {

    val str1 = "0000010"
    val str2 = "1111100"

    DaysRun.daysRunFrom(str1).right.get shouldBe DaysRun(monday = false,
                                                         tuesday = false,
                                                         wednesday = false,
                                                         thursday = false,
                                                         friday = false,
                                                         saturday = true,
                                                         sunday = false)
    DaysRun.daysRunFrom(str2).right.get shouldBe DaysRun(monday = true,
                                                         tuesday = true,
                                                         wednesday = true,
                                                         thursday = true,
                                                         friday = true,
                                                         saturday = false,
                                                         sunday = false)
  }

  it should "convert DaysRun class into DaysRun pattern" in {
    val daysRun1 = DaysRun(monday = true,
                           tuesday = true,
                           wednesday = true,
                           thursday = true,
                           friday = true,
                           saturday = false,
                           sunday = false)

    val daysRun2 = DaysRun(monday = false,
                           tuesday = false,
                           wednesday = false,
                           thursday = false,
                           friday = false,
                           saturday = false,
                           sunday = true)

    val daysRun3 = DaysRun(monday = false,
                           tuesday = false,
                           wednesday = false,
                           thursday = false,
                           friday = false,
                           saturday = false,
                           sunday = false)

    daysRun1.toDaysRunPattern.get shouldBe DaysRunPattern.Weekdays
    daysRun2.toDaysRunPattern.get shouldBe DaysRunPattern.Sundays
    daysRun3.toDaysRunPattern shouldBe None

  }
  it should "convert schedule record to schedule logs (no associated tiploc codes)" in {

    import org.scalatest.Inspectors._

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
                                                 TipLocCode("EASTCROYDON"),
                                                 Some(LocalTime.parse("0715", timeFormatter)),
                                                 None)
    val scheduleRecord =
      createScheduleRecord(locationRecords = List(locationRecord1, locationRecord2, locationRecord3, locationRecord4))

    val stanoxRecord1 = StanoxRecord(StanoxCode("12345"), TipLocCode("REIGATE"), CRS("REI"), None)
    val stanoxRecord2 = StanoxRecord(StanoxCode("23456"), TipLocCode("REDHILL"), CRS("RDH"), None)
    val stanoxRecord3 = StanoxRecord(StanoxCode("34567"), TipLocCode("MERSTHAM"), CRS("MER"), None)
    val stanoxRecord4 = StanoxRecord(StanoxCode("45678"), TipLocCode("EASTCROYDON"), CRS("ECD"), None)

    val scheduleLogs = scheduleRecord.toScheduleLogs(List(stanoxRecord1, stanoxRecord2, stanoxRecord3, stanoxRecord4))
    scheduleLogs should have size 4

    forAll(scheduleLogs)(_.scheduleTrainId shouldBe scheduleRecord.scheduleTrainId)
    forAll(scheduleLogs)(_.atocCode shouldBe scheduleRecord.atocCode)
    forAll(scheduleLogs)(_.daysRunPattern shouldBe scheduleRecord.daysRun.toDaysRunPattern.get)
    forAll(scheduleLogs)(_.scheduleStart shouldBe scheduleRecord.scheduleStartDate)
    forAll(scheduleLogs)(_.scheduleEnd shouldBe scheduleRecord.scheduleEndDate)

    scheduleLogs(0).stopSequence shouldBe 1
    scheduleLogs(0).stanoxCode shouldBe stanoxRecord1.stanoxCode
    scheduleLogs(0).locationType shouldBe locationRecord1.locationType
    scheduleLogs(0).departureTime shouldBe locationRecord1.departureTime
    scheduleLogs(0).arrivalTime shouldBe locationRecord1.arrivalTime
    scheduleLogs(0).subsequentStanoxCodes shouldBe List(stanoxRecord2.stanoxCode,
                                                        stanoxRecord3.stanoxCode,
                                                        stanoxRecord4.stanoxCode)

    scheduleLogs(1).stopSequence shouldBe 2
    scheduleLogs(1).stanoxCode shouldBe stanoxRecord2.stanoxCode
    scheduleLogs(1).locationType shouldBe locationRecord2.locationType
    scheduleLogs(1).departureTime shouldBe locationRecord2.departureTime
    scheduleLogs(1).arrivalTime shouldBe locationRecord2.arrivalTime
    scheduleLogs(1).subsequentStanoxCodes shouldBe List(stanoxRecord3.stanoxCode, stanoxRecord4.stanoxCode)

    scheduleLogs(2).stopSequence shouldBe 3
    scheduleLogs(2).stanoxCode shouldBe stanoxRecord3.stanoxCode
    scheduleLogs(2).locationType shouldBe locationRecord3.locationType
    scheduleLogs(2).departureTime shouldBe locationRecord3.departureTime
    scheduleLogs(2).arrivalTime shouldBe locationRecord3.arrivalTime
    scheduleLogs(2).subsequentStanoxCodes shouldBe List(stanoxRecord4.stanoxCode)

    scheduleLogs(3).stopSequence shouldBe 4
    scheduleLogs(3).stanoxCode shouldBe stanoxRecord4.stanoxCode
    scheduleLogs(3).locationType shouldBe locationRecord4.locationType
    scheduleLogs(3).departureTime shouldBe locationRecord4.departureTime
    scheduleLogs(3).arrivalTime shouldBe locationRecord4.arrivalTime
    scheduleLogs(3).subsequentStanoxCodes shouldBe List.empty

  }
}
