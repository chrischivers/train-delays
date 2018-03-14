package traindelays.networkrail.scheduledata

import java.time.LocalDate

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.TestFeatures

import scala.util.Random

//TODO repopulate secondary schedule table
//TODO history to also use primary table.
//TODO movement log to factor in secondary table

class JoinAssociationTest extends FlatSpec with TestFeatures {
  it should "schedule association record should be created for joining of two trains" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithJoinAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                           associatedScheduleTrainId =
                                                                             associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(id = Some(1),
                                                    mainScheduleTrainID = mainScheduleTrainId,
                                                    associatedScheduleTrainID = associatedScheduleTrainId)

    val secondaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == associatedScheduleTrainId)
    val primaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == mainScheduleTrainId)

    val scheduleRecordAssociations = associationRecord.toSecondaryScheduleRecords(
      scheduleRecordsForMainId = primaryTrainRecords,
      scheduleRecordsForAssociatedId = secondaryTrainRecords,
      stanoxRecordForAssocationLocation =
        defaultInitialState.stanoxRecords.filter(_.tipLocCode == associationRecord.location)
    )
    scheduleRecordAssociations.get should have size 6
    scheduleRecordAssociations.get.map(_.stanoxCode) shouldBe
      secondaryTrainRecords.map(_.stanoxCode) ++ primaryTrainRecords.map(_.stanoxCode).drop(1)
    scheduleRecordAssociations.get.forall(_.scheduleTrainId == associatedScheduleTrainId) shouldBe true
  }

  it should "schedule association record should NOT be created for joining of two trains when dates do not overlap" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithJoinAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                           associatedScheduleTrainId =
                                                                             associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(
      id = Some(1),
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId,
      associatedStartDate = LocalDate.parse("2018-09-01"),
      associatedEndDate = LocalDate.parse("2018-10-02")
    )

    val secondaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == associatedScheduleTrainId)
    val primaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == mainScheduleTrainId)

    val scheduleRecordAssociations = associationRecord.toSecondaryScheduleRecords(
      scheduleRecordsForMainId = primaryTrainRecords,
      scheduleRecordsForAssociatedId = secondaryTrainRecords,
      stanoxRecordForAssocationLocation =
        defaultInitialState.stanoxRecords.filter(_.tipLocCode == associationRecord.location)
    )
    scheduleRecordAssociations.isEmpty shouldBe true
  }

  it should "schedule association record should NOT be created for joining of two trains when schedule train Ids do not match" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithJoinAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                           associatedScheduleTrainId =
                                                                             ScheduleTrainId("35678"))

    val associationRecord = createAssociationRecord(
      id = Some(1),
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId
    )

    val secondaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == associatedScheduleTrainId)
    val primaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == mainScheduleTrainId)

    val scheduleRecordAssociations = associationRecord.toSecondaryScheduleRecords(
      scheduleRecordsForMainId = primaryTrainRecords,
      scheduleRecordsForAssociatedId = secondaryTrainRecords,
      stanoxRecordForAssocationLocation =
        defaultInitialState.stanoxRecords.filter(_.tipLocCode == associationRecord.location)
    )
    scheduleRecordAssociations.isEmpty shouldBe true
  }

  it should "schedule association record should NOT be created for joining of two trains with different day of the week patterns" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithJoinAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                           associatedScheduleTrainId =
                                                                             associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(
      id = Some(1),
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = ScheduleTrainId("24567"),
      daysRunPattern = DaysRunPattern.Saturdays
    )

    val secondaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == associatedScheduleTrainId)
    val primaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == mainScheduleTrainId)

    val scheduleRecordAssociations = associationRecord.toSecondaryScheduleRecords(
      scheduleRecordsForMainId = primaryTrainRecords,
      scheduleRecordsForAssociatedId = secondaryTrainRecords,
      stanoxRecordForAssocationLocation =
        defaultInitialState.stanoxRecords.filter(_.tipLocCode == associationRecord.location)
    )
    scheduleRecordAssociations.isEmpty shouldBe true
  }

  it should "irrelevant schedule records are disregarded when joining of two trains" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithJoinAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                           associatedScheduleTrainId =
                                                                             associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(id = Some(1),
                                                    mainScheduleTrainID = mainScheduleTrainId,
                                                    associatedScheduleTrainID = associatedScheduleTrainId)

    val secondaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == associatedScheduleTrainId)
    val primaryTrainRecords =
      defaultInitialState.schedulePrimaryRecords.filter(_.scheduleTrainId == mainScheduleTrainId)
    val irrelevantRecords =
      primaryTrainRecords.map(rec => rec.copy(scheduleTrainId = ScheduleTrainId(s"fake${Random.nextString(6)}")))

    val scheduleRecordAssociations = associationRecord.toSecondaryScheduleRecords(
      scheduleRecordsForMainId = primaryTrainRecords ++ irrelevantRecords,
      scheduleRecordsForAssociatedId = secondaryTrainRecords ++ irrelevantRecords,
      stanoxRecordForAssocationLocation =
        defaultInitialState.stanoxRecords.filter(_.tipLocCode == associationRecord.location)
    )
    scheduleRecordAssociations.get should have size 6
    scheduleRecordAssociations.get.map(_.stanoxCode) shouldBe
      secondaryTrainRecords.map(_.stanoxCode) ++ primaryTrainRecords.map(_.stanoxCode).drop(1)
    scheduleRecordAssociations.get.forall(_.scheduleTrainId == associatedScheduleTrainId) shouldBe true
  }
}
