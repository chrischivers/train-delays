package traindelays.networkrail.scheduledata

import java.time.LocalDate

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.TestFeatures

class DivideAssociationTest extends FlatSpec with TestFeatures {
  it should "schedule association record should be created for dividing of two trains" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithDivideAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                             associatedScheduleTrainId =
                                                                               associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(
      id = Some(1),
      location = defaultInitialState.stanoxRecords(3).tipLocCode,
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId,
      associationCategory = Some(AssociationCategory.Divide)
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
    scheduleRecordAssociations.get should have size 5
    scheduleRecordAssociations.get.map(_.stanoxCode) shouldBe
      primaryTrainRecords.map(_.stanoxCode).dropRight(2) ++ secondaryTrainRecords.map(_.stanoxCode)
    scheduleRecordAssociations.get.forall(_.scheduleTrainId == associationRecord.associatedScheduleTrainId) shouldBe true
  }

  it should "schedule association record should NOT be created for joining of two trains when dates do not overlap" in {

    val mainScheduleTrainId       = ScheduleTrainId("87334")
    val associatedScheduleTrainId = ScheduleTrainId("22012")
    val defaultInitialState = createDefaultInitialStateWithDivideAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                             associatedScheduleTrainId =
                                                                               associatedScheduleTrainId)

    val associationRecord = createAssociationRecord(
      id = Some(1),
      location = defaultInitialState.stanoxRecords(3).tipLocCode,
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId,
      associationCategory = Some(AssociationCategory.Divide),
      associatedStartDate = LocalDate.parse("2018-09-21"),
      associatedEndDate = LocalDate.parse("2018-09-22")
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
    val defaultInitialState = createDefaultInitialStateWithDivideAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                             associatedScheduleTrainId =
                                                                               ScheduleTrainId("45678"))

    val associationRecord = createAssociationRecord(
      id = Some(1),
      location = defaultInitialState.stanoxRecords(3).tipLocCode,
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId,
      associationCategory = Some(AssociationCategory.Divide)
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
    val defaultInitialState = createDefaultInitialStateWithDivideAssociation(mainScheduleTrainId = mainScheduleTrainId,
                                                                             associatedScheduleTrainId =
                                                                               associatedScheduleTrainId)
    val associationRecord = createAssociationRecord(
      id = Some(1),
      daysRunPattern = DaysRunPattern.Saturdays,
      location = defaultInitialState.stanoxRecords(3).tipLocCode,
      mainScheduleTrainID = mainScheduleTrainId,
      associatedScheduleTrainID = associatedScheduleTrainId,
      associationCategory = Some(AssociationCategory.Divide)
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
}
