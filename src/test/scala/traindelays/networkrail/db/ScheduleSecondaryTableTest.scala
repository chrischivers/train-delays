package traindelays.networkrail.db

import java.nio.file.Paths

import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.ScheduleDataConfig
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.{StanoxCode, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class ScheduleSecondaryTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve an inserted schedule association record from the database (single insertion)" in {

    val scheduleAssociationRecord = createScheduleRecordSecondary()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.scheduleSecondaryTable.safeAddRecord(scheduleAssociationRecord).unsafeRunSync()
      val retrievedRecords = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe scheduleAssociationRecord.copy(id = Some(1))
    }
  }

  it should "delete all schedule association records from the database" in {

    val scheduleAssociationRecord1 = createScheduleRecordSecondary(associationId = 1)
    val scheduleAssociationRecord2 =
      createScheduleRecordSecondary(scheduleTrainId = ScheduleTrainId("A87532"), associationId = 2)

    withInitialState(testDatabaseConfig)(
      AppInitialState(scheduleSecondaryRecords = List(scheduleAssociationRecord1, scheduleAssociationRecord2))
    ) { fixture =>
      val retrievedRecord1 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 2
      fixture.scheduleSecondaryTable.deleteAllRecords().unsafeRunSync()
      val retrievedRecord2 = fixture.scheduleSecondaryTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "delete a schedule association record from the database by id, start date and stpIndicator" in {

    val scheduleAssociationRecord = createScheduleRecordSecondary()

    withInitialState(testDatabaseConfig)(
      AppInitialState(scheduleSecondaryRecords = List(scheduleAssociationRecord))
    ) { fixture =>
      val retrievedRecord1 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 1
      fixture.scheduleSecondaryTable
        .deleteRecord(scheduleAssociationRecord.scheduleTrainId,
                      scheduleAssociationRecord.scheduleStart,
                      scheduleAssociationRecord.stpIndicator)
        .unsafeRunSync()
      val retrievedRecord2 = fixture.scheduleSecondaryTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "delete a schedule association record from the database by association id" in {

    val scheduleAssociationRecord = createScheduleRecordSecondary()

    withInitialState(testDatabaseConfig)(
      AppInitialState(scheduleSecondaryRecords = List(scheduleAssociationRecord))
    ) { fixture =>
      val retrievedRecord1 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 1
      fixture.scheduleSecondaryTable
        .deleteRecord(scheduleAssociationRecord.associationId)
        .unsafeRunSync()
      val retrievedRecord2 = fixture.scheduleSecondaryTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "retrieve distinct stanox codes from the DB" in {

    val scheduleAssociationRecord1 =
      createScheduleRecordSecondary(stanoxCode = StanoxCode("12345"), associationId = 1)
    val scheduleAssociationRecord2 = createScheduleRecordSecondary(scheduleTrainId = ScheduleTrainId("5653864"),
                                                                   stanoxCode = StanoxCode("23456"),
                                                                   associationId = 2)

    withInitialState(testDatabaseConfig)(
      AppInitialState(scheduleSecondaryRecords = List(scheduleAssociationRecord1, scheduleAssociationRecord2))) {
      fixture =>
        val distinctStanoxCodes = fixture.scheduleSecondaryTable.retrieveAllDistinctStanoxCodes.unsafeRunSync()
        distinctStanoxCodes should have size 2
        distinctStanoxCodes should contain theSameElementsAs List(StanoxCode("12345"), StanoxCode("23456"))
    }

  }

  it should "memoize retrieval of an inserted schedule association record from the database" in {

    import scala.concurrent.duration._

    val scheduleAssociationRecord = createScheduleRecordSecondary()

    withInitialState(testDatabaseConfig,
                     scheduleDataConfig = ScheduleDataConfig(Uri.unsafeFromString(""),
                                                             Uri.unsafeFromString(""),
                                                             Paths.get(""),
                                                             Paths.get(""),
                                                             2.seconds))(
      AppInitialState(
        scheduleSecondaryRecords = List(scheduleAssociationRecord)
      )) { fixture =>
      val retrievedRecords1 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()

      retrievedRecords1 should have size 1
      retrievedRecords1.head.stanoxCode shouldBe StanoxCode("12345")
      fixture.scheduleSecondaryTable.deleteAllRecords().unsafeRunSync()

      val retrievedRecords2 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords2 should have size 1

      Thread.sleep(2000)

      val retrievedRecords3 = fixture.scheduleSecondaryTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords3 should have size 0
    }
  }

}
