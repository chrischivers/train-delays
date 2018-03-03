package traindelays.networkrail.db

import java.nio.file.Paths

import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.ScheduleDataConfig
import traindelays.networkrail.TestFeatures
import traindelays.networkrail.scheduledata._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class AssociationTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve an inserted association record from the database (single insertion)" in {

    val associationRecord = createAssociationRecord()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.associationTable.addRecord(associationRecord).unsafeRunSync()
      val retrievedRecords = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe associationRecord.copy(id = Some(1))
    }

  }

  it should "delete all association records from the database" in {

    val associationRecord1 = createAssociationRecord()
    val associationRecord2 = createAssociationRecord(mainScheduleTrainID = ScheduleTrainId("A87532"))

    withInitialState(testDatabaseConfig)(
      AppInitialState(associationRecords = List(associationRecord1, associationRecord2))
    ) { fixture =>
      val retrievedRecord1 = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 2
      fixture.associationTable.deleteAllRecords().unsafeRunSync()
      val retrievedRecord2 = fixture.associationTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "delete an association record from the database by main train id, association tracin id, start date, location and stpIndicator" in {

    val associationRecord = createAssociationRecord()

    withInitialState(testDatabaseConfig)(
      AppInitialState(associationRecords = List(associationRecord))
    ) { fixture =>
      val retrievedRecord1 = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecord1 should have size 1
      fixture.associationTable
        .deleteRecord(
          associationRecord.mainScheduleTrainId,
          associationRecord.associatedScheduleTrainId,
          associationRecord.associatedStart,
          associationRecord.stpIndicator,
          associationRecord.location
        )
        .unsafeRunSync()
      val retrievedRecord2 = fixture.associationTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecord2 should have size 0
    }
  }

  it should "retrieve multiple inserted association records from the database" in {

    val associationRecord1 = createAssociationRecord()
    val associationRecord2 = createAssociationRecord().copy(mainScheduleTrainId = ScheduleTrainId("123456"))

    withInitialState(testDatabaseConfig)(
      AppInitialState(associationRecords = List(associationRecord1, associationRecord2))) { fixture =>
      val retrievedRecords = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe associationRecord1.copy(id = Some(1))
      retrievedRecords(1) shouldBe associationRecord2.copy(id = Some(2))
    }

  }

  it should "memoize retrieval of an inserted association record from the database" in {

    import scala.concurrent.duration._

    val associationRecord = createAssociationRecord()

    withInitialState(testDatabaseConfig,
                     scheduleDataConfig = ScheduleDataConfig(Uri.unsafeFromString(""),
                                                             Uri.unsafeFromString(""),
                                                             Paths.get(""),
                                                             Paths.get(""),
                                                             2 seconds))(
      AppInitialState(
        associationRecords = List(associationRecord)
      )) { fixture =>
      val retrievedRecords1 = fixture.associationTable.retrieveAllRecords().unsafeRunSync()

      retrievedRecords1 should have size 1
      retrievedRecords1.head shouldBe associationRecord.copy(id = Some(1))

      fixture.associationTable.deleteAllRecords().unsafeRunSync()

      val retrievedRecords2 = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords2 should have size 1

      Thread.sleep(2000)

      val retrievedRecords3 = fixture.associationTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords3 should have size 0
    }
  }

  it should "delete all records from the database" in {

    val associationRecord = createAssociationRecord()

    withInitialState(testDatabaseConfig)(
      AppInitialState(
        associationRecords = List(associationRecord)
      )) { fixture =>
      val retrievedRecords = (for {
        _         <- fixture.associationTable.deleteAllRecords()
        retrieved <- fixture.associationTable.retrieveAllRecords()
      } yield retrieved).unsafeRunSync()

      retrievedRecords should have size 0
    }

  }
}
