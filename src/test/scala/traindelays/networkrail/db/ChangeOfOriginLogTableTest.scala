package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.TestFeatures
import traindelays.networkrail.movementdata._

import scala.concurrent.ExecutionContext.Implicits.global

class ChangeOfOriginLogTableTest extends FlatSpec with TestFeatures {

  it should "insert and retrieve a change of origin log record from the database" in {

    val changeOfOriginLog = createChangeOfOriginLog()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.changeOfOriginLogTable.addRecord(changeOfOriginLog).unsafeRunSync()
      val retrievedRecords = fixture.changeOfOriginLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe changeOfOriginLog.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted change of origin log records from the database" in {

    val changeOfOriginLogRecord1 = createChangeOfOriginLog()
    val changeOfOriginLogRecord2 = createChangeOfOriginLog().copy(trainId = TrainId("862F60MY31"))

    withInitialState(testDatabaseConfig)(
      AppInitialState(changeOfOriginLogs = List(changeOfOriginLogRecord1, changeOfOriginLogRecord2))) { fixture =>
      val retrievedRecords = fixture.changeOfOriginLogTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe changeOfOriginLogRecord1.copy(id = Some(1))
      retrievedRecords(1) shouldBe changeOfOriginLogRecord2.copy(id = Some(2))
    }

  }

}
