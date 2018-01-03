package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
import traindelays.networkrail.watching.WatchingRecord

import scala.concurrent.ExecutionContext.Implicits.global

class WatchingTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a watching record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.watchingTable.addRecord(getWatchingRecord())
    }
  }

  it should "retrieve an inserted watching record from the database" in {

    val watchingRecord = getWatchingRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(watchingRecords = List(watchingRecord))) {
      fixture =>
        fixture.watchingTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe watchingRecord.copy(id = Some(1))
  }

  it should "retrieve multiple inserted watching records from the database" in {

    val watchingRecord1 = getWatchingRecord()
    val watchingRecord2 = getWatchingRecord().copy(userId = "BCDEFGH")

    val retrievedRecords =
      withInitialState(config)(AppInitialState(watchingRecords = List(watchingRecord1, watchingRecord2))) { fixture =>
        fixture.watchingTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
    retrievedRecords(1) shouldBe watchingRecord2.copy(id = Some(2))
  }

  def getWatchingRecord(userId: String = "ABCDEFG",
                        trainId: String = "G76481",
                        serviceCode: String = "24745000",
                        stanox: String = "REDHILL") =
    WatchingRecord(None, userId, trainId, serviceCode, stanox)

}
