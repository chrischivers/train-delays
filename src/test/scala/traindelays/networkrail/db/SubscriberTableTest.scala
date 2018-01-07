package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
import traindelays.networkrail.subscribers.SubscriberRecord

import scala.concurrent.ExecutionContext.Implicits.global

class SubscriberTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a watching record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.subscriberTable.addRecord(getSubscriberRecord())
    }
  }

  it should "retrieve an inserted watching record from the database" in {

    val subscriberRecord = getSubscriberRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(subscriberRecords = List(subscriberRecord))) {
      fixture =>
        fixture.subscriberTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe subscriberRecord.copy(id = Some(1))
  }

  it should "retrieve multiple inserted watching records from the database" in {

    val watchingRecord1 = getSubscriberRecord()
    val watchingRecord2 = getSubscriberRecord().copy(userId = "BCDEFGH")

    val retrievedRecords =
      withInitialState(config)(AppInitialState(subscriberRecords = List(watchingRecord1, watchingRecord2))) { fixture =>
        fixture.subscriberTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
    retrievedRecords(1) shouldBe watchingRecord2.copy(id = Some(2))
  }

  it should "retrieve a record based on a scheduleCode, train ID and stanox" in {

    val watchingRecord1 = getSubscriberRecord()

    val retrievedRecords =
      withInitialState(config)(AppInitialState(subscriberRecords = List(watchingRecord1))) { fixture =>
        fixture.subscriberTable.subscriberRecordsFor(watchingRecord1.trainId,
                                                     watchingRecord1.serviceCode,
                                                     watchingRecord1.stanox)
      }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
  }

  def getSubscriberRecord(userId: String = "ABCDEFG",
                          email: String = "test@test.com",
                          trainId: String = "G76481",
                          serviceCode: String = "24745000",
                          stanox: String = "REDHILL") =
    SubscriberRecord(None, userId, email, trainId, serviceCode, stanox)

}
