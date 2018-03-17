package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.subscribers.UserId
import traindelays.SubscribersConfig
import traindelays.networkrail.TestFeatures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class SubscriberTableTest extends FlatSpec with TestFeatures {

  it should "insert a watching record into the database" in {

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.subscriberTable.addRecord(createSubscriberRecord())
    }
  }

  it should "retrieve inserted watching records from the database" in {

    val subscriberRecord = createSubscriberRecord()

    withInitialState(testDatabaseConfig)(AppInitialState(subscriberRecords = List(subscriberRecord))) { fixture =>
      val retrievedRecords = fixture.subscriberTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe subscriberRecord.copy(id = Some(1))
    }

  }

  it should "retrieve multiple inserted watching records from the database" in {

    val watchingRecord1 = createSubscriberRecord()
    val watchingRecord2 = createSubscriberRecord().copy(userId = UserId("BCDEFGH"))

    withInitialState(testDatabaseConfig)(AppInitialState(subscriberRecords = List(watchingRecord1, watchingRecord2))) {
      fixture =>
        val retrievedRecords = fixture.subscriberTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords should have size 2
        retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
        retrievedRecords(1) shouldBe watchingRecord2.copy(id = Some(2))
    }

  }

  it should "retrieve a record based on a scheduleCode, train ID and stanox" in {

    val watchingRecord1 = createSubscriberRecord()

    withInitialState(testDatabaseConfig)(AppInitialState(subscriberRecords = List(watchingRecord1))) { fixture =>
      val retrievedRecords = fixture.subscriberTable
        .subscriberRecordsFor(watchingRecord1.scheduleTrainId, watchingRecord1.serviceCode)
        .unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
    }
  }

  it should "retrieve a record based on a userID" in {

    val watchingRecord1 = createSubscriberRecord()

    withInitialState(testDatabaseConfig)(AppInitialState(subscriberRecords = List(watchingRecord1))) { fixture =>
      val retrievedRecords = fixture.subscriberTable
        .subscriberRecordsFor(watchingRecord1.userId)
        .unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
    }
  }

  it should "memoize retreival of records to minimize database calls" in {

    import scala.concurrent.duration._

    val subscriberRecord = createSubscriberRecord()

    withInitialState(testDatabaseConfig, SubscribersConfig(3 seconds, 15 minutes))(
      AppInitialState(subscriberRecords = List(subscriberRecord))) { fixture =>
      val retrievedRecords1 = fixture.subscriberTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords1 should have size 1
      retrievedRecords1.head shouldBe subscriberRecord.copy(id = Some(1))

      fixture.subscriberTable.deleteAllRecords().unsafeRunSync()

      val retrievedRecords2 = fixture.subscriberTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords2 should have size 1
      retrievedRecords2.head shouldBe subscriberRecord.copy(id = Some(1))

      Thread.sleep(3000)

      val retrievedRecords3 = fixture.subscriberTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords3 should have size 0
    }

  }

}
