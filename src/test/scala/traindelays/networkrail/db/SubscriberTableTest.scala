package traindelays.networkrail.db

import cats.effect.IO
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.{SubscriberRecord, UserId}
import traindelays.networkrail.{ServiceCode, StanoxCode}
import traindelays.{DatabaseConfig, SubscribersConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class SubscriberTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a watching record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.subscriberTable.addRecord(getSubscriberRecord())
    }
  }

  it should "retrieve inserted watching records from the database" in {

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
    val watchingRecord2 = getSubscriberRecord().copy(userId = UserId("BCDEFGH"))

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
        fixture.subscriberTable.subscriberRecordsFor(watchingRecord1.scheduleTrainId,
                                                     watchingRecord1.serviceCode,
                                                     watchingRecord1.stanoxCode)
      }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe watchingRecord1.copy(id = Some(1))
  }

  it should "memoize retreival of records to minimize database calls" in {

    import scala.concurrent.duration._

    val subscriberRecord = getSubscriberRecord()

    withInitialState(config, SubscribersConfig(3 seconds))(AppInitialState(subscriberRecords = List(subscriberRecord))) {
      fixture =>
        IO {
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

  def getSubscriberRecord(userId: UserId = UserId("ABCDEFG"),
                          email: String = "test@test.com",
                          emailVerified: Option[Boolean] = Some(true),
                          name: Option[String] = Some("joebloggs"),
                          firstName: Option[String] = Some("Joe"),
                          familyName: Option[String] = Some("Bloggs"),
                          locale: Option[String] = Some("GB"),
                          scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
                          serviceCode: ServiceCode = ServiceCode("24745000"),
                          stanoxCode: StanoxCode = StanoxCode("REDHILL")) =
    SubscriberRecord(None,
                     userId,
                     email,
                     emailVerified,
                     name,
                     firstName,
                     familyName,
                     locale,
                     scheduleTrainId,
                     serviceCode,
                     stanoxCode)

}
