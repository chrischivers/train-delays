package traindelays.networkrail.db

import java.nio.file.Paths

import cats.effect.IO
import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.scheduledata.StanoxRecord
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}
import traindelays.{DatabaseConfig, ScheduleDataConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global

class StanoxTableTest extends FlatSpec with TestFeatures {

  it should "retrieve an inserted stanox record from the database (single insertion)" in {

    val stanoxRecord = getStanoxRecord()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.addRecord(stanoxRecord).unsafeRunSync()
      val retrievedRecords = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe stanoxRecord
    }
  }

  it should "retrieve multiple inserted stanox records from the database (multiple insertion)" in {

    val stanoxRecord1 = getStanoxRecord()
    val stanoxRecord2 = getStanoxRecord().copy(stanoxCode = StanoxCode("12345"), description = Some("REIGATE_DESC"))

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.addRecords(List(stanoxRecord1, stanoxRecord2)).unsafeRunSync()
      val retrievedRecords = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe stanoxRecord1
      retrievedRecords(1) shouldBe stanoxRecord2
    }
  }

  it should "retrieve stanox records where crs field is not null" in {

    val stanoxRecord1 = getStanoxRecord(stanoxCode = StanoxCode("12345"))
    val stanoxRecord2 = getStanoxRecord(stanoxCode = StanoxCode("23456"), crs = None)

    withInitialState(testDatabaseConfig)(AppInitialState(stanoxRecords = List(stanoxRecord1, stanoxRecord2))) {
      fixture =>
        val retrievedRecords = fixture.stanoxTable.retrieveAllRecordsWithCRS().unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe stanoxRecord1
    }

  }

  it should "retrieve a record by stanox code" in {

    val stanoxRecord1 = getStanoxRecord(stanoxCode = StanoxCode("12345"))
    val stanoxRecord2 = getStanoxRecord(stanoxCode = StanoxCode("23456"))

    withInitialState(testDatabaseConfig)(AppInitialState(stanoxRecords = List(stanoxRecord1, stanoxRecord2))) {
      fixture =>
        val retrievedRecords = fixture.stanoxTable.stanoxRecordFor(stanoxRecord2.stanoxCode).unsafeRunSync()
        retrievedRecords.get shouldBe stanoxRecord2
    }
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val stanoxRecord1 = getStanoxRecord()

    withInitialState(testDatabaseConfig,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 2 seconds))(
      AppInitialState(stanoxRecords = List(stanoxRecord1))) { fixture =>
      val retrievedRecords1 = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()

      retrievedRecords1 should have size 1
      retrievedRecords1.head shouldBe stanoxRecord1

      fixture.stanoxTable.deleteAllRecords().unsafeRunSync()

      val retrievedRecords2 = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords2 should have size 1
      retrievedRecords2.head shouldBe stanoxRecord1

      Thread.sleep(2000)

      val retrievedRecords3 = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords3 should have size 0

    }
  }

  def getStanoxRecord(stanoxCode: StanoxCode = StanoxCode("87722"),
                      tipLocCode: TipLocCode = TipLocCode("REDHILL"),
                      crs: Option[CRS] = Some(CRS("RDH")),
                      description: Option[String] = Some("REDHILL")) =
    StanoxRecord(stanoxCode, tipLocCode, crs, description)

}
