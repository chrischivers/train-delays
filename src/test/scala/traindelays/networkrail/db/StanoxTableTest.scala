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

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a stanox record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.stanoxTable.addRecord(getStanoxRecord())
    }
  }

  it should "retrieve an inserted stanox record from the database" in {

    val stanoxRecord = getStanoxRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(stanoxRecords = List(stanoxRecord))) { fixture =>
      fixture.stanoxTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe stanoxRecord
  }

  it should "retrieve multiple inserted stanox records from the database" in {

    val stanoxRecord1 = getStanoxRecord()
    val stanoxRecord2 = getStanoxRecord().copy(stanoxCode = StanoxCode("12345"), description = Some("REIGATE_DESC"))

    val retrievedRecords =
      withInitialState(config)(AppInitialState(stanoxRecords = List(stanoxRecord1, stanoxRecord2))) { fixture =>
        fixture.stanoxTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe stanoxRecord1
    retrievedRecords(1) shouldBe stanoxRecord2
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val stanoxRecord1 = getStanoxRecord()

    withInitialState(config,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 2 seconds))(
      AppInitialState(stanoxRecords = List(stanoxRecord1))) { fixture =>
      IO {
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
  }

  def getStanoxRecord(stanoxCode: StanoxCode = StanoxCode("87722"),
                      tipLocCode: TipLocCode = TipLocCode("REDHILL"),
                      crs: CRS = CRS("RDH"),
                      description: Option[String] = Some("REDHILL")) =
    StanoxRecord(stanoxCode, tipLocCode, crs, description)

}
