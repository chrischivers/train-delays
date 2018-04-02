package traindelays.networkrail.db

import java.nio.file.Paths

import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail._
import traindelays.ScheduleDataConfig
import traindelays.networkrail.scheduledata.DecodedStanoxRecord

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

class StanoxTableTest extends FlatSpec with TestFeatures {

  it should "retrieve an inserted stanox record from the database (single insertion)" in {

    val stanoxRecord = createStanoxRecord()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.safeAddRecord(stanoxRecord).unsafeRunSync()
      val retrievedRecords = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe stanoxRecord
    }
  }

  it should "insert and retrieve a stanox record which is in the primary list" in {

    val primary             = Definitions.primaryStanoxTiplocCombinations.head
    val decodedStanoxRecord = createDecodedStanoxCreateRecord(stanoxCode = Some(primary._1), tipLocCode = primary._2)

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.safeAddRecord(decodedStanoxRecord.toStanoxRecord).unsafeRunSync()
      val retrievedRecords = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 1
      retrievedRecords.head shouldBe decodedStanoxRecord.toStanoxRecord
      retrievedRecords.head.primary shouldBe Some(true)
    }
  }

  it should "retrieve multiple inserted stanox records from the database (multiple insertion)" in {

    val stanoxRecord1 = createStanoxRecord()
    val stanoxRecord2 =
      createStanoxRecord().copy(stanoxCode = Some(StanoxCode("12345")), description = Some("REIGATE_DESC"))

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.addRecords(List(stanoxRecord1, stanoxRecord2)).unsafeRunSync()
      val retrievedRecords = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords should have size 2
      retrievedRecords.head shouldBe stanoxRecord1
      retrievedRecords(1) shouldBe stanoxRecord2
    }
  }

  it should "retrieve stanox records where crs field is not null" in {

    val stanoxRecord1 = createStanoxRecord(stanoxCode = Some(StanoxCode("12345")))
    val stanoxRecord2 = createStanoxRecord(stanoxCode = Some(StanoxCode("23456")), crs = None)

    withInitialState(testDatabaseConfig)(AppInitialState(stanoxRecords = List(stanoxRecord1, stanoxRecord2))) {
      fixture =>
        val retrievedRecords = fixture.stanoxTable.retrieveAllNonEmptyRecords().unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe stanoxRecord1
    }

  }

  it should "retrieve a record by stanox code" in {

    val stanoxRecord1 = createStanoxRecord(stanoxCode = Some(StanoxCode("12345")))
    val stanoxRecord2 = createStanoxRecord(stanoxCode = Some(StanoxCode("23456")))

    withInitialState(testDatabaseConfig)(AppInitialState(stanoxRecords = List(stanoxRecord1, stanoxRecord2))) {
      fixture =>
        val retrievedRecords = fixture.stanoxTable.stanoxRecordsFor(stanoxRecord2.stanoxCode.get).unsafeRunSync()
        retrievedRecords should have size 1
        retrievedRecords.head shouldBe stanoxRecord2
    }
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val stanoxRecord1 = createStanoxRecord()

    withInitialState(testDatabaseConfig,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""),
                                          Uri.unsafeFromString(""),
                                          Paths.get(""),
                                          Paths.get(""),
                                          2 seconds))(AppInitialState(stanoxRecords = List(stanoxRecord1))) { fixture =>
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
  it should "delete an inserted stanox record from the database" in {

    val stanoxRecord = createStanoxRecord()

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.safeAddRecord(stanoxRecord).unsafeRunSync()
      val retrievedRecords1 = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords1 should have size 1
      fixture.stanoxTable.deleteRecord(stanoxRecord.tipLocCode).unsafeRunSync()
      val retrievedRecords2 = fixture.stanoxTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecords2 should have size 0
    }
  }

  it should "update an inserted stanox record in the database" in {

    val stanoxRecord1 = createStanoxRecord()
    val stanoxRecord2 = stanoxRecord1.copy(crs = Some(CRS("UYT")))

    withInitialState(testDatabaseConfig)() { fixture =>
      fixture.stanoxTable.safeAddRecord(stanoxRecord1).unsafeRunSync()
      val retrievedRecords1 = fixture.stanoxTable.retrieveAllRecords().unsafeRunSync()
      retrievedRecords1.head shouldBe stanoxRecord1
      fixture.stanoxTable.updateRecord(stanoxRecord2).unsafeRunSync()
      val retrievedRecords2 = fixture.stanoxTable.retrieveAllRecords(forceRefresh = true).unsafeRunSync()
      retrievedRecords2 should have size 1
      retrievedRecords2.head shouldBe stanoxRecord2
    }
  }

  def createStanoxRecord(stanoxCode: Option[StanoxCode] = Some(StanoxCode("87722")),
                         tipLocCode: TipLocCode = TipLocCode("REDHILL"),
                         crs: Option[CRS] = Some(CRS("RDH")),
                         description: Option[String] = Some("REDHILL")) =
    StanoxRecord(tipLocCode, stanoxCode, crs, description)

  def createDecodedStanoxCreateRecord(stanoxCode: Option[StanoxCode] = Some(StanoxCode("87722")),
                                      tipLocCode: TipLocCode = TipLocCode("REDHILL"),
                                      crs: Option[CRS] = Some(CRS("RDH")),
                                      description: Option[String] = Some("REDHILL")) =
    DecodedStanoxRecord.Create(tipLocCode, stanoxCode, crs, description)

}
