package traindelays.networkrail.db

import java.nio.file.Paths

import cats.effect.IO
import org.http4s.Uri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.{CRS, Stanox}
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import traindelays.{DatabaseConfig, ScheduleDataConfig, TestFeatures}
import traindelays.networkrail.scheduledata._

import scala.concurrent.ExecutionContext.Implicits.global

class TipLocTableTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "insert a tiploc record into the database" in {

    withInitialState(config)() { fixture =>
      fixture.tipLocTable.addRecord(getTipLocRecord())
    }
  }

  it should "retrieve an inserted tiploc record from the database" in {

    val tipLocRecord = getTipLocRecord()

    val retrievedRecords = withInitialState(config)(AppInitialState(tiplocRecords = List(tipLocRecord))) { fixture =>
      fixture.tipLocTable.retrieveAllRecords()
    }

    retrievedRecords should have size 1
    retrievedRecords.head shouldBe tipLocRecord
  }

  it should "retrieve multiple inserted tiploc records from the database" in {

    val tipLocRecord1 = getTipLocRecord()
    val tipLocRecord2 = getTipLocRecord().copy(tipLocCode = TipLocCode("REIGATE"), description = Some("REIGATE_DESC"))

    val retrievedRecords =
      withInitialState(config)(AppInitialState(tiplocRecords = List(tipLocRecord1, tipLocRecord2))) { fixture =>
        fixture.tipLocTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe tipLocRecord1
    retrievedRecords(1) shouldBe tipLocRecord2
  }

  it should "memoize retrieval of an inserted record from the database" in {

    import scala.concurrent.duration._

    val tipLocRecord1 = getTipLocRecord()

    withInitialState(config,
                     scheduleDataConfig =
                       ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 2 seconds))(
      AppInitialState(tiplocRecords = List(tipLocRecord1))) { fixture =>
      IO {
        val retrievedRecords1 = fixture.tipLocTable.retrieveAllRecords().unsafeRunSync()

        retrievedRecords1 should have size 1
        retrievedRecords1.head shouldBe tipLocRecord1

        fixture.tipLocTable.deleteAllRecords().unsafeRunSync()

        val retrievedRecords2 = fixture.tipLocTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords2 should have size 1
        retrievedRecords2.head shouldBe tipLocRecord1

        Thread.sleep(2000)

        val retrievedRecords3 = fixture.tipLocTable.retrieveAllRecords().unsafeRunSync()
        retrievedRecords3 should have size 0
      }
    }
  }

  def getTipLocRecord(tipLocCode: TipLocCode = TipLocCode("REDHILL"),
                      stanox: Stanox = Stanox("87722"),
                      crs: CRS = CRS("RED"),
                      description: Option[String] = Some("REDHILL")) =
    TipLocRecord(tipLocCode, stanox, crs, description)

}
