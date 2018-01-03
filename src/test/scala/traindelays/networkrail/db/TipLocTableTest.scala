package traindelays.networkrail.db

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{DatabaseConfig, TestFeatures}
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
    val tipLocRecord2 = getTipLocRecord().copy(tipLocCode = "REIGATE", description = Some("REIGATE_DESC"))

    val retrievedRecords =
      withInitialState(config)(AppInitialState(tiplocRecords = List(tipLocRecord1, tipLocRecord2))) { fixture =>
        fixture.tipLocTable.retrieveAllRecords()
      }

    retrievedRecords should have size 2
    retrievedRecords.head shouldBe tipLocRecord1
    retrievedRecords(1) shouldBe tipLocRecord2
  }

  def getTipLocRecord(tipLocCode: String = "REDHILL",
                      stanox: String = "87722",
                      description: Option[String] = Some("REDHILL")) =
    TipLocRecord(tipLocCode, stanox, description)

}
