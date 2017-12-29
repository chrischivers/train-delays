package traindelays.networkrail

import java.nio.file.{Files, Paths}

import org.scalactic.TripleEqualsSupport
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord, TipLocRecord}

import scala.concurrent.ExecutionContext.Implicits.global

class NetworkRailClientIntegrationTest
    extends FlatSpec
    with IntegrationTest
    with TripleEqualsSupport
    with BeforeAndAfterEach {

  //TODO mock this out
  ignore should "download schedule data from server to tmp directory" in {

    val configWithTmpDownloadLocation = testconfig.networkRailConfig.copy(
      scheduleDataConf = testconfig.networkRailConfig.scheduleDataConf
        .copy(tmpDownloadLocation = Paths.get("/tmp/network-rail-test-download.gz")))

    val networkRailClient = NetworkRailClient(configWithTmpDownloadLocation, client)
    networkRailClient.downloadScheduleData.unsafeRunSync()

    val path = testconfig.networkRailConfig.scheduleDataConf.tmpDownloadLocation
    Files.exists(path) should ===(true)
    Files.size(path) should be > 0L

    cleanUpFile(configWithTmpDownloadLocation.scheduleDataConf.tmpDownloadLocation.toString)
  }

  it should "unpack downloaded schedule/tiploc data and parse json correctly" in {

    val networkRailClient = NetworkRailClient(testconfig.networkRailConfig, client)
    networkRailClient.unpackScheduleData.unsafeRunSync()
    Files.exists(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation) shouldBe true

    val scheduleDataReader = ScheduleDataReader(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation)

    val scheduleResults = scheduleDataReader.readData[ScheduleRecord].runLog.unsafeRunSync().toList
    scheduleResults.size should ===(19990)

    val tipLocResult = scheduleDataReader.readData[TipLocRecord].runLog.unsafeRunSync().toList
    tipLocResult.size should ===(11042)
  }

  it should "unpack downloaded schedule/tiploc data and persist to DB" in {

    val networkRailClient = NetworkRailClient(testconfig.networkRailConfig, client)
    networkRailClient.unpackScheduleData.unsafeRunSync()
    Files.exists(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation) shouldBe true

    val scheduleDataReader = ScheduleDataReader(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation)
    val scheduleResults    = scheduleDataReader.readData[ScheduleRecord].runLog.unsafeRunSync().toList.take(10)

    val tipLocResults = scheduleDataReader.readData[TipLocRecord].runLog.unsafeRunSync().toList.take(10)

    val retrievedScheduleRecords = db.common.withScheduleTable(testconfig.databaseConfig)(scheduleResults: _*) {
      _.retrieveAllRecords()
    }

    val retrievedTiplocRecords = db.common.withTiplocTable(testconfig.databaseConfig)(tipLocResults: _*) {
      _.retrieveAllRecords()
    }
    retrievedScheduleRecords.size should ===(scheduleResults.size)
    retrievedScheduleRecords should contain theSameElementsAs scheduleResults

    retrievedTiplocRecords.size should ===(tipLocResults.size)
    retrievedTiplocRecords should contain theSameElementsAs tipLocResults
  }

  override protected def afterEach(): Unit =
    cleanUpFile(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation.toString)

  private def cleanUpFile(location: String) =
    Paths.get(location).toFile.delete()
}
