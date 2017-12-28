package traindelays.networkrail

import java.nio.file.{Files, Paths}

import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord, TipLocRecord}

class NetworkRailClientIntegrationTest extends FlatSpec with IntegrationTest {

  //TODO mock this out
  ignore should "download schedule data from server to tmp directory" in {

    val configWithTmpDownloadLocation = testconfig.networkRailConfig.copy(
      scheduleDataConf = testconfig.networkRailConfig.scheduleDataConf
        .copy(tmpDownloadLocation = Paths.get("/tmp/network-rail-test-download.gz")))

    val networkRailClient = NetworkRailClient(configWithTmpDownloadLocation, client)
    networkRailClient.downloadScheduleData.unsafeRunSync()

    val path = testconfig.networkRailConfig.scheduleDataConf.tmpDownloadLocation
    Files.exists(path) shouldBe true
    Files.size(path) should be > 0L

    cleanUpFile(configWithTmpDownloadLocation.scheduleDataConf.tmpDownloadLocation.toString)
  }

  it should "unpack downloaded schedule data and parse json correctly" in {

    val networkRailClient = NetworkRailClient(testconfig.networkRailConfig, client)
    networkRailClient.unpackScheduleData.unsafeRunSync()
    Files.exists(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation) shouldBe true

    val scheduleDataReader = ScheduleDataReader(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation)

    val scheduleResult = scheduleDataReader.readData[ScheduleRecord].runLog.unsafeRunSync().toList
    scheduleResult.size shouldBe 19990

    val tipLocResult = scheduleDataReader.readData[TipLocRecord].runLog.unsafeRunSync().toList
    tipLocResult.size shouldBe 11042

    cleanUpFile(testconfig.networkRailConfig.scheduleDataConf.tmpUnzipLocation.toString)
  }

  def cleanUpFile(location: String) =
    Paths.get(location).toFile.delete()
}
