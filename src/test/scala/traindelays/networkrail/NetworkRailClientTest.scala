package traindelays.networkrail

import java.nio.file.{Path, Paths}

import cats.effect.IO
import org.http4s.client.blaze.PooledHttp1Client
import org.scalatest.FlatSpec
import traindelays.ConfigLoader

//class NetworkRailClientTest extends FlatSpec {
//
//  it should "download schedule data from server" in {
//    val config            = ConfigLoader.defaultConfig
//    val client            = PooledHttp1Client[IO]()
//    val networkRailClient = NetworkRailClient(config.networkRailConfig, client)
//    networkRailClient.downloadScheduleData.unsafeRunSync()
//
//    Paths.get(config.networkRailConfig.scheduleDataConf.tmpDownloadLocation).
//  }
//}
