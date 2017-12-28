package traindelays.networkrail

import java.nio.file.Paths

import cats.effect.IO
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.{ConfigLoader, TrainDelaysConfig}

trait IntegrationTest {

  def client = PooledHttp1Client[IO]()

  private val defaultConfig: TrainDelaysConfig = ConfigLoader.defaultConfig
  val testconfig: TrainDelaysConfig = defaultConfig.copy(
    networkRailConfig = defaultConfig.networkRailConfig.copy(
      scheduleDataConf = defaultConfig.networkRailConfig.scheduleDataConf
        .copy(
          tmpDownloadLocation = Paths.get(getClass.getResource("/southern-toc-schedule-test.gz").getPath),
          tmpUnzipLocation = Paths.get("/tmp/southern-toc-schedule-test.dat")
        )))

}
