package traindelays.networkrail

import java.nio.file.Paths
import java.util

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.networkrail.movementdata.MovementMessageHandler
import traindelays.stomp.StompHandler
import traindelays.{ConfigLoader, DatabaseConfig, TrainDelaysConfig}

import scala.collection.mutable.ListBuffer

trait IntegrationTest {

  def client = PooledHttp1Client[IO]()

  private val defaultConfig: TrainDelaysConfig = ConfigLoader.defaultConfig
  val testconfig: TrainDelaysConfig = defaultConfig.copy(
    networkRailConfig = defaultConfig.networkRailConfig.copy(
      scheduleData = defaultConfig.networkRailConfig.scheduleData
        .copy(
          tmpDownloadLocation = Paths.get(getClass.getResource("/southern-toc-schedule-test.gz").getPath),
          tmpUnzipLocation = Paths.get("/tmp/southern-toc-schedule-test.dat")
        )),
    databaseConfig = DatabaseConfig(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost/traindelays",
      "postgres",
      "",
      List("db/migration/common"),
      10
    )
  )
}
