package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata._

import scala.language.reflectiveCalls

object UpdatePopulateScheduleTable extends App with StrictLogging {

  val config = TrainDelaysConfig()
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = withTransactor(
    config.databaseConfig.copy(url = "jdbc:postgresql://localhost/traindelays", username = "postgres"))() { db =>
    val stanoxTable   = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTable = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _ <- networkRailClient deleteTmpFiles ()
        _ <- IO.pure(logger.info("Downloading update schedule data"))
        _ <- networkRailClient.downloadUpdateScheduleData
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData
        _ <- IO.pure(logger.info("Writing stanox records"))
        _ <- writeStanoxRecords(stanoxTable, scheduleDataReader)
        _ <- IO.pure(logger.info("Writing schedule records"))
        _ <- writeScheduleRecords(stanoxTable, scheduleTable, scheduleDataReader)
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }.run

  app.unsafeRunSync()
}
