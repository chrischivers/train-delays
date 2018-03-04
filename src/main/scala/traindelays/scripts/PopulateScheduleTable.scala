package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata._

trait PopulateScheduleTable extends App with StrictLogging {

  val config = TrainDelaysConfig()

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable      = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTable    = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    val client             = PooledHttp1Client[IO]()
    val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
    val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

    fs2.Stream.eval {
      for {
        _ <- networkRailClient.deleteTmpFiles()
        _ <- IO.pure(logger.info("Downloading schedule data"))
        _ <- downloadScheduleData(networkRailClient)
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData
        _ <- IO.pure(logger.info("Writing schedule data records"))
        _ <- writeScheduleDataRecords(stanoxTable, scheduleTable, associationTable, scheduleDataReader)
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }.run

  app.unsafeRunSync()

  protected def downloadScheduleData(networkRailClient: NetworkRailClient): IO[Unit]
}
