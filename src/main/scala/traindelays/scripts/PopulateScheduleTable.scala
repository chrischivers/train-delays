package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.Http1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata._

trait PopulateScheduleTable extends App with StrictLogging {

  val config = TrainDelaysConfig()

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTablePrimary   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTableSecondary = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        client <- Http1Client[IO]()
        networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
        scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)
        _ <- networkRailClient.deleteTmpFiles()
        _ <- IO.pure(logger.info("Downloading schedule data"))
        _ <- downloadScheduleData(networkRailClient).compile.drain
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData.compile.drain
        _ <- IO.pure(logger.info("Writing schedule data records"))
        _ <- writeScheduleDataRecords(stanoxTable,
                                      scheduleTablePrimary,
                                      scheduleTableSecondary,
                                      associationTable,
                                      scheduleDataReader).compile.drain
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }

  app.compile.drain.unsafeRunSync()

  protected def downloadScheduleData(networkRailClient: NetworkRailClient): fs2.Stream[IO, Unit]
}
