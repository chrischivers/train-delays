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

  def run(flushFirst: Boolean = false) = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTablePrimary   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTableSecondary = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _      <- IO(logger.info("Starting population of schedule table"))
        client <- Http1Client[IO]()
        networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
        scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)
        _ <- networkRailClient.deleteTmpFiles()
        _ <- IO.pure(logger.info("Downloading schedule data"))
        _ <- downloadScheduleData(networkRailClient).compile.drain
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData.compile.drain
        _ <- if (flushFirst)
          scheduleTablePrimary
            .deleteAllRecords()
            .flatMap(_ => IO(logger.info("Deleted all records from Schedule Table Primary")))
        else IO.unit
        _ <- if (flushFirst)
          associationTable
            .deleteAllRecords()
            .flatMap(_ => IO(logger.info("Deleted all records from Association Table")))
        else IO.unit
        _ <- if (flushFirst)
          stanoxTable.deleteAllRecords().flatMap(_ => IO(logger.info("Deleted all records from Stanox Table")))
        else IO.unit
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

  run().compile.drain.unsafeRunSync()

  protected def downloadScheduleData(networkRailClient: NetworkRailClient): fs2.Stream[IO, Unit]
}
