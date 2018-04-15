package traindelays.scripts

import cats.effect.IO
import cats.syntax.flatMap._
import com.typesafe.scalalogging.StrictLogging
import org.http4s.Uri
import org.http4s.client.blaze.Http1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata._
import cats.instances.list._
import cats.syntax.traverse._

trait PopulateScheduleTable extends StrictLogging {

  def run(flushFirst: Boolean = false, downloadUris: List[Uri], config: TrainDelaysConfig) =
    withTransactor(config.databaseConfig)() { db =>
      val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val scheduleTablePrimary   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val scheduleTableSecondary = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

      def handlePopulationForSingleUri(networkRailClient: NetworkRailClient,
                                       uri: Uri,
                                       scheduleDataReader: ScheduleDataReader) =
        for {
          _ <- networkRailClient.deleteTmpFiles()
          _ <- IO(logger.info(s"Downloading schedule data from uri $uri"))
          _ <- downloadScheduleData(networkRailClient, uri)
          _ <- IO(logger.info("Unpacking schedule data"))
          _ <- networkRailClient.unpackScheduleData
          _ <- IO(logger.info("Writing schedule data records"))
          _ <- writeScheduleDataRecords(stanoxTable,
                                        scheduleTablePrimary,
                                        scheduleTableSecondary,
                                        associationTable,
                                        scheduleDataReader)
        } yield ()

      fs2.Stream.eval {
        for {
          _      <- IO(logger.info(s"Starting population of schedule table. Flush first set to $flushFirst"))
          client <- Http1Client[IO]()
          networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
          scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)
          _ <- if (flushFirst)
            IO(logger.info("Deleting all records from Schedule Table Primary")) >> scheduleTablePrimary
              .deleteAllRecords()
          else IO.unit
          _ <- if (flushFirst)
            IO(logger.info("Deleting all records from Schedule Table Secondary")) >> scheduleTableSecondary
              .deleteAllRecords()
          else IO.unit
          _ <- if (flushFirst)
            IO(logger.info("Deleting all records from Association Table")) >> associationTable.deleteAllRecords()
          else IO.unit
          _ <- if (flushFirst)
            IO(logger.info("Deleting all records from Stanox Table")) >> stanoxTable.deleteAllRecords()
          else IO.unit
          _ <- downloadUris.traverse[IO, Unit] { uri =>
            handlePopulationForSingleUri(networkRailClient, uri, scheduleDataReader)
          }
          _ <- IO(logger.info("Schedule Table population complete"))
        } yield ()
      }
    }

  protected def downloadScheduleData(networkRailClient: NetworkRailClient, uri: Uri): IO[Unit]
}
