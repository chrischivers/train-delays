package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.Http1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata._
import cats.syntax.flatMap._
import fs2.Stream

import scala.concurrent.ExecutionContext

trait PopulateScheduleTable extends StrictLogging {

  val config = TrainDelaysConfig()

  def run(flushFirst: Boolean = false)(implicit ec: ExecutionContext) = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTablePrimary   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTableSecondary = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    (for {
      _      <- Stream.eval(IO(logger.info(s"Starting population of schedule table. Flush first set to $flushFirst")))
      client <- Stream.eval(Http1Client[IO]())
      networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
      scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)
      _ <- Stream.eval(networkRailClient.deleteTmpFiles())
      _ <- Stream.eval(IO(logger.info("Downloading schedule data")))
      _ <- downloadScheduleData(networkRailClient)
      _ <- Stream.eval(IO(logger.info("Unpacking schedule data")))
      _ <- networkRailClient.unpackScheduleData
      _ <- Stream.eval[IO, Unit] {
        if (flushFirst)
          IO(logger.info("Deleting all records from Schedule Table Primary")) >> scheduleTablePrimary.deleteAllRecords()
        else IO.unit
      }
      _ <- Stream.eval[IO, Unit] {
        if (flushFirst)
          IO(logger.info("Deleting all records from Association Table")) >> associationTable.deleteAllRecords()
        else IO.unit
      }
      _ <- Stream.eval[IO, Unit] {
        if (flushFirst) IO(logger.info("Deleting all records from Stanox Table")) >> stanoxTable.deleteAllRecords()
        else IO.unit
      }
      _ <- Stream.eval(IO(logger.info("Writing schedule data records")))
      _ <- writeScheduleDataRecords(stanoxTable,
                                    scheduleTablePrimary,
                                    scheduleTableSecondary,
                                    associationTable,
                                    scheduleDataReader)
      _ <- Stream.eval(IO(logger.info("Schedule Table population complete")))
    } yield ())
      .handleErrorWith(err => fs2.Stream.eval(IO(logger.error("Error populating schedule table", err))))
  }
  protected def downloadScheduleData(networkRailClient: NetworkRailClient): fs2.Stream[IO, Unit]
}
