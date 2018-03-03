package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.Pipe
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.TrainDelaysConfig
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata._
import scala.language.reflectiveCalls

object PopulateScheduleTable extends App with StrictLogging {

  val config = TrainDelaysConfig()
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable   = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTable = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _ <- networkRailClient deleteTmpFiles ()
        _ <- IO.pure(logger.info("Downloading schedule data"))
        _ <- networkRailClient.downloadScheduleData
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData
        _ <- IO.pure(logger.info("Writing stanox records"))
        _ <- writeStanoxRecords(stanoxTable)
        _ <- IO.pure(logger.info("Writing schedule records"))
        _ <- writeScheduleRecords(stanoxTable, scheduleTable)
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }.run

  app.unsafeRunSync()

  private def writeStanoxRecords(stanoxTable: StanoxTable) =
    for {
      existingStanoxRecords <- stanoxTable.retrieveAllRecords(forceRefresh = true)
      _ <- scheduleDataReader
        .readData[DecodedStanoxRecord]
        .to(stanoxTable.dbUpdater)
        .run
    } yield ()

  private def writeScheduleRecords(stanoxTable: StanoxTable, scheduleTable: ScheduleTable) =
    for {
      existingStanoxRecords <- stanoxTable.retrieveAllRecords(forceRefresh = true)
      existingStanoxRecordsMap = StanoxRecord.stanoxRecordsToMap(existingStanoxRecords)

      recordsTransformationPipe: Pipe[
        IO,
        DecodedScheduleRecord,
        Either[DecodedScheduleRecord.Delete, List[ScheduleRecord]]] = (in: fs2.Stream[IO, DecodedScheduleRecord]) => {
        in.map {
          case rec @ DecodedScheduleRecord.Create(_, _, _, _, _, _, _, _) =>
            Right(rec.toScheduleLogs(existingStanoxRecordsMap))
          case rec @ DecodedScheduleRecord.Delete(_, _, _) => Left(rec)
        }
      }

      _ <- scheduleDataReader
        .readData[DecodedScheduleRecord]
        .through(recordsTransformationPipe)
        .zipWithIndex
        .observe1 {
          case (_, index) =>
            IO {
              if (index % 1000 == 0) println(index)
            }
        }
        .map { case (data, _) => data }
        .to(scheduleTable.dbUpdater)
        .run
    } yield ()
}
