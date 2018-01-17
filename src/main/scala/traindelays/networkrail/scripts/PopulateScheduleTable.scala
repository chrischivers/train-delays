package traindelays.networkrail.scripts

import cats.effect.IO
import fs2.Pipe
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.ConfigLoader
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.movementdata.{CancellationLog, TrainCancellationRecord}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord, TipLocRecord}

object PopulateScheduleTable extends App {

  val config = ConfigLoader.defaultConfig
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = for {
    _ <- networkRailClient.downloadScheduleData
    _ <- networkRailClient.unpackScheduleData
    _ <- deleteAllRecords
    _ <- writeTiplocRecords
    _ <- writeScheduleRecords
  } yield ()

  private def writeTiplocRecords =
    withTransactor(config.databaseConfig)() { db =>
      val tipLocTable = TipLocTable(db)
      scheduleDataReader
        .readData[TipLocRecord]
        .to(tipLocTable.dbWriter)
    }.run

  private def writeScheduleRecords =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      val tipLocTable   = TipLocTable(db)
      scheduleDataReader
        .readData[ScheduleRecord]
        .through(recordsToLogsPipe(tipLocTable))
        .flatMap[ScheduleLog](fs2.Stream(_: _*))
        .to(scheduleTable.dbWriter)
    }.run

  private def deleteAllRecords =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      fs2.Stream.eval(scheduleTable.deleteAllRecords())
    }.run

  private def recordsToLogsPipe(tipLocTable: TipLocTable): Pipe[IO, ScheduleRecord, List[ScheduleLog]] =
    (in: fs2.Stream[IO, ScheduleRecord]) => {
      in.flatMap(x => fs2.Stream.eval(x.toScheduleLogs(tipLocTable)))

    }

  app.unsafeRunSync()

}
