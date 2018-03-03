package traindelays

import cats.effect.IO
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db.{ScheduleTable, StanoxTable}
import traindelays.networkrail.scheduledata.{DecodedScheduleRecord, DecodedStanoxRecord, ScheduleDataReader}

package object scripts {

  def writeStanoxRecords(stanoxTable: StanoxTable, scheduleDataReader: ScheduleDataReader) =
    scheduleDataReader
      .readData[DecodedStanoxRecord]
      .to(stanoxTable.dbUpdater)
      .run

  def writeScheduleRecords(stanoxTable: StanoxTable,
                           scheduleTable: ScheduleTable,
                           scheduleDataReader: ScheduleDataReader) =
    for {
      existingStanoxRecords <- stanoxTable.retrieveAllRecords(forceRefresh = true)
      existingStanoxRecordsMap = StanoxRecord.stanoxRecordsToMap(existingStanoxRecords)

      recordsTransformationPipe: fs2.Pipe[
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
