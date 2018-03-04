package traindelays

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail.db.{AssociationTable, ScheduleTable, StanoxTable}
import traindelays.networkrail.scheduledata._

package object scripts extends StrictLogging {

  def writeScheduleDataRecords(stanoxTable: StanoxTable,
                               scheduleTable: ScheduleTable,
                               associationTable: AssociationTable,
                               scheduleDataReader: ScheduleDataReader): IO[Unit] = {
    val dbUpdater = databaseHandler(scheduleTable, stanoxTable, associationTable)

    scheduleDataReader.readData
      .through(progressLogger)
      .to(dbUpdater)
      .compile
      .drain
  }

  private def progressLogger[A](stream: fs2.Stream[IO, A]): fs2.Stream[IO, A] =
    stream.zipWithIndex
      .observe1 {
        case (_, index) => IO(if (index % 1000 == 0) logger.info(s"Reading schedule data: $index records processed"))
      }
      .map { case (data, _) => data }

  def databaseHandler(scheduleTable: ScheduleTable,
                      stanoxTable: StanoxTable,
                      associationTable: AssociationTable): fs2.Sink[IO, DecodedRecord] = fs2.Sink {

    case rec: DecodedScheduleRecord.Create => rec.toScheduleLogs(stanoxTable).flatMap(scheduleTable.addRecords)
    case rec: DecodedScheduleRecord.Delete =>
      scheduleTable.deleteRecord(rec.scheduleTrainId, rec.scheduleStartDate, rec.stpIndicator)
    case rec: DecodedStanoxRecord.Create      => stanoxTable.addRecord(rec.toStanoxRecord)
    case rec: DecodedStanoxRecord.Update      => stanoxTable.updateRecord(rec.toStanoxRecord)
    case rec: DecodedStanoxRecord.Delete      => stanoxTable.deleteRecord(rec.tipLocCode)
    case rec: DecodedAssociationRecord.Create => rec.toAssociationRecord.fold(IO.unit)(associationTable.addRecord)
    case rec: DecodedAssociationRecord.Delete =>
      associationTable.deleteRecord(rec.mainScheduleTrainId,
                                    rec.associatedScheduleTrainId,
                                    rec.associationStartDate,
                                    rec.stpIndicator,
                                    rec.location)
    case _: OtherDecodedRecord => IO.unit
    case _                     => throw new RuntimeException("Unhandled record type")
  }
}
