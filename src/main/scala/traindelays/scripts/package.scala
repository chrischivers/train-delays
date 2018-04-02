package traindelays

import cats.effect.IO
import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordPrimary, ScheduleRecordSecondary}
import traindelays.networkrail.db.{AssociationTable, ScheduleTable, StanoxTable}
import traindelays.networkrail.scheduledata._

package object scripts extends StrictLogging {

  def writeScheduleDataRecords(stanoxTable: StanoxTable,
                               scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
                               scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
                               associationTable: AssociationTable,
                               scheduleDataReader: ScheduleDataReader): IO[Unit] = {
    val dbUpdater = databaseHandler(scheduleTablePrimary, scheduleTableSecondary, stanoxTable, associationTable)

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

  def databaseHandler(scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
                      scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
                      stanoxTable: StanoxTable,
                      associationTable: AssociationTable): fs2.Sink[IO, DecodedRecord] =
    fs2.Sink {

      case rec: DecodedScheduleRecord.Create =>
        rec.toScheduleLogs(stanoxTable).flatMap(_.traverse(scheduleTablePrimary.safeAddRecord).void)
      case rec: DecodedScheduleRecord.Delete =>
        scheduleTablePrimary.deleteRecord(rec.scheduleTrainId, rec.scheduleStartDate, rec.stpIndicator)
      case rec: DecodedStanoxRecord.Create      => stanoxTable.safeAddRecord(rec.toStanoxRecord)
      case rec: DecodedStanoxRecord.Update      => stanoxTable.updateRecord(rec.toStanoxRecord)
      case rec: DecodedStanoxRecord.Delete      => stanoxTable.deleteRecord(rec.tipLocCode)
      case rec: DecodedAssociationRecord.Create => rec.toAssociationRecord.fold(IO.unit)(associationTable.safeAddRecord)
      case rec: DecodedAssociationRecord.Delete =>
        for {
          toDelete <- associationTable.retrieveRecordFor(rec.mainScheduleTrainId,
                                                         rec.associatedScheduleTrainId,
                                                         rec.associationStartDate,
                                                         rec.stpIndicator,
                                                         rec.location)
          _ <- toDelete.flatMap(rec => rec.id).fold(IO.unit)(id => scheduleTableSecondary.deleteRecord(id))
          _ <- toDelete.flatMap(rec => rec.id).fold(IO.unit)(id => associationTable.deleteRecordBy(id))
        } yield ()
      case _: OtherDecodedRecord => IO.unit
      case _ =>
        logger.error("unhandled record type")
        IO.raiseError(new RuntimeException("Unhandled record type"))
    }
}
