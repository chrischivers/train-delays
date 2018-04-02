package traindelays.scripts

import cats.effect.IO
import traindelays.TrainDelaysConfig
import traindelays.networkrail.db._
import cats.instances.list._
import cats.syntax.traverse._
import com.typesafe.scalalogging.StrictLogging
import cats.syntax.flatMap._

object PopulateSecondaryScheduleTable extends StrictLogging {

  val config = TrainDelaysConfig.defaultConfig

  def run(flushFirst: Boolean = false) = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val schedulePrimaryTable   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleSecondaryTable = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _ <- IO(logger.info(s"Starting population of secondary schedule table. Flush first set to $flushFirst"))
        _ <- if (flushFirst)
          IO(logger.info("Deleting all records from Schedule Table Secondary")) >> scheduleSecondaryTable
            .deleteAllRecords()
        else IO.unit
        recordsNotInSecondary <- associationTable.retrieveJoinOrDivideRecordsNotInSecondaryTable()
        _ <- recordsNotInSecondary.traverse[IO, Unit] { associationRecord =>
          for {
            scheduleRecordsForMainId <- schedulePrimaryTable.retrieveScheduleRecordsFor(
              associationRecord.mainScheduleTrainId)
            scheduleRecordsForAssociatedId <- schedulePrimaryTable.retrieveScheduleRecordsFor(
              associationRecord.associatedScheduleTrainId)
            stanoxRecordForAssociationLocation <- stanoxTable.stanoxRecordsFor(associationRecord.location)
            _ <- associationRecord
              .toSecondaryScheduleRecords(scheduleRecordsForMainId,
                                          scheduleRecordsForAssociatedId,
                                          stanoxRecordForAssociationLocation)
              .fold(IO.unit) {
                _.traverse[IO, Unit] { record =>
                  scheduleSecondaryTable.safeAddRecord(record)
                }.map(_ => ())
              }
          } yield ()
        }
        _ <- IO(logger.info("Finished population of secondary schedule table"))
      } yield ()

    }
  }
}
