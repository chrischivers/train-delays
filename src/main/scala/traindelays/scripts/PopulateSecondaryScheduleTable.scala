package traindelays.scripts

import cats.effect.IO
import traindelays.TrainDelaysConfig
import traindelays.networkrail.db._
import cats.instances.list._
import cats.syntax.traverse._
import com.typesafe.scalalogging.StrictLogging

object PopulateSecondaryScheduleTable extends App with StrictLogging {

  val config = TrainDelaysConfig()

  def run(flushFirst: Boolean = false) = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val schedulePrimaryTable   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleSecondaryTable = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _ <- IO(logger.info("Starting population of secondary schedule table"))
        _ <- if (flushFirst)
          scheduleSecondaryTable
            .deleteAllRecords()
            .flatMap(_ => IO(logger.info("Deleted all records from Schedule Table Secondary")))
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
                  scheduleSecondaryTable.addRecord(record)
                }.map(_ => ())
              }
          } yield ()
        }
      } yield ()

    }
  }

  run().compile.drain.unsafeRunSync()

}
