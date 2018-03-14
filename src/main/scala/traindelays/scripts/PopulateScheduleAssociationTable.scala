package traindelays.scripts

import cats.effect.IO
import traindelays.TrainDelaysConfig
import traindelays.networkrail.db._
import cats.instances.list._
import cats.syntax.traverse._

object PopulateScheduleAssociationTable extends App {

  val config = TrainDelaysConfig()

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val schedulePrimaryTable   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleSecondaryTable = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable       = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {

      associationTable.retrieveJoinOrDivideRecordsNotInSecondaryTable().flatMap {
        _.traverse { associationRecord =>
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
                _.traverse { record =>
                  scheduleSecondaryTable.addRecord(record)
                }.map(_ => ())
              }
          } yield ()
        }
      }

    }
  }
  app.compile.drain.unsafeRunSync()

}
