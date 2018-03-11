package traindelays.scripts

import traindelays.TrainDelaysConfig
import traindelays.networkrail.db.{AssociationTable, SchedulePrimaryTable, StanoxTable, withTransactor}

object PopulateScheduleAssociationTable {

  val config = TrainDelaysConfig()

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable       = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTableMain = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val associationTable  = AssociationTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        associationRecords <- associationTable.retrieveAllRecords()

      } yield ()
    }
  }

}
