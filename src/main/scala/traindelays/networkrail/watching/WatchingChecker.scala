package traindelays.networkrail.watching

import cats.effect.IO
import traindelays.networkrail.db.{MovementLogTable, WatchingTable}

trait WatchingChecker {

  def generateWatchingReports: IO[List[WatchingReport]]
}

object WatchingChecker {
  def apply(movementLogTable: MovementLogTable, watchingTable: WatchingTable) = new WatchingChecker {
    override def generateWatchingReports: IO[List[WatchingReport]] =
      for {
        watchingRecords <- watchingTable.retrieveAllRecords()
        movementLogs    <- movementLogTable.retrieveAllRecords() //TODO make scalable
      } yield {
        watchingRecords.map { watchingRecord =>
          val filteredLogs =
            movementLogs.filter(x => x.trainId == watchingRecord.trainId && x.serviceCode == watchingRecord.serviceCode)
          WatchingReport(watchingRecord, filteredLogs)
        }
      }
  }
}
