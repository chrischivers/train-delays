package traindelays.networkrail

import traindelays.networkrail.movementdata.MovementLog

package object watching {

  case class WatchingRecord(id: Option[Int], userId: String, trainId: String, serviceCode: String, stanox: String)

  case class WatchingReport(watchingRecord: WatchingRecord, movementLogs: List[MovementLog])
}
