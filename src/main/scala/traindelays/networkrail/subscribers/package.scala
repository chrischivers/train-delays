package traindelays.networkrail

import traindelays.networkrail.movementdata.MovementLog

package object subscribers {

  case class SubscriberRecord(id: Option[Int],
                              userId: String,
                              email: String,
                              trainId: String,
                              serviceCode: String,
                              stanox: String)

  case class SubscriberReport(subscriberRecord: SubscriberRecord, movementLogs: List[MovementLog])
}
