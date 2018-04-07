package traindelays.networkrail

import java.time.LocalTime

import doobie.util.meta.Meta
import io.circe.Decoder
import traindelays.networkrail.movementdata.MovementLog
import traindelays.networkrail.scheduledata.{DaysRunPattern, ScheduleTrainId}

package object subscribers {

  case class UserId(value: String)
  object UserId {
    implicit val decoder: Decoder[UserId] = Decoder.decodeString.map(UserId(_))

    implicit val meta: Meta[UserId] =
      Meta[String].xmap(UserId(_), _.value)
  }

  case class SubscriberRecord(id: Option[Int],
                              userId: UserId,
                              emailAddress: String,
                              emailVerified: Option[Boolean],
                              name: Option[String],
                              firstName: Option[String],
                              familyName: Option[String],
                              locale: Option[String],
                              scheduleTrainId: ScheduleTrainId,
                              serviceCode: ServiceCode,
                              fromStanoxCode: StanoxCode,
                              fromCRS: CRS,
                              departureTime: LocalTime,
                              toStanoxCode: StanoxCode,
                              toCRS: CRS,
                              arrivalTime: LocalTime,
                              daysRunPattern: DaysRunPattern)

  case class SubscriberReport(subscriberRecord: SubscriberRecord, movementLogs: List[MovementLog])
}
