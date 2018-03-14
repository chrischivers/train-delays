package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail._
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata._

trait ScheduleTable[A <: ScheduleRecord] extends MemoizedTable[A] {

  def deleteAllRecords(): IO[Unit]

  def deleteRecord(scheduleTrainId: ScheduleTrainId, scheduleStartDate: LocalDate, stpIndicator: StpIndicator): IO[Unit]

  def deleteRecord(associationId: Int): IO[Unit]

  def retrieveScheduleRecordsFor(from: StanoxCode,
                                 to: StanoxCode,
                                 pattern: DaysRunPattern,
                                 stpIndicator: StpIndicator): IO[List[A]]

  def retrieveScheduleRecordsFor(trainId: ScheduleTrainId, stanoxCode: StanoxCode): IO[List[A]]

  def retrieveScheduleRecordsFor(trainId: ScheduleTrainId): IO[List[A]]

  def retrieveRecordBy(id: Int): IO[Option[A]]

  def retrieveAllDistinctStanoxCodes: IO[List[StanoxCode]]

}

object ScheduleTable extends StrictLogging {

  import doobie._
  import doobie.implicits._
  import doobie.postgres.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[java.sql.Time]
    .xmap(t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute), lt => java.sql.Time.valueOf(lt))

  implicit val localTimeListMeta: Meta[List[LocalTime]] =
    Meta[List[String]].xmap(_.map(t => LocalTime.parse(t)), lt => lt.map(_.toString))

  trait ScheduleRecord {
    val id: Option[Int]
    val scheduleTrainId: ScheduleTrainId
    val serviceCode: ServiceCode
    val stpIndicator: StpIndicator
    val trainCategory: Option[TrainCategory]
    val trainStatus: Option[TrainStatus]
    val atocCode: Option[AtocCode]
    val stopSequence: Int
    val stanoxCode: StanoxCode
    val subsequentStanoxCodes: List[StanoxCode]
    val subsequentArrivalTimes: List[LocalTime]
    val monday: Boolean
    val tuesday: Boolean
    val wednesday: Boolean
    val thursday: Boolean
    val friday: Boolean
    val saturday: Boolean
    val sunday: Boolean
    val daysRunPattern: DaysRunPattern
    val scheduleStart: LocalDate
    val scheduleEnd: LocalDate
    val locationType: LocationType
    val arrivalTime: Option[LocalTime]
    val departureTime: Option[LocalTime]

    val insertValuesFragment: doobie.Fragment
    def insertValuesBaseFragment: doobie.Fragment =
      fr"$scheduleTrainId, $serviceCode, $stpIndicator, $trainCategory, " ++
        fr"$trainStatus, $atocCode, $stopSequence, $stanoxCode, $subsequentStanoxCodes, $subsequentArrivalTimes, $monday, " ++
        fr"$tuesday, $wednesday, $thursday, $friday, $saturday, $sunday, $daysRunPattern, $scheduleStart, $scheduleEnd, " ++
        fr"$locationType, $arrivalTime, $departureTime"
  }

  object ScheduleRecord {
    val baseInsertFieldsFragment
      : doobie.Fragment = fr"schedule_train_id, service_code, stp_indicator, train_category, " ++
      fr"train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes, subsequent_arrival_times, " ++
      fr"monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, schedule_start, " ++
      fr"schedule_end, location_type, arrival_time, departure_time"

    val baseSelectFieldsFragment
      : doobie.Fragment = fr"id, schedule_train_id, service_code, stp_indicator, train_category, " ++
      fr"train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes, subsequent_arrival_times, " ++
      fr"monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, schedule_start, " ++
      fr"schedule_end, location_type, arrival_time, departure_time"
  }

  case class ScheduleRecordPrimary(id: Option[Int],
                                   scheduleTrainId: ScheduleTrainId,
                                   serviceCode: ServiceCode,
                                   stpIndicator: StpIndicator,
                                   trainCategory: Option[TrainCategory],
                                   trainStatus: Option[TrainStatus],
                                   atocCode: Option[AtocCode],
                                   stopSequence: Int,
                                   stanoxCode: StanoxCode,
                                   subsequentStanoxCodes: List[StanoxCode],
                                   subsequentArrivalTimes: List[LocalTime],
                                   monday: Boolean,
                                   tuesday: Boolean,
                                   wednesday: Boolean,
                                   thursday: Boolean,
                                   friday: Boolean,
                                   saturday: Boolean,
                                   sunday: Boolean,
                                   daysRunPattern: DaysRunPattern,
                                   scheduleStart: LocalDate,
                                   scheduleEnd: LocalDate,
                                   locationType: LocationType,
                                   arrivalTime: Option[LocalTime],
                                   departureTime: Option[LocalTime])
      extends ScheduleRecord {

    override val insertValuesFragment: doobie.Fragment = super.insertValuesBaseFragment
  }

  object ScheduleRecordPrimary {
    val insertFieldsFragment: doobie.Fragment = ScheduleRecord.baseInsertFieldsFragment
    val selectFieldsFragment: doobie.Fragment = ScheduleRecord.baseSelectFieldsFragment
  }

  case class ScheduleRecordSecondary(id: Option[Int],
                                     scheduleTrainId: ScheduleTrainId,
                                     serviceCode: ServiceCode,
                                     stpIndicator: StpIndicator,
                                     trainCategory: Option[TrainCategory],
                                     trainStatus: Option[TrainStatus],
                                     atocCode: Option[AtocCode],
                                     stopSequence: Int,
                                     stanoxCode: StanoxCode,
                                     subsequentStanoxCodes: List[StanoxCode],
                                     subsequentArrivalTimes: List[LocalTime],
                                     monday: Boolean,
                                     tuesday: Boolean,
                                     wednesday: Boolean,
                                     thursday: Boolean,
                                     friday: Boolean,
                                     saturday: Boolean,
                                     sunday: Boolean,
                                     daysRunPattern: DaysRunPattern,
                                     scheduleStart: LocalDate,
                                     scheduleEnd: LocalDate,
                                     locationType: LocationType,
                                     arrivalTime: Option[LocalTime],
                                     departureTime: Option[LocalTime],
                                     associationId: Int)
      extends ScheduleRecord {
    override val insertValuesFragment: doobie.Fragment = super.insertValuesBaseFragment ++ fr", $associationId"
  }

  object ScheduleRecordSecondary {
    val insertFieldsFragment: doobie.Fragment = ScheduleRecord.baseInsertFieldsFragment ++ fr", association_id"
    val selectFieldsFragment: doobie.Fragment = ScheduleRecord.baseSelectFieldsFragment ++ fr", association_id"
  }

  def addScheduleLogRecord(log: ScheduleRecord): Update0 = {
    val (tableName, fields) = log match {
      case _: ScheduleRecordPrimary => (SchedulePrimaryTable.tableName, ScheduleRecordPrimary.insertFieldsFragment)
      case _: ScheduleRecordSecondary =>
        (ScheduleSecondaryTable.tableName, ScheduleRecordSecondary.insertFieldsFragment)
    }
    (fr"INSERT INTO " ++ tableName ++ fr" (" ++ fields ++ fr") VALUES (" ++ log.insertValuesFragment ++ fr")").update
  }
  def deleteAllScheduleRecords(tableName: doobie.Fragment): Update0 =
    (fr"DELETE FROM" ++ tableName).update

  def allScheduleRecordsFragment(tableName: doobie.Fragment, selectFields: doobie.Fragment): Fragment =
    fr"SELECT " ++ selectFields ++ fr" FROM " ++ tableName

  def scheduleRecordsForFragment(
      fromStation: StanoxCode,
      toStation: StanoxCode,
      daysRunPattern: DaysRunPattern,
      stpIndicator: StpIndicator)(tableName: doobie.Fragment, selectFields: doobie.Fragment): Fragment =
    fr"SELECT " ++ selectFields ++ fr" FROM " ++ tableName ++
      fr"WHERE stanox_code = $fromStation" ++
      fr"AND days_run_pattern = $daysRunPattern" ++
      fr"AND $toStation = ANY(subsequent_stanox_codes)" ++
      fr"AND stp_indicator = $stpIndicator"

  def scheduleRecordForFragment(id: Int)(tableName: doobie.Fragment, selectFields: doobie.Fragment): Fragment =
    fr"SELECT " ++ selectFields ++ fr" FROM " ++ tableName ++
      fr"WHERE id = $id"

  def scheduleRecordsForFragment(trainId: ScheduleTrainId, fromStation: StanoxCode)(
      tableName: doobie.Fragment,
      selectFields: doobie.Fragment): Fragment =
    fr"SELECT " ++ selectFields ++ fr" FROM " ++ tableName ++
      fr"WHERE schedule_train_id = $trainId" ++
      fr"AND stanox_code = $fromStation"

  def scheduleRecordsForFragment(trainId: ScheduleTrainId)(tableName: doobie.Fragment,
                                                           selectFields: doobie.Fragment): Fragment =
    fr"SELECT " ++ selectFields ++ fr" FROM " ++ tableName ++
      fr"WHERE schedule_train_id = $trainId"

  def deleteRecord(scheduleTrainId: ScheduleTrainId, scheduleStartDate: LocalDate, stpIndicator: StpIndicator)(
      tableName: doobie.Fragment): Update0 =
    (fr"DELETE FROM " ++ tableName ++
      fr"WHERE schedule_train_id = ${scheduleTrainId}" ++
      fr"AND schedule_start = ${scheduleStartDate}" ++
      fr"AND stp_indicator = ${stpIndicator}").update

  def distinctStanoxCodes(tableName: doobie.Fragment) =
    (fr"SELECT DISTINCT stanox_code FROM " ++ tableName).query[StanoxCode]
}
