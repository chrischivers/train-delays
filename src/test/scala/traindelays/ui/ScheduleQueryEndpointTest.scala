package traindelays.ui

import cats.effect.IO
//import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, EntityEncoder, Request, Uri}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.{StanoxCode, TestFeatures}
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord.DaysRunPattern
import traindelays.UIConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class ScheduleQueryEndpointTest extends FlatSpec with TestFeatures {

  val uiTestConfig                                      = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails: AuthenticatedDetails = createAuthenticatedDetails()
  val initialState: AppInitialState                     = createDefaultInitialState()

  implicit val executionContext = ExecutionContext.Implicits.global

  implicit val scheduleQueryResponseEntityDecoder: EntityDecoder[IO, List[ScheduleQueryResponse]] =
    org.http4s.circe.jsonOf[IO, List[ScheduleQueryResponse]]

  val scheduleQueryRequestEntityEncoder: EntityEncoder[IO, ScheduleQueryRequest] =
    org.http4s.circe.jsonEncoderOf[IO, ScheduleQueryRequest]

  it should "return an empty list if query yields no results" in {

    val scheduleQueryRequest = ScheduleQueryRequest(
      idToken = None,
      StanoxCode("67822"),
      StanoxCode("335675"),
      DaysRunPattern.Weekdays
    )

    withInitialState(testDatabaseConfig)(initialState) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val request =
        Request[IO](method = POST,
                    uri = Uri(path = "/schedule-query"),
                    body = scheduleQueryRequestEntityEncoder.toEntity(scheduleQueryRequest).unsafeRunSync().body)
      val response              = service.orNotFound(request).unsafeRunSync()
      val scheduleQueryResponse = response.as[List[ScheduleQueryResponse]].attempt.unsafeRunSync().right.get
      scheduleQueryResponse should have size 0
    }
  }

  it should "fetch a list of schedule records given a query" in {

    val firstScheduleLogRecord = initialState.scheduleLogRecords.head
    val lastScheduleLogRecord  = initialState.scheduleLogRecords.last

    val scheduleQueryRequest = ScheduleQueryRequest(
      idToken = None,
      firstScheduleLogRecord.stanoxCode,
      lastScheduleLogRecord.stanoxCode,
      firstScheduleLogRecord.daysRunPattern
    )

    withInitialState(testDatabaseConfig)(initialState) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val request =
        Request[IO](method = POST,
                    uri = Uri(path = "/schedule-query"),
                    body = scheduleQueryRequestEntityEncoder.toEntity(scheduleQueryRequest).unsafeRunSync().body)
      val response              = service.orNotFound(request).unsafeRunSync()
      val scheduleQueryResponse = response.as[List[ScheduleQueryResponse]].attempt.unsafeRunSync().right.get
      scheduleQueryResponse should have size 1

      scheduleQueryResponse.head shouldBe ScheduleQueryResponse(
        id = 1,
        firstScheduleLogRecord.scheduleTrainId,
        firstScheduleLogRecord.atocCode,
        traindelays.networkrail.Definitions.atocToOperatorNameMapping(firstScheduleLogRecord.atocCode.get),
        firstScheduleLogRecord.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == firstScheduleLogRecord.stanoxCode).get.crs.get,
        firstScheduleLogRecord.departureTime.get,
        lastScheduleLogRecord.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == lastScheduleLogRecord.stanoxCode).get.crs.get,
        lastScheduleLogRecord.arrivalTime.get,
        firstScheduleLogRecord.daysRunPattern,
        firstScheduleLogRecord.scheduleStart,
        firstScheduleLogRecord.scheduleEnd,
        subscribed = false
      )
    }
  }
  it should "return an empty list if from/to stanox are the same " in {

    val firstScheduleLogRecord = initialState.scheduleLogRecords.head

    val scheduleQueryRequest = ScheduleQueryRequest(
      idToken = None,
      firstScheduleLogRecord.stanoxCode,
      firstScheduleLogRecord.stanoxCode,
      DaysRunPattern.Weekdays
    )

    withInitialState(testDatabaseConfig)(initialState) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val request =
        Request[IO](method = POST,
                    uri = Uri(path = "/schedule-query"),
                    body = scheduleQueryRequestEntityEncoder.toEntity(scheduleQueryRequest).unsafeRunSync().body)
      val response              = service.orNotFound(request).unsafeRunSync()
      val scheduleQueryResponse = response.as[List[ScheduleQueryResponse]].attempt.unsafeRunSync().right.get
      scheduleQueryResponse should have size 0
    }
  }
}
