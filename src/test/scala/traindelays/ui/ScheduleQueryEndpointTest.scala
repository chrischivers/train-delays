package traindelays.ui

import cats.effect.IO
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, EntityEncoder, Request, Uri}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.{TestFeatures, UIConfig}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ScheduleQueryEndpointTest extends FlatSpec with TestFeatures {

  val uiTestConfig                                      = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails: AuthenticatedDetails = createAuthenticatedDetails()
  val initialState: AppInitialState                     = createDefaultInitialState()

  implicit val scheduleQueryResponseEntityDecoder: EntityDecoder[IO, List[ScheduleQueryResponse]] =
    org.http4s.circe.jsonOf[IO, List[ScheduleQueryResponse]]

  val scheduleQueryRequestEntityEncoder: EntityEncoder[IO, ScheduleQueryRequest] =
    org.http4s.circe.jsonEncoderOf[IO, ScheduleQueryRequest]

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
        traindelays.networkrail.tocs.tocs.mapping(firstScheduleLogRecord.atocCode),
        firstScheduleLogRecord.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == firstScheduleLogRecord.stanoxCode).get.crs.get,
        timeFormatter.format(firstScheduleLogRecord.departureTime.get),
        lastScheduleLogRecord.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == lastScheduleLogRecord.stanoxCode).get.crs.get,
        timeFormatter.format(lastScheduleLogRecord.arrivalTime.get),
        firstScheduleLogRecord.daysRunPattern,
        "Current",
        dateFormatter.format(firstScheduleLogRecord.scheduleEnd),
        subscribed = false
      )
    //TODO

    }
  }

}