package traindelays.ui

import cats.effect.IO
import io.circe.Json
import org.http4s.dsl.io._
import org.http4s.{EntityBody, EntityDecoder, EntityEncoder, Request, Uri}
import org.scalatest.FlatSpec
import traindelays.{DatabaseConfig, TestFeatures, UIConfig}
import org.http4s.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ScheduleQueryEndpointTest extends FlatSpec with TestFeatures {

  val uiTestConfig                = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails = createAuthenticatedDetails()
  val initialState                = createDefaultInitialState()

  implicit val scheduleQueryResponseEntityDecoder: EntityDecoder[IO, ScheduleQueryResponse] =
    org.http4s.circe.jsonOf[IO, ScheduleQueryResponse]

  val scheduleQueryRequestEntityEncoder: EntityEncoder[IO, ScheduleQueryRequest] =
    org.http4s.circe.jsonEncoderOf[IO, ScheduleQueryRequest]

  it should "fetch a list of schedule records given a query" in {

    val scheduleQueryRequest = ScheduleQueryRequest(
      idToken = None,
      initialState.scheduleLogRecords.head.stanoxCode,
      initialState.scheduleLogRecords.last.stanoxCode,
      initialState.scheduleLogRecords.head.daysRunPattern
    )

    withInitialState(testDatabaseConfig)(initialState) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val request =
        Request[IO](method = POST,
                    uri = Uri(path = "/schedule-query"),
                    body = scheduleQueryRequestEntityEncoder.toEntity(scheduleQueryRequest).unsafeRunSync().body)
      val response     = service.orNotFound(request).unsafeRunSync()
      val jsonResponse = response.as[ScheduleQueryResponse].unsafeRunSync()
      println(jsonResponse)
    }
  }

}
