package traindelays.ui

import cats.effect.IO
import io.circe.Json
import org.http4s.{HttpService, Request, Status, Uri}
import org.scalatest.FlatSpec
import traindelays.networkrail.subscribers.UserId
import traindelays.{DatabaseConfig, TestFeatures, UIConfig}
import org.http4s.dsl.io._
import org.scalatest.Matchers._
import org.http4s.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ServiceTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()
  val uiTestConfig                     = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails = AuthenticatedDetails(UserId("173649593023"),
                                                         "test@test.com",
                                                         Some(true),
                                                         Some("joebloggs"),
                                                         Some("Joe"),
                                                         Some("Bloggs"),
                                                         Some("GB"))

  val initialState = createDefaultInitialState()

  it should "fetch a json list of stations from /stations" in {

    withInitialState(config)(initialState) { fixture =>
      val service      = serviceFrom(fixture)
      val request      = Request[IO](method = GET, uri = Uri(path = "/stations"))
      val response     = service.orNotFound(request).unsafeRunSync()
      val jsonResponse = response.as[Json].unsafeRunSync()
      jsonResponse shouldBe Json.arr(initialState.stanoxRecords.sortBy(_.stanoxCode.value).map { stanoxRecord =>
        Json.obj(
          "key" -> Json.fromString(stanoxRecord.stanoxCode.value),
          "value" -> Json.fromString(
            s"${stanoxRecord.description.getOrElse("")} [${stanoxRecord.crs.map(_.value).getOrElse("")}]")
        )
      }: _*)
    }

  }

  private def serviceFrom(fixture: TrainDelaysTestFixture): HttpService[IO] =
    Service(fixture.scheduleTable,
            fixture.stanoxTable,
            fixture.subscriberTable,
            uiTestConfig,
            MockGoogleAuthenticator(defaultAuthenticatedDetails))
}
