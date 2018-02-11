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
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}
import traindelays.networkrail.scheduledata.StanoxRecord

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class StationsEndpointTest extends FlatSpec with TestFeatures {

  val uiTestConfig                = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails = createAuthenticatedDetails()
  val initialState                = createDefaultInitialState()

  it should "fetch a json list of active stations from /stations" in {

    withInitialState(testDatabaseConfig)(initialState) { fixture =>
      val service      = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
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

  it should "fetch a json list of stations from /stations ignoring those not in the schedule" in {

    val additionalStanoxNotInSchedule =
      StanoxRecord(StanoxCode("73321"), TipLocCode("HJ9321"), Some(CRS("LDS")), Some("Leeds"))

    withInitialState(testDatabaseConfig)(
      initialState.copy(stanoxRecords = initialState.stanoxRecords :+ additionalStanoxNotInSchedule)) { fixture =>
      val service      = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
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

  it should "fetch a json list of stations from /stations ignoring those without a CRS" in {

    val modifiedInitialState = initialState.copy(
      stanoxRecords = initialState.stanoxRecords
        .dropRight(1) :+ initialState.stanoxRecords.takeRight(1).head.copy(crs = None))

    withInitialState(testDatabaseConfig)(modifiedInitialState) { fixture =>
      val service      = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val request      = Request[IO](method = GET, uri = Uri(path = "/stations"))
      val response     = service.orNotFound(request).unsafeRunSync()
      val jsonResponse = response.as[Json].unsafeRunSync()
      jsonResponse shouldBe Json.arr(
        modifiedInitialState.stanoxRecords.filter(_.crs.isDefined).sortBy(_.stanoxCode.value) map { stanoxRecord =>
          Json.obj(
            "key" -> Json.fromString(stanoxRecord.stanoxCode.value),
            "value" -> Json.fromString(
              s"${stanoxRecord.description.getOrElse("")} [${stanoxRecord.crs.map(_.value).getOrElse("")}]")
          )
        }: _*)
    }
  }
}
