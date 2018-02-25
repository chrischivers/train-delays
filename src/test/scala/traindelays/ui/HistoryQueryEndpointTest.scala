package traindelays.ui

import java.time.Duration

import cats.effect.IO
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, Request, Uri}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata.EventType.{Arrival, Departure}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.{TestFeatures, UIConfig}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class HistoryQueryEndpointTest extends FlatSpec with TestFeatures {

  val uiTestConfig                                      = UIConfig(2, 1 minute, "")
  val defaultAuthenticatedDetails: AuthenticatedDetails = createAuthenticatedDetails()
  val initialState: AppInitialState                     = createDefaultInitialState()

  implicit val historyQueryResponseEntityDecoder: EntityDecoder[IO, HistoryQueryResponse] =
    org.http4s.circe.jsonOf[IO, HistoryQueryResponse]

  it should "return a 404 if no history found" in {

    val movementLog = createMovementLog()
    withInitialState(testDatabaseConfig)(initialState.copy(movementLogs = List(movementLog))) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val queryParams =
        Map("scheduleTrainId" -> movementLog.scheduleTrainId.value, "fromStanox" -> "57883", "toStanox" -> "87742")
      val request = Request[IO](method = GET,
                                uri = Uri(path = "/history-query")
                                  .setQueryParams(queryParams.map { case (k, v) => k -> Seq(v) }))
      val response = service.orNotFound(request).unsafeRunSync()
      response.status.code shouldBe 404
    }
  }

  it should "return the history of a single record" in {

    val now = System.currentTimeMillis()

    val movementLogDeparture = createMovementLog(eventType = Departure,
                                                 stanoxCode = initialState.stanoxRecords(0).stanoxCode,
                                                 originDepartureTimestamp = now,
                                                 plannedPassengerTimestamp = now)
    val movementLogArrival = createMovementLog(
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords(1).stanoxCode,
      originDepartureTimestamp = now,
      plannedPassengerTimestamp = now + 120000,
      actualTimestamp = now + 180000
    )

    withInitialState(testDatabaseConfig)(
      initialState.copy(movementLogs = List(movementLogDeparture, movementLogArrival))) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val queryParams = Map(
        "scheduleTrainId" -> movementLogDeparture.scheduleTrainId.value,
        "fromStanox"      -> movementLogDeparture.stanoxCode.value,
        "toStanox"        -> movementLogArrival.stanoxCode.value
      )
      val request = Request[IO](method = GET,
                                uri = Uri(path = "/history-query")
                                  .setQueryParams(queryParams.map { case (k, v) => k -> Seq(v) }))
      val response = service.orNotFound(request).unsafeRunSync()
      response.status.code shouldBe 200
      response.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
        movementLogDeparture.scheduleTrainId,
        movementLogDeparture.toc,
        movementLogDeparture.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture.stanoxCode).get.crs.get,
        movementLogArrival.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival.stanoxCode).get.crs.get,
        movementLogDeparture.plannedPassengerTime,
        movementLogArrival.plannedPassengerTime,
        List(
          HistoryQueryRecord(
            movementLogDeparture.originDepartureDate,
            movementLogDeparture.actualTime,
            Duration.between(movementLogDeparture.plannedPassengerTime, movementLogDeparture.actualTime).toMinutes,
            movementLogArrival.actualTime,
            Duration.between(movementLogArrival.plannedPassengerTime, movementLogArrival.actualTime).toMinutes
          )
        )
      )
    }
  }

  it should "return the history of multiple records" in {

    val now       = System.currentTimeMillis()
    val yesterday = System.currentTimeMillis() - 86400000

    val movementLogDeparture1 = createMovementLog(eventType = Departure,
                                                  stanoxCode = initialState.stanoxRecords.head.stanoxCode,
                                                  originDepartureTimestamp = now,
                                                  plannedPassengerTimestamp = now)
    val movementLogArrival1 = createMovementLog(
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode,
      originDepartureTimestamp = now,
      plannedPassengerTimestamp = now + 1200000,
      actualTimestamp = now + 1800000
    )

    val movementLogDeparture2 = createMovementLog(eventType = Departure,
                                                  stanoxCode = initialState.stanoxRecords.head.stanoxCode,
                                                  originDepartureTimestamp = yesterday,
                                                  plannedPassengerTimestamp = yesterday)
    val movementLogArrival2 = createMovementLog(
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode,
      originDepartureTimestamp = yesterday,
      plannedPassengerTimestamp = yesterday + 1200000,
      actualTimestamp = now + 1800000
    )

    withInitialState(testDatabaseConfig)(
      initialState.copy(
        movementLogs = List(movementLogDeparture1, movementLogArrival1, movementLogDeparture2, movementLogArrival2))) {
      fixture =>
        val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
        val queryParams = Map(
          "scheduleTrainId" -> movementLogDeparture1.scheduleTrainId.value,
          "fromStanox"      -> movementLogDeparture1.stanoxCode.value,
          "toStanox"        -> movementLogArrival1.stanoxCode.value
        )
        val request = Request[IO](method = GET,
                                  uri = Uri(path = "/history-query")
                                    .setQueryParams(queryParams.map { case (k, v) => k -> Seq(v) }))
        val response = service.orNotFound(request).unsafeRunSync()
        response.status.code shouldBe 200
        response.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
          movementLogDeparture1.scheduleTrainId,
          movementLogDeparture1.toc,
          movementLogDeparture1.stanoxCode,
          initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.crs.get,
          movementLogArrival1.stanoxCode,
          initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival1.stanoxCode).get.crs.get,
          movementLogDeparture1.plannedPassengerTime,
          movementLogArrival1.plannedPassengerTime,
          List(
            HistoryQueryRecord(
              movementLogDeparture2.originDepartureDate,
              movementLogDeparture2.actualTime,
              Duration.between(movementLogDeparture2.plannedPassengerTime, movementLogDeparture2.actualTime).toMinutes,
              movementLogArrival2.actualTime,
              Duration.between(movementLogArrival2.plannedPassengerTime, movementLogArrival2.actualTime).toMinutes
            ),
            HistoryQueryRecord(
              movementLogDeparture1.originDepartureDate,
              movementLogDeparture1.actualTime,
              Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
              movementLogArrival1.actualTime,
              Duration.between(movementLogArrival1.plannedPassengerTime, movementLogArrival1.actualTime).toMinutes
            )
          )
        )
    }
  }

  it should "return the history of a record, excluding records relating to a different scheduleTrainId or stanox" in {

    val now = System.currentTimeMillis()

    val movementLogDeparture = createMovementLog(eventType = Departure,
                                                 stanoxCode = initialState.stanoxRecords.head.stanoxCode,
                                                 originDepartureTimestamp = now,
                                                 plannedPassengerTimestamp = now)
    val movementLogArrival = createMovementLog(
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode,
      originDepartureTimestamp = now,
      plannedPassengerTimestamp = now + 1200000,
      actualTimestamp = now + 1800000
    )

    val departureLogWithDifferentScheduleTrainId = movementLogDeparture.copy(scheduleTrainId = ScheduleTrainId("93845"))
    val arrivalLogWithDifferentScheduleTrainId   = movementLogArrival.copy(scheduleTrainId = ScheduleTrainId("93845"))

    val departureLogWithDifferentStanox =
      movementLogDeparture.copy(stanoxCode = initialState.stanoxRecords(3).stanoxCode)

    withInitialState(testDatabaseConfig)(
      initialState.copy(movementLogs = List(
        movementLogDeparture,
        movementLogArrival,
        departureLogWithDifferentScheduleTrainId,
        arrivalLogWithDifferentScheduleTrainId,
        departureLogWithDifferentStanox
      ))) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val queryParams = Map(
        "scheduleTrainId" -> movementLogDeparture.scheduleTrainId.value,
        "fromStanox"      -> movementLogDeparture.stanoxCode.value,
        "toStanox"        -> movementLogArrival.stanoxCode.value
      )
      val request = Request[IO](method = GET,
                                uri = Uri(path = "/history-query")
                                  .setQueryParams(queryParams.map { case (k, v) => k -> Seq(v) }))
      val response = service.orNotFound(request).unsafeRunSync()
      response.status.code shouldBe 200
      response.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
        movementLogDeparture.scheduleTrainId,
        movementLogDeparture.toc,
        movementLogDeparture.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture.stanoxCode).get.crs.get,
        movementLogArrival.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival.stanoxCode).get.crs.get,
        movementLogDeparture.plannedPassengerTime,
        movementLogArrival.plannedPassengerTime,
        List(
          HistoryQueryRecord(
            movementLogDeparture.originDepartureDate,
            movementLogDeparture.actualTime,
            Duration.between(movementLogDeparture.plannedPassengerTime, movementLogDeparture.actualTime).toMinutes,
            movementLogArrival.actualTime,
            Duration.between(movementLogArrival.plannedPassengerTime, movementLogArrival.actualTime).toMinutes
          )
        )
      )
    }
  }

  it should "return a single history record for different departure/arrival combinations on the same route" in {

    val now = System.currentTimeMillis()

    val movementLogDeparture1 = createMovementLog(eventType = Departure,
                                                  stanoxCode = initialState.stanoxRecords.head.stanoxCode,
                                                  originDepartureTimestamp = now,
                                                  plannedPassengerTimestamp = now)

    val movementLogArrival2 = createMovementLog(eventType = Arrival,
                                                stanoxCode = initialState.stanoxRecords(1).stanoxCode,
                                                originDepartureTimestamp = now,
                                                plannedPassengerTimestamp = now + 1200000)

    val movementLogDeparture3 = createMovementLog(eventType = Departure,
                                                  stanoxCode = initialState.stanoxRecords(1).stanoxCode,
                                                  originDepartureTimestamp = now,
                                                  plannedPassengerTimestamp = now + 1300000)

    val movementLogArrival4 = createMovementLog(
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode,
      originDepartureTimestamp = now,
      plannedPassengerTimestamp = now + 1800000,
      actualTimestamp = now + 1900000
    )

    withInitialState(testDatabaseConfig)(
      initialState.copy(
        movementLogs = List(
          movementLogDeparture1,
          movementLogArrival2,
          movementLogDeparture3,
          movementLogArrival4
        ))) { fixture =>
      val service = serviceFrom(fixture, uiTestConfig, defaultAuthenticatedDetails)
      val queryParams1 = Map(
        "scheduleTrainId" -> movementLogDeparture1.scheduleTrainId.value,
        "fromStanox"      -> movementLogDeparture1.stanoxCode.value,
        "toStanox"        -> movementLogArrival4.stanoxCode.value
      )
      val request1 = Request[IO](method = GET,
                                 uri = Uri(path = "/history-query")
                                   .setQueryParams(queryParams1.map { case (k, v) => k -> Seq(v) }))
      val response1 = service.orNotFound(request1).unsafeRunSync()
      response1.status.code shouldBe 200
      response1.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
        movementLogDeparture1.scheduleTrainId,
        movementLogDeparture1.toc,
        movementLogDeparture1.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.crs.get,
        movementLogArrival4.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival4.stanoxCode).get.crs.get,
        movementLogDeparture1.plannedPassengerTime,
        movementLogArrival4.plannedPassengerTime,
        List(
          HistoryQueryRecord(
            movementLogDeparture1.originDepartureDate,
            movementLogDeparture1.actualTime,
            Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
            movementLogArrival4.actualTime,
            Duration.between(movementLogArrival4.plannedPassengerTime, movementLogArrival4.actualTime).toMinutes
          )
        )
      )

      val queryParams2 = Map(
        "scheduleTrainId" -> movementLogDeparture3.scheduleTrainId.value,
        "fromStanox"      -> movementLogDeparture3.stanoxCode.value,
        "toStanox"        -> movementLogArrival4.stanoxCode.value
      )
      val request2 = Request[IO](method = GET,
                                 uri = Uri(path = "/history-query")
                                   .setQueryParams(queryParams2.map { case (k, v) => k -> Seq(v) }))
      val response2 = service.orNotFound(request2).unsafeRunSync()
      response2.status.code shouldBe 200
      response2.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
        movementLogDeparture3.scheduleTrainId,
        movementLogDeparture3.toc,
        movementLogDeparture3.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture3.stanoxCode).get.crs.get,
        movementLogArrival4.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival4.stanoxCode).get.crs.get,
        movementLogDeparture3.plannedPassengerTime,
        movementLogArrival4.plannedPassengerTime,
        List(
          HistoryQueryRecord(
            movementLogDeparture3.originDepartureDate,
            movementLogDeparture3.actualTime,
            Duration.between(movementLogDeparture3.plannedPassengerTime, movementLogDeparture3.actualTime).toMinutes,
            movementLogArrival4.actualTime,
            Duration.between(movementLogArrival4.plannedPassengerTime, movementLogArrival4.actualTime).toMinutes
          )
        )
      )

      val queryParams3 = Map(
        "scheduleTrainId" -> movementLogDeparture1.scheduleTrainId.value,
        "fromStanox"      -> movementLogDeparture1.stanoxCode.value,
        "toStanox"        -> movementLogArrival2.stanoxCode.value
      )
      val request3 = Request[IO](method = GET,
                                 uri = Uri(path = "/history-query")
                                   .setQueryParams(queryParams3.map { case (k, v) => k -> Seq(v) }))
      val response3 = service.orNotFound(request3).unsafeRunSync()
      response3.status.code shouldBe 200
      response3.as[HistoryQueryResponse].unsafeRunSync() shouldBe HistoryQueryResponse(
        movementLogDeparture1.scheduleTrainId,
        movementLogDeparture1.toc,
        movementLogDeparture1.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.crs.get,
        movementLogArrival2.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode == movementLogArrival2.stanoxCode).get.crs.get,
        movementLogDeparture1.plannedPassengerTime,
        movementLogArrival2.plannedPassengerTime,
        List(
          HistoryQueryRecord(
            movementLogDeparture1.originDepartureDate,
            movementLogDeparture1.actualTime,
            Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
            movementLogArrival2.actualTime,
            Duration.between(movementLogArrival2.plannedPassengerTime, movementLogArrival2.actualTime).toMinutes
          )
        )
      )
    }
  }
}
