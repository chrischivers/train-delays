package traindelays.ui

import java.time._
import java.util.concurrent.TimeUnit

import cats.effect.IO
//import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, Request, Uri}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata.EventType.{Arrival, Departure}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.UIConfig
import traindelays.networkrail.TestFeatures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class HistoryQueryEndpointTest extends FlatSpec with TestFeatures {

  implicit val executionContext = ExecutionContext.Implicits.global

  val uiTestConfig                                      = UIConfig(2, FiniteDuration(1, TimeUnit.MINUTES), "")
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

    val movementLogDeparture = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords(0).stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get)
    )
    val movementLogArrival = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords(1).stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).arrivalTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).arrivalTime.get)
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
        initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture.stanoxCode).get.atocCode,
        movementLogDeparture.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture.stanoxCode).get.crs.get,
        movementLogArrival.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival.stanoxCode).get.crs.get,
        movementLogDeparture.plannedPassengerTime,
        movementLogArrival.plannedPassengerTime,
        List(
          HistoryQueryMovementRecord(
            movementLogDeparture.originDepartureDate,
            movementLogDeparture.actualTime,
            Duration.between(movementLogDeparture.plannedPassengerTime, movementLogDeparture.actualTime).toMinutes,
            movementLogArrival.actualTime,
            Duration.between(movementLogArrival.plannedPassengerTime, movementLogArrival.actualTime).toMinutes
          )
        ),
        List.empty
      )
    }
  }

  it should "return the history of multiple records" in {

    println(initialState.schedulePrimaryRecords.head)

    val movementLogDeparture1 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords.head.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get)
    )
    val movementLogArrival1 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get) + 180000
    )

    val movementLogDeparture2 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords.head.stanoxCode.get,
      originDepartureTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      actualTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get) + 60000
    )
    val movementLogArrival2 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode.get,
      originDepartureTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get),
      actualTimestamp = yesterdayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get) + 1800000
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
          initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.atocCode,
          movementLogDeparture1.stanoxCode,
          initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture1.stanoxCode).get.crs.get,
          movementLogArrival1.stanoxCode,
          initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival1.stanoxCode).get.crs.get,
          movementLogDeparture1.plannedPassengerTime,
          movementLogArrival1.plannedPassengerTime,
          List(
            HistoryQueryMovementRecord(
              movementLogDeparture2.originDepartureDate,
              movementLogDeparture2.actualTime,
              Duration.between(movementLogDeparture2.plannedPassengerTime, movementLogDeparture2.actualTime).toMinutes,
              movementLogArrival2.actualTime,
              Duration.between(movementLogArrival2.plannedPassengerTime, movementLogArrival2.actualTime).toMinutes
            ),
            HistoryQueryMovementRecord(
              movementLogDeparture1.originDepartureDate,
              movementLogDeparture1.actualTime,
              Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
              movementLogArrival1.actualTime,
              Duration.between(movementLogArrival1.plannedPassengerTime, movementLogArrival1.actualTime).toMinutes
            )
          ),
          List.empty
        )
    }
  }

  it should "return the history of a record, excluding records relating to a different scheduleTrainId or stanox" in {

    val movementLogDeparture = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords.head.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get)
    )
    val movementLogArrival = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get) + 1800000
    )

    val departureLogWithDifferentScheduleTrainId = movementLogDeparture.copy(scheduleTrainId = ScheduleTrainId("93845"))
    val arrivalLogWithDifferentScheduleTrainId   = movementLogArrival.copy(scheduleTrainId = ScheduleTrainId("93845"))

    val departureLogWithDifferentStanox =
      movementLogDeparture.copy(
        stanoxCode = initialState.stanoxRecords(3).stanoxCode.get,
        plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(3).departureTime.get),
        actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(3).departureTime.get) + 1800000
      )

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
        initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture.stanoxCode).get.atocCode,
        movementLogDeparture.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture.stanoxCode).get.crs.get,
        movementLogArrival.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival.stanoxCode).get.crs.get,
        movementLogDeparture.plannedPassengerTime,
        movementLogArrival.plannedPassengerTime,
        List(
          HistoryQueryMovementRecord(
            movementLogDeparture.originDepartureDate,
            movementLogDeparture.actualTime,
            Duration.between(movementLogDeparture.plannedPassengerTime, movementLogDeparture.actualTime).toMinutes,
            movementLogArrival.actualTime,
            Duration.between(movementLogArrival.plannedPassengerTime, movementLogArrival.actualTime).toMinutes
          )
        ),
        List.empty
      )
    }
  }

  it should "return a single history record for different departure/arrival combinations on the same route" in {

    val movementLogDeparture1 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords.head.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get)
    )

    val movementLogArrival2 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords(1).stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).arrivalTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).arrivalTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).arrivalTime.get) + 60000
    )

    val movementLogDeparture3 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Departure,
      stanoxCode = initialState.stanoxRecords(1).stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).departureTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords(1).departureTime.get) + 30000
    )

    val movementLogArrival4 = createMovementLog(
      scheduleTrainId = initialState.schedulePrimaryRecords.head.scheduleTrainId,
      eventType = Arrival,
      stanoxCode = initialState.stanoxRecords.last.stanoxCode.get,
      originDepartureTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.head.departureTime.get),
      plannedPassengerTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get),
      actualTimestamp = todayMillisWith(initialState.schedulePrimaryRecords.last.arrivalTime.get) + 1900000
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
        initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.atocCode,
        movementLogDeparture1.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture1.stanoxCode).get.crs.get,
        movementLogArrival4.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival4.stanoxCode).get.crs.get,
        movementLogDeparture1.plannedPassengerTime,
        movementLogArrival4.plannedPassengerTime,
        List(
          HistoryQueryMovementRecord(
            movementLogDeparture1.originDepartureDate,
            movementLogDeparture1.actualTime,
            Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
            movementLogArrival4.actualTime,
            Duration.between(movementLogArrival4.plannedPassengerTime, movementLogArrival4.actualTime).toMinutes
          )
        ),
        List.empty
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
        initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture3.stanoxCode).get.atocCode,
        movementLogDeparture3.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture3.stanoxCode).get.crs.get,
        movementLogArrival4.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival4.stanoxCode).get.crs.get,
        movementLogDeparture3.plannedPassengerTime,
        movementLogArrival4.plannedPassengerTime,
        List(
          HistoryQueryMovementRecord(
            movementLogDeparture3.originDepartureDate,
            movementLogDeparture3.actualTime,
            Duration.between(movementLogDeparture3.plannedPassengerTime, movementLogDeparture3.actualTime).toMinutes,
            movementLogArrival4.actualTime,
            Duration.between(movementLogArrival4.plannedPassengerTime, movementLogArrival4.actualTime).toMinutes
          )
        ),
        List.empty
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
        initialState.schedulePrimaryRecords.find(_.stanoxCode == movementLogDeparture1.stanoxCode).get.atocCode,
        movementLogDeparture1.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogDeparture1.stanoxCode).get.crs.get,
        movementLogArrival2.stanoxCode,
        initialState.stanoxRecords.find(_.stanoxCode.get == movementLogArrival2.stanoxCode).get.crs.get,
        movementLogDeparture1.plannedPassengerTime,
        movementLogArrival2.plannedPassengerTime,
        List(
          HistoryQueryMovementRecord(
            movementLogDeparture1.originDepartureDate,
            movementLogDeparture1.actualTime,
            Duration.between(movementLogDeparture1.plannedPassengerTime, movementLogDeparture1.actualTime).toMinutes,
            movementLogArrival2.actualTime,
            Duration.between(movementLogArrival2.plannedPassengerTime, movementLogArrival2.actualTime).toMinutes
          )
        ),
        List.empty
      )
    }
  }

  private val timeZone = ZoneId.of("Europe/London")

  private def todayMillisWith(localTime: LocalTime) = {
    val now         = Instant.now()
    val millisToAdd = Duration.between(LocalDateTime.ofInstant(now, timeZone).toLocalTime, localTime).toMillis
    now.toEpochMilli + millisToAdd
  }

  private def yesterdayMillisWith(localTime: LocalTime) = {
    val now         = Instant.now()
    val millisToAdd = Duration.between(LocalDateTime.ofInstant(now, timeZone).toLocalTime, localTime).toMillis
    now.toEpochMilli + millisToAdd - 86400000
  }
  //TODO test canellations in response
}
