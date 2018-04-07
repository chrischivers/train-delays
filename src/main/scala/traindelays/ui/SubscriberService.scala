package traindelays.ui

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.{EntityDecoder, EntityEncoder, Request, Response}
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.dsl.io._
import org.postgresql.util.PSQLException
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordPrimary, ScheduleRecordSecondary}
import traindelays.networkrail.db.{ScheduleTable, SubscriberTable}
import traindelays.networkrail.subscribers.SubscriberRecord
import org.http4s.circe._

trait SubscriberService {
  def handleSubscribeRequest(request: Request[IO]): IO[Response[IO]]
  def handleSubscriberRecordsRequest(subscriberToken: String): IO[Response[IO]]
  def deleteSubscriberRecord(subscriberToken: String, recordIdToDelete: String): IO[Response[IO]]
}

object SubscriberService extends StrictLogging {

  def apply(subscriberTable: SubscriberTable,
            scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
            scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
            googleAuthenticator: GoogleAuthenticator) =
    new SubscriberService {

      import cats.instances.list._
      import cats.syntax.traverse._

      implicit val subscriberRecordsListResponseEncoder: EntityEncoder[IO, List[SubscriberRecordsResponse]] =
        jsonEncoderOf[IO, List[SubscriberRecordsResponse]]

      implicit val subscribeRequestEntityDecoder: EntityDecoder[IO, SubscribeRequest] =
        jsonOf[IO, SubscribeRequest]

      override def handleSubscribeRequest(request: Request[IO]): IO[Response[IO]] = {

        def process(subscribeRequest: SubscribeRequest): IO[Unit] =
          for {
            maybeAuthenticatedDetails <- googleAuthenticator.verifyToken(subscribeRequest.idToken)
            _ <- maybeAuthenticatedDetails
              .fold[IO[List[Unit]]](
                IO.raiseError(new RuntimeException(
                  s"Unable to retrieve authentication details for id token ${subscribeRequest.idToken}"))) {
                authenticatedDetails =>
                  subscribeRequest.records
                    .map { record =>
                      for {
                        scheduleRec <- retrieveRecordById(record.id, scheduleTablePrimary, scheduleTableSecondary)
                        maybeSubscriberRecord = scheduleRec.map { scheduleRec =>
                          SubscriberRecord(
                            None,
                            authenticatedDetails.userId,
                            authenticatedDetails.email,
                            authenticatedDetails.emailVerified,
                            authenticatedDetails.name,
                            authenticatedDetails.firstName,
                            authenticatedDetails.familyName,
                            authenticatedDetails.locale,
                            scheduleRec.scheduleTrainId,
                            scheduleRec.serviceCode,
                            record.fromStanox,
                            record.fromCRS,
                            record.departureTime,
                            record.toStanox,
                            record.toCRS,
                            record.arrivalTime,
                            record.daysRunPattern
                          )
                        }
                        _ <- maybeSubscriberRecord.fold[IO[Unit]](
                          IO.raiseError(new RuntimeException(s"Invalid schedule ID ${record.id}")))(subscriberRec =>
                          subscriberTable.safeAddRecord(subscriberRec))
                      } yield ()
                    }
                    .sequence[IO, Unit]
              }
          } yield ()

        request
          .as[SubscribeRequest]
          .attempt
          .flatMap {
            case Right(subscriberRequest) =>
              logger.info(s"Received subscribe request [$subscriberRequest]")
              process(subscriberRequest).attempt
                .flatMap {
                  case Left(e)
                      if e.isInstanceOf[PSQLException] && e
                        .asInstanceOf[PSQLException]
                        .getMessage
                        .contains("duplicate key value") =>
                    logger.info(s"Subscriber already subscribed to route")
                    Conflict("Already subscribed")
                  case Left(e) =>
                    logger.error(s"Unable to process subscriber request received", e)
                    InternalServerError()
                  case Right(_) =>
                    logger.info("Subscriber request successful")
                    Ok()
                }
            case Left(err) =>
              logger.error(s"Unable to decode subscribe request ${request.toString()}", err)
              BadRequest()
          }
      }

      override def handleSubscriberRecordsRequest(subscriberToken: String): IO[Response[IO]] =
        (for {
          maybeAuthenticatedDetails <- googleAuthenticator.verifyToken(subscriberToken)
          records <- maybeAuthenticatedDetails
            .fold[IO[List[SubscriberRecord]]](IO.raiseError(
              new RuntimeException(s"Unable to retrieve authentication details for id token $subscriberToken"))) {
              authenticatedDetails =>
                subscriberTable.subscriberRecordsFor(authenticatedDetails.userId)
            }
        } yield records).attempt.flatMap {
          case Right(subscriberRecords) =>
            Ok(SubscriberRecordsResponse.subscriberRecordsResponseFrom(subscriberRecords))
          case Left(err) =>
            logger.error(s"Unable to get subscribers records for id $subscriberToken", err)
            BadRequest()
        }

      //TODO test coverage for this
      override def deleteSubscriberRecord(subscriberToken: String, recordIdToDelete: String): IO[Response[IO]] =
        (for {
          recordId                  <- IO.fromEither(scala.util.Try(recordIdToDelete.toInt).toEither)
          maybeAuthenticatedDetails <- googleAuthenticator.verifyToken(subscriberToken)
          _ <- maybeAuthenticatedDetails
            .fold[IO[Unit]](
              IO.raiseError(
                new RuntimeException(s"Unable to retrieve authentication details for id token $subscriberToken"))) {
              authenticatedDetails =>
                for {
                  subscriberRecord <- subscriberTable.subscriberRecordFor(recordId)
                  userIdMatches = subscriberRecord.exists(rec => authenticatedDetails.userId == rec.userId)
                  _ <- if (userIdMatches) subscriberTable.deleteRecord(recordId)
                  else IO.raiseError(new RuntimeException("user ID does not match user of record being deleted"))
                } yield ()
            }
        } yield ()).attempt.flatMap {
          case Right(_) =>
            Ok("Record deleted")
          case Left(err) =>
            logger.error(s"Unable to get delete records for id $recordIdToDelete", err)
            BadRequest()
        }

      private def retrieveRecordById(
          scheduleQueryResponseId: ScheduleQueryResponseId,
          scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
          scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary]): IO[Option[ScheduleTable.ScheduleRecord]] =
        scheduleQueryResponseId.recordType match {
          case ScheduleRecordPrimary.recordTypeString =>
            scheduleTablePrimary.retrieveRecordBy(scheduleQueryResponseId.value)
          case ScheduleRecordSecondary.recordTypeString =>
            scheduleTableSecondary.retrieveRecordBy(scheduleQueryResponseId.value)
        }

    }
}
