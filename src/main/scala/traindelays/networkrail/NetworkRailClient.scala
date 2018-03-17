package traindelays.networkrail

import java.nio.file.{Path, StandardOpenOption}
import java.text.SimpleDateFormat
import java.util.Calendar

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.compress._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, EntityBody, Headers, Request, Uri}
import traindelays.NetworkRailConfig
import traindelays.stomp.{StompClient, StompStreamListener}

import scala.concurrent.ExecutionContext
import scala.util.Try

trait NetworkRailClient {

  def downloadFullScheduleData: fs2.Stream[IO, Unit]

  def downloadUpdateScheduleData: fs2.Stream[IO, Unit]

  def deleteTmpFiles(): IO[Unit]

  def unpackScheduleData: fs2.Stream[IO, Unit]

  def subscribeToTopic(topic: String, listener: StompStreamListener): IO[Unit]
}

object NetworkRailClient extends StrictLogging {
  def apply(config: NetworkRailConfig, client: Client[IO])(implicit ec: ExecutionContext) = new NetworkRailClient {

    val credentials = BasicCredentials(config.username, config.password)

    override def downloadFullScheduleData: fs2.Stream[IO, Unit] = downloadFromUrl(config.scheduleData.fullDownloadUrl)

    override def downloadUpdateScheduleData: fs2.Stream[IO, Unit] = {
      val simpleDateFormat = new SimpleDateFormat("E")
      val calendar         = Calendar.getInstance
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      val day = simpleDateFormat.format(calendar.getTime).toLowerCase
      val url = Uri.unsafeFromString(config.scheduleData.updateDownloadUrl.renderString.replace("DAY_FIELD", day))
      for {
        _ <- fs2.Stream.eval(IO(logger.info(s"Getting updated schedule from URL $url")))
        _ <- downloadFromUrl(url)
      } yield ()
    }

    private def downloadFromUrl(url: Uri): fs2.Stream[IO, Unit] =
      for {
        _ <- fs2.Stream.eval(IO(logger.info(s"Downloading from URL $url")))
        request = Request[IO](uri = url)
          .withHeaders(Headers(Authorization(credentials)))
        _ <- followRedirects(client, config.maxRedirects).streaming(request) { resp =>
          if (resp.status.isSuccess) {
            logger.info("Response successful. Writing to file...")
            writeToFile(config.scheduleData.tmpDownloadLocation, resp.body)
          } else {
            val msg = s"Call to download schedule unsuccessful. Status code [${resp.status}"
            logger.error(msg)
            fs2.Stream.raiseError(new IllegalStateException(msg))
          }
        }
      } yield ()

    override def unpackScheduleData: fs2.Stream[IO, Unit] =
      fs2.io.file
        .readAll[IO](config.scheduleData.tmpDownloadLocation, 4096)
        .drop(10) //drops gzip header
        .through(inflate(nowrap = true))
        .to(fs2.io.file.writeAll[IO](config.scheduleData.tmpUnzipLocation,
                                     flags = List(StandardOpenOption.CREATE, StandardOpenOption.SYNC)))

    override def subscribeToTopic(topic: String, listener: StompStreamListener): IO[Unit] =
      for {
        _ <- IO(logger.info(s"Subscribing to $topic"))
        _ <- IO(StompClient(config).subscribe(topic, listener))
      } yield ()

    override def deleteTmpFiles() =
      for {
        _ <- IO(logger.info("Deleting tmp files"))
        _ <- IO.fromEither(Try(config.scheduleData.tmpDownloadLocation.toFile.delete()).toEither)
        _ <- IO.fromEither(Try(config.scheduleData.tmpUnzipLocation.toFile.delete()).toEither)
      } yield ()

  }

  private def writeToFile(path: Path, data: EntityBody[IO])(implicit ec: ExecutionContext): fs2.Stream[IO, Unit] =
    data.to(fs2.io.file.writeAllAsync[IO](path)) >> fs2.Stream.eval(IO(logger.info("Finished writing to file")))

}
