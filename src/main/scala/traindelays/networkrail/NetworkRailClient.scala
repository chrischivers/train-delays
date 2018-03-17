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
import cats.syntax.flatMap._

import scala.util.Try

trait NetworkRailClient {

  def downloadFullScheduleData: IO[Unit]

  def downloadUpdateScheduleData: IO[Unit]

  def deleteTmpFiles(): IO[Unit]

  def unpackScheduleData: IO[Unit]

  def subscribeToTopic(topic: String, listener: StompStreamListener): IO[Unit]
}

object NetworkRailClient extends StrictLogging {
  def apply(config: NetworkRailConfig, client: Client[IO]) = new NetworkRailClient {

    val credentials = BasicCredentials(config.username, config.password)

    override def downloadFullScheduleData: IO[Unit] = downloadFromUrl(config.scheduleData.fullDownloadUrl)

    override def downloadUpdateScheduleData: IO[Unit] = {
      val simpleDateFormat = new SimpleDateFormat("E")
      val calendar         = Calendar.getInstance
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      val day = simpleDateFormat.format(calendar.getTime).toLowerCase
      val url = Uri.unsafeFromString(config.scheduleData.updateDownloadUrl.renderString.replace("DAY_FIELD", day))
      for {
        _ <- IO(logger.info(s"Getting updated schedule from URL $url"))
        _ <- downloadFromUrl(url)
      } yield ()
    }

    private def downloadFromUrl(url: Uri): IO[Unit] =
      for {
        _ <- IO(logger.info(s"Downloading from URL $url"))
        request = Request[IO](uri = url)
          .withHeaders(Headers(Authorization(credentials)))
        _ <- followRedirects(client, config.maxRedirects)
          .streaming(request) { resp =>
            logger.info("Response successful. Writing to file...")
            fs2.Stream.eval(writeToFile(config.scheduleData.tmpDownloadLocation, resp.body))
          }
          .compile
          .drain
      } yield ()

    override def unpackScheduleData: IO[Unit] =
      fs2.io.file
        .readAll[IO](config.scheduleData.tmpDownloadLocation, 4096)
        .drop(10) //drops gzip header
        .through(inflate(nowrap = true))
        .to(fs2.io.file.writeAll[IO](config.scheduleData.tmpUnzipLocation,
                                     flags = List(StandardOpenOption.CREATE, StandardOpenOption.SYNC)))
        .compile
        .drain >> IO(logger.info("Finished unpacking schedule data"))

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

  private def writeToFile(path: Path, data: EntityBody[IO]): IO[Unit] =
    data.to(fs2.io.file.writeAll(path)).compile.drain >> IO(logger.info("Finished writing to file"))

}
