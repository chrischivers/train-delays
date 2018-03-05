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

trait NetworkRailClient {

  def downloadFullScheduleData: fs2.Stream[IO, Unit]

  def downloadUpdateScheduleData: fs2.Stream[IO, Unit]

  def deleteTmpFiles(): IO[Unit]

  def unpackScheduleData: fs2.Stream[IO, Unit]

  def subscribeToTopic(topic: String, listener: StompStreamListener)
}

object NetworkRailClient extends StrictLogging {
  def apply(config: NetworkRailConfig, client: Client[IO]) = new NetworkRailClient {

    val credentials = BasicCredentials(config.username, config.password)

    override def downloadFullScheduleData: fs2.Stream[IO, Unit] = downloadFromUrl(config.scheduleData.fullDownloadUrl)

    override def downloadUpdateScheduleData: fs2.Stream[IO, Unit] = {
      val simpleDateFormat = new SimpleDateFormat("E")
      val calendar         = Calendar.getInstance
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      val day = simpleDateFormat.format(calendar.getTime).toLowerCase
      val url = Uri.unsafeFromString(config.scheduleData.updateDownloadUrl.renderString.replace("DAY_FIELD", day))
      logger.info(s"Getting updated schedule from URL $url")
      downloadFromUrl(url)
    }

    private def downloadFromUrl(url: Uri): fs2.Stream[IO, Unit] = {
      val request =
        Request[IO](uri = url).withHeaders(Headers(Authorization(credentials)))
      followRedirects(client, config.maxRedirects).streaming(request) { resp =>
        if (resp.status.isSuccess) {
          writeToFile(config.scheduleData.tmpDownloadLocation, resp.body)
        } else throw new IllegalStateException(s"Call to download schedule unsuccessful. Status code [${resp.status}")
      }
    }

    override def unpackScheduleData: fs2.Stream[IO, Unit] =
      fs2.io.file
        .readAll[IO](config.scheduleData.tmpDownloadLocation, 4096)
        .drop(10) //drops gzip header
        .through(inflate(nowrap = true))
        .to(fs2.io.file.writeAll[IO](config.scheduleData.tmpUnzipLocation,
                                     flags = List(StandardOpenOption.CREATE, StandardOpenOption.SYNC)))

    override def subscribeToTopic(topic: String, listener: StompStreamListener): Unit = {
      logger.info(s"Subscribing to $topic")
      StompClient(config)
        .subscribe(topic, listener)
    }

    override def deleteTmpFiles() = IO {
      config.scheduleData.tmpDownloadLocation.toFile.delete()
      config.scheduleData.tmpUnzipLocation.toFile.delete()
    }

  }

  private def writeToFile(path: Path, data: EntityBody[IO]): fs2.Stream[IO, Unit] =
    data
      .to(fs2.io.file.writeAll(path))

}
