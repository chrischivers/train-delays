package traindelays.networkrail

import java.nio.file.Path

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.compress._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, EntityBody, Headers, Request}
import traindelays.NetworkRailConfig
import traindelays.stomp.{StompClient, StompHandler}

trait NetworkRailClient {

  def downloadScheduleData: IO[Unit]

  def unpackScheduleData: IO[Unit]

  def subscribeToTopic(topic: String, listener: StompHandler)
}

object NetworkRailClient extends StrictLogging {
  def apply(config: NetworkRailConfig, client: Client[IO]) = new NetworkRailClient {

    val credentials = BasicCredentials(config.username, config.password)

    override def downloadScheduleData: IO[Unit] = {
      val request =
        Request[IO](uri = config.scheduleData.downloadUrl).withHeaders(Headers(Authorization(credentials)))

      followRedirects(client, config.maxRedirects).fetch(request) { resp =>
        if (resp.status.isSuccess) {
          writeToFile(config.scheduleData.tmpDownloadLocation, resp.body).map(_ => ())
        } else throw new IllegalStateException(s"Call to download schedule unsuccessful. Status code [${resp.status}")
      }
    }

    override def unpackScheduleData: IO[Unit] =
      fs2.io.file
        .readAll[IO](config.scheduleData.tmpDownloadLocation, 4096)
        .drop(10) //drop gzip header
        .through(inflate(nowrap = true))
        .to(fs2.io.file.writeAll[IO](config.scheduleData.tmpUnzipLocation))
        .run

    override def subscribeToTopic(topic: String, listener: StompHandler): Unit = {
      logger.info(s"Subscribing to $topic")
      StompClient(config)
        .subscribe(topic, listener)
    }
  }

  private def writeToFile(path: Path, data: EntityBody[IO]): IO[Option[Unit]] =
    data
      .to(fs2.io.file.writeAll(path))
      .runLast

}
