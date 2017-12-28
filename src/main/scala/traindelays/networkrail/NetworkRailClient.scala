package traindelays.networkrail

import java.nio.file.Path

import cats.effect.IO
import fs2.compress._
import net.ser1.stomp.{Client => StompClient}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, EntityBody, Headers, Request}
import traindelays.NetworkRailConfig

trait NetworkRailClient {

  def write(path: Path, data: EntityBody[IO]): IO[Option[Unit]] =
    data
      .to(fs2.io.file.writeAll(path))
      .runLast

  def downloadScheduleData: IO[Unit]

  def unpackScheduleData: IO[Unit]

//
//  def start = {
//    val client = new Client(config.host, config.port, config.username, config.password)
//    val listener = new Listener {
//      override def message(header: util.Map[_, _], body: String): Unit = ???
//    }
//    client.subscribe(topic, listener)
//  }

}

object NetworkRailClient {
  def apply(config: NetworkRailConfig, client: Client[IO]) = new NetworkRailClient {

    val credentials = BasicCredentials(config.username, config.password)

    override def downloadScheduleData: IO[Unit] = {
      val request =
        Request[IO](uri = config.scheduleDataConf.downloadUrl).withHeaders(Headers(Authorization(credentials)))

      followRedirects(client, config.maxRedirects).fetch(request) { resp =>
        if (resp.status.isSuccess) {
          write(config.scheduleDataConf.tmpDownloadLocation, resp.body).map(_ => ())
        } else throw new IllegalStateException(s"Call to download schedule unsuccessful. Status code [${resp.status}")
      }
    }

    override def unpackScheduleData: IO[Unit] =
      fs2.io.file
        .readAll[IO](config.scheduleDataConf.tmpDownloadLocation, 4096)
        .drop(10) //drop gzip header
        .through(inflate(nowrap = true))
        .to(fs2.io.file.writeAll[IO](config.scheduleDataConf.tmpUnzipLocation))
        .run
  }
}

/*
public class MyClient {


    public static void main(String[] args) throws Exception {
        new MyClient().go();
    }

    /*
 * Connect to a single topic and subscribe a listener
 * @throws Exception Too lazy to implement exception handling....
 */
    private void go() throws Exception {
        System.out.println("| Connecting...");
        Client client = new Client(SERVER, PORT, USERNAME, PASSWORD);
        if (client.isConnected()) {
            System.out.println("| Connected to " + SERVER + ":" + PORT);
        } else {
            System.out.println("| Could not connect");
            return;
        }
        System.out.println("| Subscribing...");
        Listener listener = new MyListener();
        client.subscribe(TOPIC , listener);
        System.out.println("| Subscribed to " + TOPIC);
        System.out.println("| Waiting for message...");
    }
}
 */
