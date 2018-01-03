package traindelays.networkrail.scheduledata

import java.nio.file.Path

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder
import io.circe.fs2._

trait ScheduleDataReader extends StrictLogging {

  def readData[A](implicit dec: Decoder[A], jsonFilter: JsonFilter[A], transformer: Transformer[A]): fs2.Stream[IO, A]
}

object ScheduleDataReader {

  def apply(unzippedScheduleFileLocation: Path) = new ScheduleDataReader {

    override def readData[A](implicit dec: Decoder[A],
                             jsonFilter: JsonFilter[A],
                             transformer: Transformer[A]): fs2.Stream[IO, A] =
      fs2.io.file
        .readAll[IO](unzippedScheduleFileLocation, 4096)
        .through(fs2.text.utf8Decode)
        .through(stringStreamParser[IO])
        .filter(jsonFilter.jsonFilter)
        .through(decoder[IO, A])
        .through(transformer.transform)

  }
}
