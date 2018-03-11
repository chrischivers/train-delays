package traindelays.networkrail.scheduledata

import java.nio.file.Path

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.fs2._

trait ScheduleDataReader extends StrictLogging {

  def readData: fs2.Stream[IO, DecodedRecord]
}

object ScheduleDataReader {

  def apply(unzippedScheduleFileLocation: Path) = new ScheduleDataReader {

    override def readData: fs2.Stream[IO, DecodedRecord] =
      fs2.io.file
        .readAll[IO](unzippedScheduleFileLocation, 4096)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .through(stringStreamParser[IO])
        .dropLast //EOF line
        .through(decoder[IO, DecodedRecord])
  }
}
