package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import fs2.{Pipe, Sink}
import traindelays.networkrail.db.MovementLogTable

trait MovementProcessor {

  def stream: fs2.Stream[IO, Unit]
//  def runAsync(f: (Either[Throwable, Unit] => IO[Unit])): IO[Unit]
//  def run: IO[Unit]
}

object MovementProcessor {
  def apply(movementMessageQueue: Queue[IO, MovementRecord], movementLogTable: MovementLogTable) =
    new MovementProcessor {

      private val recordsToLogPipe: Pipe[IO, MovementRecord, Option[MovementLog]] =
        (in: fs2.Stream[IO, MovementRecord]) => in.map(_.toMovementLog)

      private val dbWriter: Sink[IO, MovementLog] =
        fs2.Sink { movementLog: MovementLog =>
          movementLogTable.addRecord(movementLog)
        }

      override def stream: fs2.Stream[IO, Unit] =
        movementMessageQueue.dequeue
          .through(recordsToLogPipe)
          .collect[MovementLog] { case Some(ml) => ml }
          .to(dbWriter)
//
//      override def runAsync(f: (Either[Throwable, Unit] => IO[Unit])) = stream.run.runAsync(f)
//
//      override def run: IO[Unit] = stream.run
    }
}
