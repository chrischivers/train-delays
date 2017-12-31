package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import fs2.{Pipe, Sink}
import traindelays.networkrail.db.MovementLogTable

trait MovementProcessor {

  val recordsToLogPipe: Pipe[IO, MovementRecord, Option[MovementLog]]
  val dbWriter: Sink[IO, MovementLog]
}

object MovementLogger {
  def apply(movementMessageQueue: Queue[IO, MovementRecord], movementLogTable: MovementLogTable) =
    new MovementProcessor {

      override val recordsToLogPipe: Pipe[IO, MovementRecord, Option[MovementLog]] =
        (in: fs2.Stream[IO, MovementRecord]) => in.map(_.toMovementLog)

      override val dbWriter: Sink[IO, MovementLog] =
        fs2.Sink { movementLog: MovementLog =>
          movementLogTable.addRecord(movementLog)
        }

      movementMessageQueue.dequeue
        .through(recordsToLogPipe)
        .collect[MovementLog] { case Some(ml) => ml }
        .to(dbWriter)

    }
}
