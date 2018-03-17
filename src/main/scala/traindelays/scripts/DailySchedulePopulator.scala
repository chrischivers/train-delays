package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.Scheduler

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object DailySchedulePopulator extends App with StrictLogging {

  val repopulate = args.toList.contains("repopulate")
  val runNow     = args.toList.contains("run-now")

  val app: fs2.Stream[IO, Unit] = for {
    _ <- if (repopulate)
      FullPopulateScheduleTable.run(flushFirst = true) >> PopulateSecondaryScheduleTable.run(flushFirst = true)
    else fs2.Stream.eval(IO(logger.info("Skipping full population of schedule tables")))
    _ <- if (runNow) UpdatePopulateScheduleTable.run() >> PopulateSecondaryScheduleTable.run()
    else fs2.Stream.eval(IO.unit)
    scheduler <- Scheduler[IO](1)
    result <- scheduler
      .awakeEvery[IO](24.hours) >> UpdatePopulateScheduleTable.run() >> PopulateSecondaryScheduleTable.run()
  } yield result

  app.compile.drain.unsafeRunSync()
}
