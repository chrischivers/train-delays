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
      fs2.Stream.eval(IO(logger.info("Running repopulation process"))) >>
        FullPopulateScheduleTable.run(flushFirst = true) >> PopulateSecondaryScheduleTable.run(flushFirst = true)
    else fs2.Stream.eval(IO(logger.info("Skipping full population of schedule tables")))
    _ <- if (runNow)
      fs2.Stream.eval(IO(logger.info("Running update process"))) >> UpdatePopulateScheduleTable
        .run() >> PopulateSecondaryScheduleTable.run()
    else fs2.Stream.eval(IO(logger.info("Skipping immediate update of schedule tables")))
    scheduler <- Scheduler[IO](1)
    result <- scheduler
      .awakeEvery[IO](24.hours) >> UpdatePopulateScheduleTable.run() >> PopulateSecondaryScheduleTable.run()
  } yield result

  app.compile.drain.unsafeRunSync()
}
