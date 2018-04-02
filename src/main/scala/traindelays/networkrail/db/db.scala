package traindelays.networkrail

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream
import org.flywaydb.core.Flyway
import traindelays.DatabaseConfig
import doobie.hikari._
import doobie.hikari.implicits._

import scala.concurrent.duration.FiniteDuration
import scalacache.memoization.memoizeF

package object db {

  private val migrationLocation = "db/migration/common"

  def setUpTransactor(config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ()) =
    for {
      transactor <- HikariTransactor
        .newHikariTransactor[IO](config.driverClassName, config.url, config.username, config.password)
      _ <- transactor.configure { datasource =>
        IO {
          val flyway = new Flyway()
          flyway.setDataSource(datasource)
          flyway.setLocations(migrationLocation)
          beforeMigration(flyway)
          flyway.migrate()
        }
      }
    } yield transactor

  def withTransactor[A](config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ())(
      f: HikariTransactor[IO] => Stream[IO, A]): Stream[IO, A] =
    Stream.bracket(setUpTransactor(config)(beforeMigration))(
      f,
      (t: HikariTransactor[IO]) => t.shutdown
    )

  protected trait Table[A] extends StrictLogging {

    protected def addRecord(record: A): IO[Unit]

    def safeAddRecord(record: A): IO[Unit] =
      addRecord(record).attempt
        .map {
          case Right(_) => ()
          case Left(err) =>
            logger.error(s"Error adding record to DB $record.", err)
        }

    protected def retrieveAll(): IO[List[A]]

  }

  trait MemoizedTable[A] extends Table[A] {

    import scalacache.CatsEffect.modes._
    import scalacache._
    import scalacache.guava._

    implicit protected val memoizeCache: Cache[List[A]] = GuavaCache[List[A]]
    val memoizeFor: FiniteDuration

    def retrieveAllRecords(forceRefresh: Boolean = false): IO[List[A]] =
      if (forceRefresh) retrieveAll()
      else {
        memoizeF(Some(memoizeFor)) {
          retrieveAll()
        }
      }
  }

  trait NonMemoizedTable[A] extends Table[A] {

    def retrieveAllRecords(): IO[List[A]] =
      retrieveAll()
  }

}
