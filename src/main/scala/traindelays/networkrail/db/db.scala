package traindelays.networkrail

import cats.effect.IO
import fs2.Stream
import org.flywaydb.core.Flyway
import traindelays.DatabaseConfig
import doobie.hikari._
import doobie.hikari.implicits._
import traindelays.networkrail.db.Table

import scala.concurrent.duration.FiniteDuration
import scalacache.memoization.memoizeF

package object db {

  private val commonMigrationLocation    = "db/migration/common"
  private val h2MigrationsLocation       = "db/migration/h2"
  private val postgresMigrationsLocation = "db/migration/postgres"

  def setUpTransactor(config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ()) =
    for {
      transactor <- HikariTransactor[IO](config.driverClassName, config.url, config.username, config.password)
      _ <- transactor
        .configure { datasource =>
          datasource.setMaximumPoolSize(config.maximumPoolSize)
          val flyway = new Flyway()
          flyway.setDataSource(datasource)
          if (config.driverClassName.contains("h2")) {
            flyway.setLocations(commonMigrationLocation, h2MigrationsLocation)
          } else {
            flyway.setLocations(commonMigrationLocation, postgresMigrationsLocation)
          }
          flyway.migrate()

        }

    } yield transactor

  def withTransactor[A](config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ())(
      f: HikariTransactor[IO] => Stream[IO, A]): Stream[IO, A] =
    Stream.bracket(setUpTransactor(config)(beforeMigration))(
      f,
      (t: HikariTransactor[IO]) => t.shutdown
    )

  protected trait Table[A] {

    def addRecord(record: A): IO[Unit]

    protected def retrieveAll(): IO[List[A]]

    val dbWriter: fs2.Sink[IO, A] = fs2.Sink { record =>
      addRecord(record)
    }
  }

  trait MemoizedTable[A] extends Table[A] {

    import scalacache.CatsEffect.modes._
    import scalacache._
    import scalacache.guava._

    implicit private val memoizeCache: Cache[List[A]] = GuavaCache[List[A]]
    val memoizeFor: FiniteDuration

    def retrieveAllRecords(): IO[List[A]] =
      memoizeF(Some(memoizeFor)) {
        retrieveAll()
      }
  }

  trait NonMemoizedTable[A] extends Table[A] {

    def retrieveAllRecords(): IO[List[A]] =
      retrieveAll()
  }

}
