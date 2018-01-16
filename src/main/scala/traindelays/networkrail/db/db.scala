package traindelays.networkrail

import cats.effect.IO
import fs2.Stream
import org.flywaydb.core.Flyway
import traindelays.DatabaseConfig
import doobie.hikari._, doobie.hikari.implicits._

package object db {

  def setUpTransactor(config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ()) =
    for {
      transactor <- HikariTransactor[IO](config.driverClassName, config.url, config.username, config.password)
      _ <- transactor
        .configure { datasource =>
          datasource.setMaximumPoolSize(config.maximumPoolSize)
          val flyway = new Flyway()
          flyway.setDataSource(datasource)
          flyway.setLocations("db/migration")
          flyway.migrate()

        }

    } yield transactor

  def withTransactor[A](config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ())(
      f: HikariTransactor[IO] => Stream[IO, A]): Stream[IO, A] =
    Stream.bracket(setUpTransactor(config)(beforeMigration))(
      f,
      (t: HikariTransactor[IO]) => t.shutdown
    )

  trait Table[A] {

    def addRecord(record: A): IO[Unit]

    def retrieveAllRecords(): IO[List[A]]

    val dbWriter: fs2.Sink[IO, A] = fs2.Sink { record =>
      addRecord(record)
    }
  }

}
