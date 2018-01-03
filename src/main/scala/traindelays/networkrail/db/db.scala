package traindelays.networkrail

import cats.effect.IO
import fs2.Stream
import org.flywaydb.core.Flyway
import traindelays.DatabaseConfig

package object db {

  import doobie.hikari._, doobie.hikari.implicits._

  def transactor(config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ()) =
    for {
      transactor <- HikariTransactor[IO](config.driverClassName, config.url, config.username, config.password)
      _ <- transactor
        .configure { datasource =>
          datasource.setMaximumPoolSize(config.maximumPoolSize)
          if (config.migrationScripts.nonEmpty) {
            val flyway = new Flyway()
            flyway.setDataSource(datasource)
            flyway.setLocations(config.migrationScripts: _*)
            flyway.migrate()
          }
        }

    } yield transactor

  def usingTransactor[A](config: DatabaseConfig)(beforeMigration: Flyway => Unit = _ => ())(
      f: HikariTransactor[IO] => Stream[IO, A]): Stream[IO, A] =
    Stream.bracket(transactor(config)(beforeMigration))(
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
