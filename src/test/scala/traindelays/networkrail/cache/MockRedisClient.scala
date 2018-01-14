package traindelays.networkrail.cache

import akka.actor.ActorSystem
import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}

import scala.collection.mutable
import scala.concurrent.Future

object MockRedisClient {

  def apply[T]()(implicit actorSystem: ActorSystem) = new RedisClient {

    var store: mutable.Map[String, (Long, T)] = mutable.Map[String, (Long, T)]()

    override def set[V](key: String,
                        value: V,
                        exSeconds: Option[Long],
                        pxMilliseconds: Option[Long],
                        NX: Boolean,
                        XX: Boolean)(implicit evidence$10: ByteStringSerializer[V]): Future[Boolean] =
      value match {
        case value: V =>
          Future(
            store += (key -> (pxMilliseconds.fold(-1L)(expire => System.currentTimeMillis() + expire), value
              .asInstanceOf[T]))).map(_ => true)
        case _ => throw new IllegalStateException("Value not of type R")
      }

    override def get[R](key: String)(implicit evidence$2: ByteStringDeserializer[R]): Future[Option[R]] =
      store.get(key) match {
        case value: Option[(Long, R)] =>
          Future {
            value.flatMap { v =>
              if (v._1 == -1L) Some(v._2)
              else if (v._1 < System.currentTimeMillis()) None
              else Some(v._2)
            }
          }
        case _ => throw new IllegalStateException("Value returned from cache not of type R")
      }
  }
}
