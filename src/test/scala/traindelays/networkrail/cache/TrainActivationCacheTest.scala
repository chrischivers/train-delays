package traindelays.networkrail.cache

import akka.actor.ActorSystem
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.TestFeatures
import traindelays.networkrail.movementdata.TrainId
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TrainActivationCacheTest extends FlatSpec {

  implicit val actorSystem: ActorSystem = ActorSystem()

  it should "add and retrieve record to/from train activation cache" in {
    val redisClient          = MockRedisClient()
    val expiry               = 5 seconds
    val trainActivationCache = TrainActivationCache(redisClient, expiry)

    val trainId         = TrainId("G12345678")
    val scheduleTrainId = ScheduleTrainId("ABCDE")

    trainActivationCache.addToCache(trainId, scheduleTrainId).unsafeRunSync()

    val recordFromCache = trainActivationCache.getFromCache(trainId).unsafeRunSync()
    recordFromCache shouldBe Some(scheduleTrainId)
  }

  it should "return none if records not found in cache" in {
    val redisClient          = MockRedisClient()
    val expiry               = 5 seconds
    val trainActivationCache = TrainActivationCache(redisClient, expiry)

    val trainId         = TrainId("G12345678")
    val scheduleTrainId = ScheduleTrainId("ABCDE")

    val recordFromCache = trainActivationCache.getFromCache(trainId).unsafeRunSync()
    recordFromCache shouldBe None
  }

  it should "return none if records are expired in cache" in {
    val redisClient          = MockRedisClient()
    val expiry               = 2 seconds
    val trainActivationCache = TrainActivationCache(redisClient, expiry)

    val trainId         = TrainId("G12345678")
    val scheduleTrainId = ScheduleTrainId("ABCDE")

    trainActivationCache.addToCache(trainId, scheduleTrainId).unsafeRunSync()

    Thread.sleep(2000)

    val recordFromCache = trainActivationCache.getFromCache(trainId).unsafeRunSync()
    recordFromCache shouldBe None
  }

}
