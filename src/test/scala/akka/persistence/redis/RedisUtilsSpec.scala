package akka.persistence.redis

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FlatSpec, Matchers }

class RedisUtilsSpec extends FlatSpec with Matchers {

  "A RedisClient" should "use redis.sentinels as config if sentinel-list is also present" in {
    val config = ConfigFactory.parseString(s"""
         |redis {
         |  mode = sentinel
         |  master = foo
         |  database = 0
         |  sentinel-list = "host1:1234,host2:1235"
         |  sentinels = [
         |   {
         |     host = "host3"
         |     port = 1236
         |    },
         |    {
         |      host = "host4"
         |      port = 1237
         |     }
         |   ]
         |}""".stripMargin)
    val sentinels = RedisUtils.sentinels(config)
    sentinels should be(Some(List(("host3", 1236), ("host4", 1237))))
  }

  it should "use redis.sentinel-list if redis.sentinels is not present" in {
    val config = ConfigFactory.parseString(s"""
      |redis {
      |  mode = sentinel
      |  master = foo
      |  database = 0
      |  sentinel-list = "host1:1234,host2:1235"
      |}""".stripMargin)
    val sentinels = RedisUtils.sentinels(config)
    sentinels should be(Some(List(("host1", 1234), ("host2", 1235))))
  }

}
