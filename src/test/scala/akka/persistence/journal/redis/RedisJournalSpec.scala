/*
 * Copyright © 2017 Safety Data - CFH SAS.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package akka.persistence
package journal
package redis

import akka.persistence.redis._

import _root_.redis._

import com.typesafe.config.ConfigFactory

import scala.concurrent._
import scala.concurrent.duration._

class RedisJournalSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
    |akka.persistence.journal.plugin = "akka-persistence-redis.journal"
    |""".stripMargin)) {

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    CapabilityFlag.on

  override def afterAll(): Unit = {
    val redis = RedisUtils.create(ConfigFactory.load.getConfig("akka-persistence-redis"))
    Await.result(redis.eval("return redis.call('del', unpack(redis.call('keys', ARGV[1])))", args = Seq("journal:*")), 10.seconds)
    super.afterAll()
  }

}
