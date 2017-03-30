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

import com.typesafe.config.ConfigFactory

class RedisJournalSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
    |akka.persistence.journal.plugin = "akka-persistence-redis.journal"
    |
    |akka-persistence-redis {
    |  journal {
    |    event-adapters {
    |      serializer = "akka.persistence.journal.redis.StringSerializer"
    |    }
    |    event-adapter-bindings {
    |      "java.lang.String" = serializer
    |      "spray.json.JsValue" = serializer
    |    }
    |  }
    |}""".stripMargin)) {

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    CapabilityFlag.on
}
