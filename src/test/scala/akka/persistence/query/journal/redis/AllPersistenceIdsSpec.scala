/** Copyright (C) 2015-2017 Lightbend Inc. <http://www.lightbend.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.persistence.query.journal.redis

import scala.concurrent.duration._

import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.PersistenceIdsQuery
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

object AllPersistenceIdsSpec {
  val config = """
    akka.loglevel = INFO
    akka.persistence.journal.plugin = "akka-persistence-redis.journal"
    akka.test.single-expect-default = 10s
    """
}

class AllPersistenceIdsSpec extends AkkaSpec(AllPersistenceIdsSpec.config)
  with Cleanup with ImplicitSender {

  implicit val mat = ActorMaterializer()(system)

  val queries = PersistenceQuery(system).readJournalFor[ScalaReadJournal](RedisReadJournal.Identifier)

  "Redis query AllPersistenceIds" must {

    "implement standard AllPersistenceIdsQuery" in {
      queries.isInstanceOf[PersistenceIdsQuery] should ===(true)
    }

    "find existing persistenceIds" in {
      system.actorOf(TestActor.props("a")) ! "a1"
      expectMsg("a1-done")
      system.actorOf(TestActor.props("b")) ! "b1"
      expectMsg("b1-done")
      system.actorOf(TestActor.props("c")) ! "c1"
      expectMsg("c1-done")

      val src = queries.currentPersistenceIds()
      val probe = src.runWith(TestSink.probe[String])
      probe.within(10.seconds) {
        probe.request(5)
          .expectNextUnordered("a", "b", "c")
          .expectComplete()
      }
    }

    "find new persistenceIds" in {
      // a, b, c created by previous step
      system.actorOf(TestActor.props("d")) ! "d1"
      expectMsg("d1-done")

      val src = queries.persistenceIds()
      val probe = src.runWith(TestSink.probe[String])
      probe.within(10.seconds) {
        probe.request(5)
          .expectNextUnorderedN(List("a", "b", "c", "d"))

        system.actorOf(TestActor.props("e")) ! "e1"
        probe.expectNext("e")

        val more = (1 to 100).map("f" + _)
        more.foreach { p =>
          system.actorOf(TestActor.props(p)) ! p
        }

        probe.request(100)
        probe.expectNextUnorderedN(more)
      }

    }
  }

}
