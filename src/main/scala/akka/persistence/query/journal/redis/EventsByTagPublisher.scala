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
package akka
package persistence
package query
package journal
package redis

import util.ByteString

import akka.serialization.SerializationExtension

import akka.persistence.redis._
import RedisKeys._

import _root_.redis._
import api._
import pubsub._

import actor._
import SupervisorStrategy._
import stream.scaladsl._
import stream.actor._
import ActorPublisherMessage.{
  Request,
  Cancel
}

import scala.concurrent.duration._
import scala.concurrent.Future

import com.typesafe.config.Config

private object EventsByTagPublisher {

  def props(conf: Config, redis: RedisClient, tag: String, offset: Long, refreshInterval: FiniteDuration): Props =
    Props(classOf[EventsByTagPublisher], conf, redis, tag, offset, refreshInterval)

  case object Continue
  case class EventRef(sequenceNr: Long, persistenceId: String)
  case class Events(nb: Int, events: Seq[(String, Option[PersistentRepr])])

  implicit object eventRefDeserializer extends ByteStringDeserializer[EventRef] {
    private val EventRe = "(\\d+):(.*)".r
    def deserialize(bs: ByteString): EventRef = bs.utf8String match {
      case EventRe(sequenceNr, persistenceId) => EventRef(sequenceNr.toLong, persistenceId)
      case s                                  => throw new RuntimeException(f"Unable to deserializer $s")
    }
  }

}

private class EventsByTagPublisher(conf: Config, redis: RedisClient, tag: String, offset: Long, refreshInterval: FiniteDuration)
    extends ActorPublisher[EventEnvelope] with ActorLogging {

  import EventsByTagPublisher._

  private val Tag = ByteString(tag)

  private var currentOffset = offset

  private val max = conf.getInt("max")

  private var buf = Vector.empty[EventEnvelope]

  implicit val serialization = SerializationExtension(context.system)

  import context.dispatcher

  val continueTask = context.system.scheduler.schedule(
    refreshInterval, refreshInterval, self, Continue)

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: Exception => Escalate
    }

  val subscription = context.actorOf(Props(classOf[PublisherSubscription], conf, self, tagsChannel), name = "subscription")

  override def postStop(): Unit = {
    continueTask.cancel()
    context.stop(subscription)
  }

  override def receive = waiting()

  def waiting(): Receive = {
    case Request(_) =>
      log.debug("Request received")
      query()

    case Continue =>
      log.debug("Continue received")
      query()

    case Message(`tagsChannel`, Tag) =>
      log.debug("Message received")
      query()

    case Cancel =>
      log.debug("Cancel received")
      context.stop(self)

  }

  def querying(): Receive = {
    case Events(nb, events) =>
      context.become(waiting())
      buf ++= events.zipWithIndex.flatMap {
        case ((persistenceId, Some(repr @ PersistentRepr(event, sequenceNr))), idx) if !repr.deleted =>
          Some(EventEnvelope(Sequence(currentOffset + idx), persistenceId, sequenceNr, event))
        case ((persistenceId, _), idx) =>
          None
      }
      currentOffset += nb
      deliverBuf()

    case Cancel =>
      log.debug("Cancel received")
      context.stop(self)

  }

  private def query(): Unit =
    if (buf.isEmpty) {
      context.become(querying())
      val f = for {
        refs <- redis.lrange[EventRef](tagKey(tag), currentOffset, currentOffset + max - 1)
        trans = redis.transaction()
        events = Future.sequence(refs.map { case EventRef(sequenceNr, persistenceId) => trans.zrangebyscore[Array[Byte]](journalKey(persistenceId), Limit(sequenceNr), Limit(sequenceNr)).map(persistenceId -> _) })
        _ = trans.exec()
        events <- events
      } yield self ! Events(refs.size, events.map { case (id, json) => (id, json.headOption.map(persistentFromBytes(_))) })

      for (t <- f.failed) {
        log.error(t, f"Error while querying events for tag $tag")
        onErrorThenStop(t)
      }
    } else {
      deliverBuf()
    }

  private def deliverBuf(): Unit =
    if (totalDemand > 0 && buf.nonEmpty) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        buf foreach onNext
        buf = Vector.empty
      }
    }

}
