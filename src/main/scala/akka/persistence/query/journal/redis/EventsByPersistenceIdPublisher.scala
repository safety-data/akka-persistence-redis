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

import scala.util.Try

import com.typesafe.config.Config

import spray.json._

private object EventsByPersistenceIdPublisher {

  def props(conf: Config, redis: RedisClient, persistenceId: String, from: Long, to: Long, refreshInterval: FiniteDuration): Props =
    Props(classOf[EventsByPersistenceIdPublisher], conf, redis, persistenceId, from, to, refreshInterval)

  case object Continue
  case class EventRef(sequenceNr: Long, persistenceId: String)
  case class Events(events: Seq[JournalEntry])

  implicit object eventRefDeserializer extends ByteStringDeserializer[EventRef] {
    private val EventRe = "(\\d+):(.*)".r
    def deserialize(bs: ByteString): EventRef = bs.utf8String match {
      case EventRe(sequenceNr, persistenceId) => EventRef(sequenceNr.toLong, persistenceId)
      case s                                  => throw new RuntimeException(f"Unable to deserializer $s")
    }
  }

}

private class EventsByPersistenceIdPublisher(conf: Config, redis: RedisClient, persistenceId: String, from: Long, to: Long, refreshInterval: FiniteDuration)
    extends ActorPublisher[EventEnvelope] with ActorLogging {

  import EventsByPersistenceIdPublisher._

  object Long {
    def unapply(bs: ByteString): Option[Long] =
      Try(bs.utf8String.toLong).toOption
  }

  private val Channel = journalChannel(persistenceId)

  private var currentSequenceNr = from

  private val max = conf.getInt("max")

  private var buf = Vector.empty[EventEnvelope]

  import context.dispatcher

  val continueTask = context.system.scheduler.schedule(
    refreshInterval, refreshInterval, self, Continue)

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: Exception => Escalate
    }

  val subscription = context.actorOf(Props(classOf[PublisherSubscription], conf, self, Channel), name = "subscription")

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

    case Message(Channel, Long(sequenceNr)) =>
      log.debug("Message received")
      if (sequenceNr >= currentSequenceNr)
        query()

    case Cancel =>
      log.debug("Cancel received")
      context.stop(self)

  }

  def querying(): Receive = {
    case Events(events) =>
      context.become(waiting())
      val (evts, maxSequenceNr) = events.foldLeft(Seq.empty[EventEnvelope] -> currentSequenceNr) {
        case ((evts, _), JournalEntry(sequenceNr, false, manifest, event, _)) =>
          (evts :+ EventEnvelope(sequenceNr, persistenceId, sequenceNr, event), sequenceNr + 1)
        case ((evts, _), JournalEntry(sequenceNr, _, _, _, _)) =>
          (evts, sequenceNr + 1)

      }
      buf ++= evts
      currentSequenceNr = maxSequenceNr
      deliverBuf()

    case Cancel =>
      log.debug("Cancel received")
      context.stop(self)

  }

  private def query(): Unit =
    if (buf.isEmpty) {
      if (currentSequenceNr < to) {
        context.become(querying())
        val f = for {
          events <- redis.zrangebyscore[JsValue](journalKey(persistenceId), Limit(currentSequenceNr), Limit(math.min(currentSequenceNr + max - 1, to)))
        } yield self ! Events(events.map(_.convertTo[JournalEntry]))

        for (t <- f.failed) {
          log.error(t, "Error while querying events by persistence identifier")
          onErrorThenStop(t)
        }
      } else {
        onCompleteThenStop()
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

