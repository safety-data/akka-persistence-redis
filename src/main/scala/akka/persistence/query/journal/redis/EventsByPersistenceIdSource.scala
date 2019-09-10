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

import akka.persistence.redis._
import RedisKeys._

import _root_.redis._
import api._
import pubsub._

import akka.actor._
import akka.util._
import SupervisorStrategy._
import akka.stream._
import akka.stream.stage._
import akka.serialization.SerializationExtension

import scala.concurrent.duration._

import scala.collection.mutable.Queue

import scala.util.{
  Try,
  Success,
  Failure
}
import scala.reflect._

import com.typesafe.config.Config

private class EventsByPersistenceIdSource(conf: Config, redis: RedisClient, persistenceId: String, from: Long, to: Long, system: ActorSystem, live: Boolean) extends GraphStage[SourceShape[EventEnvelope]] {
  self =>

  val out: Outlet[EventEnvelope] =
    if (live)
      Outlet("EventsByPersistenceIdSource")
    else
      Outlet("CurrentEventsByPersistenceIdSource")

  override val shape: SourceShape[EventEnvelope] = SourceShape(out)

  case class EventRef(sequenceNr: Long, persistenceId: String)

  // The logic class is in one of the following states:
  //  - Waiting for client request
  val Idle = 0
  //  - Buffer was empty so database query was sent
  val Querying = 1
  //  - Database query is running and notification about new event arrived
  val NotifiedWhenQuerying = 2
  //  - Client requested element but no new one in database, waiting for notification
  val WaitingForNotification = 3
  // - Source is initializing
  val Initializing = 4
  // - Downstream requested an element during initialization
  val QueryWhenInitializing = 5

  implicit object eventRefDeserializer extends ByteStringDeserializer[EventRef] {
    private val EventRe = "(\\d+):(.*)".r
    def deserialize(bs: ByteString): EventRef = bs.utf8String match {
      case EventRe(sequenceNr, persistenceId) => EventRef(sequenceNr.toLong, persistenceId)
      case s                                  => throw new RuntimeException(f"Unable to deserializer $s")
    }
  }

  object Long {
    def unapply(bs: ByteString): Option[Long] =
      Try(bs.utf8String.toLong).toOption
  }

  implicit object longFormatter extends ByteStringDeserializer[Long] {
    def deserialize(bs: ByteString): Long =
      bs.utf8String.toLong
  }
  val Channel = journalChannel(persistenceId)

  implicit val serialization = SerializationExtension(system)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) {

      private var state = Idle

      private var buffer = Queue.empty[EventEnvelope]
      private var subscription: RedisPubSub = null
      private val max = conf.getInt("max")
      private var currentSequenceNr = from
      private var to = self.to
      private var callback: AsyncCallback[Seq[PersistentRepr]] = null

      implicit def ec = materializer.executionContext

      override def preStart(): Unit = {
        callback = getAsyncCallback[Seq[PersistentRepr]] { events =>
          if (events.isEmpty) {
            if (currentSequenceNr > to) {
              // end has been reached
              completeStage()
            } else {
              state match {
                case NotifiedWhenQuerying =>
                  // maybe we missed some new event when querying, retry
                  state = Idle
                  query()
                case Querying if live =>
                  // nothing new, wait for notification
                  state = WaitingForNotification
                case Querying =>
                  // seems this journal is empty, complete stream
                  completeStage()
                case _ =>
                  log.error(f"Unexpected source state: $state")
                  failStage(new IllegalStateException(f"Unexpected source state: $state"))
              }
            }
          } else {
            val (evts, maxSequenceNr) = events.foldLeft(Seq.empty[EventEnvelope] -> currentSequenceNr) {
              case ((evts, _), repr @ PersistentRepr(event, sequenceNr)) if !repr.deleted && sequenceNr >= currentSequenceNr && sequenceNr <= to =>
                (evts :+ EventEnvelope(Sequence(sequenceNr), persistenceId, sequenceNr, event), sequenceNr + 1)
              case ((evts, _), PersistentRepr(_, sequenceNr)) =>
                (evts, sequenceNr + 1)

            }
            currentSequenceNr = maxSequenceNr
            log.debug(f"Max sequence number is now $maxSequenceNr")
            if (evts.nonEmpty) {
              buffer ++= evts
              deliver()
            } else {
              // requery immediately
              state = Idle
              query()
            }
          }
        }

        if (live) {
          // subscribe to notification stream only if live stream was required
          val messageCallback = getAsyncCallback[Message] {
            case Message(Channel, Long(sequenceNr)) =>
              log.debug("Message received")
              state match {
                case Idle =>
                // do nothing, no query is running and no client request was performed
                case Querying =>
                  state = NotifiedWhenQuerying
                case NotifiedWhenQuerying =>
                // do nothing we already know that some new events may exist
                case WaitingForNotification =>
                  state = Idle
                  query()
              }
            case Message(ch, _) =>
              if (log.isDebugEnabled)
                log.debug(f"Message from unexpected channel: $ch")
          }

          // subscribe to the identifier change channel to be notifier about new ones
          // and invoke the enqueuing and delivering callback on each message
          subscription = RedisPubSub(
            host = redis.host,
            port = redis.port,
            channels = Seq(Channel),
            patterns = Nil,
            authPassword = redis.password,
            onMessage = messageCallback.invoke)(system)

        } else {
          // start by first querying the current highest sequenceNr
          // for the given persistent id
          // stream will stop once this has been delivered
          state = Initializing

          val initCallback = getAsyncCallback[Long] { sn =>
            if (to > sn) {
              // the initially requested max sequence number is higher than the current
              // one, restrict it to the current one
              to = sn
            }
            state match {
              case QueryWhenInitializing =>
                // during initialization, downstream asked for an element,
                // let’s query elements
                state = Idle
                query()
              case Initializing =>
                // no request from downstream, just go idle
                state = Idle
              case _ =>
                log.error(f"Unexpected source state when initializing: $state")
                failStage(new IllegalStateException(f"Unexpected source state when initializing: $state"))
            }
          }

          val f = redis.get[Long](highestSequenceNrKey(persistenceId))

          f.onComplete {
            case Success(Some(sn)) =>
              initCallback.invoke(sn)
            case Success(None) =>
              // not found, close
              val cb = getAsyncCallback[Unit] { _ => completeStage() }
              cb.invoke(())
            case Failure(t) =>
              log.error(t, "Error while initializing current events by persistent id")
              val cb = getAsyncCallback[Unit] { _ => failStage(t) }
              cb.invoke(())
          }
        }

      }

      override def postStop(): Unit = if (subscription != null) {
        subscription.stop()
      }

      private val StringSeq = classTag[Seq[String]]

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          state match {
            case Initializing =>
              state = QueryWhenInitializing
            case _ =>
              query()
          }
      })

      private def query(): Unit =
        state match {
          case Idle =>
            if (buffer.isEmpty) {
              // so, we need to fill this buffer
              state = Querying
              redis.zrangebyscore[Array[Byte]](journalKey(persistenceId), Limit(currentSequenceNr), Limit(to), Some(0L -> max)).onComplete {
                case Success(events) =>
                  callback.invoke(events.map(persistentFromBytes(_)))
                case Failure(t) =>
                  log.error(t, "Error while querying events by persistence identifier")
                  val cb = getAsyncCallback[Throwable](t => failStage(t))
              }
            } else {
              // buffer is non empty, let’s deliver buffered data
              deliver()
            }
          case _ =>
            log.error(f"Unexpected source state when querying: $state")
            failStage(new IllegalStateException(f"Unexpected source state when querying: $state"))
        }

      private def deliver(): Unit = {
        // go back to idle state, waiting for more client request
        state = Idle
        val elem = buffer.dequeue
        push(out, elem)
        if (buffer.isEmpty && currentSequenceNr > to) {
          // we delivered last buffered event and the upper bound was reached, complete
          completeStage()
        }
      }

    }

}
