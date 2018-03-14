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
import akka.stream._
import akka.stream.stage._
import akka.serialization.SerializationExtension

import scala.concurrent._
import scala.concurrent.duration._

import scala.collection.mutable.Queue

import scala.util.{
  Try,
  Success,
  Failure
}
import scala.reflect._

import com.typesafe.config.Config

private class EventsByTagSource(conf: Config, redis: RedisClient, tag: String, offset: Long, system: ActorSystem, live: Boolean) extends GraphStage[SourceShape[EventEnvelope]] {

  val out: Outlet[EventEnvelope] =
    if (live)
      Outlet("EventsByTagSource")
    else
      Outlet("CurrentEventsByTagSource")

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

  val Tag = ByteString(tag)

  implicit val serialization = SerializationExtension(system)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) {

      private var state = Idle

      private var buffer = Queue.empty[EventEnvelope]
      private var subscription: RedisPubSub = null
      private val max = conf.getInt("max")
      private var currentOffset = offset
      private var maxOffset = Long.MaxValue
      private var callback: AsyncCallback[(Int, Seq[(String, Option[PersistentRepr])])] = null

      implicit def ec = materializer.executionContext

      override def preStart(): Unit = {
        callback = getAsyncCallback[(Int, Seq[(String, Option[PersistentRepr])])] {
          case (nb, events) =>
            if (events.isEmpty) {
              if (currentOffset >= maxOffset) {
                // end has been reached
                completeStage()
              } else {
                state match {
                  case NotifiedWhenQuerying =>
                    // maybe we missed some new event when querying, retry
                    state = Idle
                    query()
                  case Querying =>
                    if (live) {
                      // nothing new, wait for notification
                      state = WaitingForNotification
                    } else {
                      // not a live stream, nothing else currently in the database, close the stream
                      completeStage()
                    }
                  case _ =>
                    log.error(f"Unexpected source state: $state")
                    failStage(new IllegalStateException(f"Unexpected source state: $state"))
                }
              }
            } else {
              val evts = events.zipWithIndex.flatMap {
                case ((persistenceId, Some(repr @ PersistentRepr(event, sequenceNr))), idx) if !repr.deleted =>
                  Some(EventEnvelope(Sequence(currentOffset + idx), persistenceId, sequenceNr, event))
                case ((persistenceId, _), idx) =>
                  None
              }
              currentOffset += nb
              if (evts.nonEmpty) {
                buffer.enqueue(evts: _*)
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
            case Message(`tagsChannel`, Tag) =>
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
            case Message(`tagsChannel`, _) =>
            // ignore other tags
            case Message(ch, _) =>
              if (log.isDebugEnabled)
                log.debug(f"Message from unexpected channel: $ch")
          }

          // subscribe to the identifier change channel to be notifier about new ones
          // and invoke the enqueuing and delivering callback on each message
          subscription = RedisPubSub(
            host = redis.host,
            port = redis.port,
            channels = Seq(tagsChannel),
            patterns = Nil,
            authPassword = redis.password,
            onMessage = messageCallback.invoke)(system)
        } else {
          // start by first querying the current length of tag events
          // for the given tag
          // stream will stop once this has been delivered
          state = Initializing

          val initCallback = getAsyncCallback[Long] { len =>
            maxOffset = len
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

          val f = redis.llen(tagKey(tag))

          f.onComplete {
            case Success(len) =>
              initCallback.invoke(len - 1)
            case Failure(t) =>
              log.error(t, "Error while initializing current events by tag")
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
              val f = for {
                // request next batch of events for this tag (potentially limiting to the max offset in the case of non live stream)
                refs <- redis.lrange[EventRef](tagKey(tag), currentOffset, math.min(maxOffset, currentOffset + max - 1))
                trans = redis.transaction()
                events = Future.sequence(refs.map { case EventRef(sequenceNr, persistenceId) => trans.zrangebyscore[Array[Byte]](journalKey(persistenceId), Limit(sequenceNr), Limit(sequenceNr)).map(persistenceId -> _) })
                _ <- trans.exec()
                events <- events
              } yield {
                (refs.size, events.map {
                  case (id, bytes) =>
                    (id, bytes.headOption.map(persistentFromBytes(_)))
                })
              }

              f.onComplete {
                case Success((nb, events)) =>
                  callback.invoke((nb, events))
                case Failure(t) =>
                  log.error(t, "Error while querying events by persistence identifier")
                  val cb = getAsyncCallback[Unit] { _ => failStage(t) }
                  cb.invoke(())
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
        if (buffer.isEmpty && currentOffset >= maxOffset) {
          // max offset has been reached and delivered, complete
          completeStage()
        }
      }

    }

}
