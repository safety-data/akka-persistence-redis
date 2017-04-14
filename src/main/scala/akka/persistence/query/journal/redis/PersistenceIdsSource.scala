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
import api.pubsub._

import akka.actor._
import SupervisorStrategy._
import akka.stream._
import akka.stream.stage._

import scala.concurrent.duration._

import scala.collection.mutable.Queue

import scala.util.{
  Success,
  Failure
}
import scala.reflect._

import com.typesafe.config.Config

private class PersistenceIdsSource(conf: Config, redis: RedisClient, system: ActorSystem) extends GraphStage[SourceShape[String]] {

  val out: Outlet[String] = Outlet("PersistenceIdsSource")

  override val shape: SourceShape[String] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) {

      private var start = true
      private var index = 0
      private var buffer = Queue.empty[String]
      private var downstreamWaiting = false
      private var subscription: RedisPubSub = null

      implicit def ec = materializer.executionContext

      override def preStart(): Unit = {
        val callback = getAsyncCallback[Message] {
          case Message(`identifiersChannel`, bs) =>
            log.debug("Message received")
            // enqueue the element
            buffer.enqueue(bs.utf8String)
            // deliver if need be
            deliver()
          case Message(ch, _) =>
            if (log.isDebugEnabled)
              log.debug(f"Message from unexpected channel: $ch")
        }

        // subscribe to the identifier change channel to be notifier about new ones
        // and invoke the enqueuing and delivering callback on each message
        subscription = RedisPubSub(host = RedisUtils.host(conf),
          port = RedisUtils.port(conf),
          channels = Seq(identifiersChannel),
          patterns = Nil,
          authPassword = RedisUtils.password(conf),
          onMessage = callback.invoke)(system)

      }

      override def postStop(): Unit = if (subscription != null) {
        subscription.stop()
      }

      private val StringSeq = classTag[Seq[String]]

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          downstreamWaiting = true
          if (buffer.isEmpty && (start || index > 0)) {
            val callback = getAsyncCallback[Cursor[Seq[String]]] {
              case Cursor(idx, StringSeq(data)) =>
                // save the index for further initialization if needed
                index = idx
                // it is not the start anymore
                start = false
                // enqueue received data
                buffer.enqueue(data: _*)
                // deliver element
                deliver()
            }

            redis.sscan[String](identifiersKey, index).onComplete {
              case Success(cursor) =>
                callback.invoke(cursor)
              case Failure(t) =>
                log.error(t, "Error while querying persistence identifiers")
                failStage(t)
            }

          } else if (buffer.isEmpty) {
            // wait for asynchornous notification and mark dowstream
            // as waiting for data
          } else {
            deliver()
          }
        }
      })

      private def deliver(): Unit = if (downstreamWaiting) {
        downstreamWaiting = false
        val elem = buffer.dequeue
        push(out, elem)
      }

    }

}
