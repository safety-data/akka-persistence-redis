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

private class CurrentPersistenceIdsSource(redis: RedisClient) extends GraphStage[SourceShape[String]] {

  val out: Outlet[String] = Outlet("CurrentPersistenceIdsSource")

  override val shape: SourceShape[String] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) {

      private var start = true
      private var index = 0
      private var buffer = Queue.empty[String]

      implicit def ec = materializer.executionContext

      private val StringSeq = classTag[Seq[String]]

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (buffer.isEmpty && (start || index > 0)) {
            val callback = getAsyncCallback[Cursor[Seq[String]]] {
              case Cursor(idx, StringSeq(data)) =>
                // save the index for further initialization if needed
                index = idx
                // it is not the start anymore
                start = false
                // enqueue received data
                buffer ++= data
                // deliver element
                deliver()
            }

            redis.sscan[String](identifiersKey, index).onComplete {
              case Success(cursor) =>
                callback.invoke(cursor)
              case Failure(t) =>
                log.error(t, "Error while querying persistence identifiers")
                val cb = getAsyncCallback[Unit] { _ => failStage(t) }
                cb.invoke(())
            }

          } else {
            deliver()
          }
        }
      })

      private def deliver(): Unit = {
        if (buffer.nonEmpty) {
          val elem = buffer.dequeue
          push(out, elem)
        } else {
          // we're done here, goodbye
          completeStage()
        }
      }

    }

}

