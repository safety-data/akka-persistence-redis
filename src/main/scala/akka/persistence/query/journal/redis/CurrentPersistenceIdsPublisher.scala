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

import actor._
import stream.scaladsl._
import stream.actor._
import ActorPublisherMessage.{
  Request,
  Cancel
}

import scala.reflect._

import scala.concurrent.duration._

private object CurrentPersistenceIdsPublisher {
  def props(redis: RedisClient, refreshInterval: FiniteDuration): Props =
    Props(classOf[CurrentPersistenceIdsPublisher], redis, refreshInterval)
}

private class CurrentPersistenceIdsPublisher(redis: RedisClient, refreshInterval: FiniteDuration) extends ActorPublisher[String] with ActorLogging {

  private case class Continue(index: Int)

  import context.dispatcher

  private var index = 0

  val continueTask = context.system.scheduler.schedule(
    refreshInterval, refreshInterval, self, Continue)

  log.debug("Starting PersistenceIdsPublisher")

  override def postStop(): Unit = {
    log.debug("Stopping PersistenceIdsPublisher")
    continueTask.cancel()
  }

  def receive = waiting()

  def waiting(): Receive = {
    case Request(_) =>
      log.debug("Request received")
      query(true)

    case Continue =>
      log.debug("Continue received")
      query(false)

    case Cancel =>
      log.debug("Cancel received")
      context.stop(self)
  }

  val StringSeq = classTag[Seq[String]]

  def querying(): Receive = {
    case Cursor(idx, StringSeq(data)) =>
      context.become(waiting())
      index = idx
      buf ++= data
      deliverBuf()
  }

  private var buf = Vector.empty[String]

  private def query(start: Boolean): Unit =
    if (buf.isEmpty && (start || index > 0)) {
      context.become(querying())
      val f = for (cursor <- redis.sscan[String](identifiersKey, index))
        yield self ! cursor

      for (t <- f.failed) {
        log.error(t, "Error while querying persistence identifiers")
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
      if (buf.isEmpty && index <= 0) {
        onCompleteThenStop()
      }
    }

}

