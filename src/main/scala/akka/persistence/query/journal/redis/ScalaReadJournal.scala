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

import actor._
import stream.scaladsl._

import akka.persistence.redis._
import RedisKeys._

import scaladsl._

import com.typesafe.config.Config

import _root_.redis._

import scala.concurrent.duration._

class ScalaReadJournal private[redis] (system: ExtendedActorSystem, conf: Config) extends ReadJournal
  with EventsByTagQuery
  with EventsByPersistenceIdQuery
  with PersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with CurrentEventsByPersistenceIdQuery
  with CurrentEventsByTagQuery {

  val redis = RedisUtils.create(conf)(system)

  /** Returns the live stream of persisted identifiers.
   *  Identifiers may appear several times in the stream.
   */
  def persistenceIds(): Source[String, NotUsed] = {
    Source.fromGraph(new PersistenceIdsSource(conf, redis, system))
  }

  /** Returns the stream of current persisted identifiers.
   *  This stream is not live, once the identifiers were all returned, it is closed.
   */
  def currentPersistenceIds(): Source[String, NotUsed] =
    Source.fromGraph(new CurrentPersistenceIdsSource(redis))

  /** Returns the live stream of events for the given `persistenceId`.
   *  Events are ordered by `sequenceNr`.
   *  When the `toSequenceNr` has been delivered, the stream is closed.
   */
  def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    Source.fromGraph(new EventsByPersistenceIdSource(conf, redis, persistenceId, fromSequenceNr, toSequenceNr, system, true))

  /** Returns the stream of current events for the given `persistenceId`.
   *  Events are ordered by `sequenceNr`.
   *  When the `toSequenceNr` has been delivered or no more elements are available at the current time, the stream is closed.
   */
  def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    Source.fromGraph(new EventsByPersistenceIdSource(conf, redis, persistenceId, fromSequenceNr, toSequenceNr, system, false))

  /** Returns the live stream of events with a given tag.
   *  The events are sorted in the order they occurred, you can rely on it.
   */
  def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = offset match {
    case NoOffset =>
      Source.fromGraph(new EventsByTagSource(conf, redis, tag, 0L, system, true))
    case Sequence(offsetValue) =>
      Source.fromGraph(new EventsByTagSource(conf, redis, tag, offsetValue, system, true))
    case _ =>
      throw new IllegalArgumentException("Redis does not support " + offset.getClass.getName + " offsets")
  }

  /** Returns the stream of current events with a given tag.
   *  The events are sorted in the order they occurred, you can rely on it.
   *
   *  Returned events are those present in the store with the given tag at the time
   *  the stream is opened.
   *
   *  Events deleted during this stream life might not appear in the stream if not delivered yet.
   *
   *  Stream is closed once all events present at the time of opening have been delivered.
   *
   */
  def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = offset match {
    case NoOffset =>
      Source.fromGraph(new EventsByTagSource(conf, redis, tag, 0L, system, false))
    case Sequence(offsetValue) =>
      Source.fromGraph(new EventsByTagSource(conf, redis, tag, offsetValue, system, false))
    case _ =>
      throw new IllegalArgumentException("Redis does not support " + offset.getClass.getName + " offsets")
  }

}
