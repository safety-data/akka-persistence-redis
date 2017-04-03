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
    with EventsByTagQuery2
    with EventsByPersistenceIdQuery
    with AllPersistenceIdsQuery
    with CurrentPersistenceIdsQuery {

  val redis = RedisUtils.create(conf)(system)

  /** Returns the live stream of persisted identifiers.
   *  Identifiers may appear several times in the stream.
   */
  def allPersistenceIds(): Source[String, NotUsed] = {
    val props = AllPersistenceIdsPublisher.props(conf, redis, conf.getDuration("refresh-interval", MILLISECONDS).milliseconds)
    Source.actorPublisher[String](props)
      .mapMaterializedValue(_ => NotUsed)
  }

  /** Returns the stream of current persisted identifiers.
   *  This stream is not live, once, the identifiers were all returned, it is closed.
   */
  def currentPersistenceIds(): Source[String, NotUsed] = {
    val props = PersistenceIdsPublisher.props(redis, conf.getDuration("refresh-interval", MILLISECONDS).milliseconds)
    Source.actorPublisher[String](props)
      .mapMaterializedValue(_ => NotUsed)
  }

  /** Returns the live stream of events for the given `persistenceId`.
   *  Events are ordered by `sequenceNr`.
   *  When the `toSequenceNr` has been delivered, the stream is closed.
   */
  def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val props = EventsByPersistenceIdPublisher.props(conf, redis, persistenceId, fromSequenceNr, toSequenceNr, conf.getDuration("refresh-interval", MILLISECONDS).milliseconds)
    Source.actorPublisher[EventEnvelope](props)
      .mapMaterializedValue(_ => NotUsed)
  }

  /** Returns the live stream of events with a given tag.
   *  The events are sorted in the order they occurred, you can rely on it.
   */
  def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope2, NotUsed] = offset match {
    case Sequence(offsetValue) =>
      val props = EventsByTagPublisher.props(conf, redis, tag, offsetValue, conf.getDuration("refresh-interval", MILLISECONDS).milliseconds)
      Source.actorPublisher[EventEnvelope2](props)
        .mapMaterializedValue(_ => NotUsed)
    case NoOffset =>
      val props = EventsByTagPublisher.props(conf, redis, tag, 0, conf.getDuration("refresh-interval", MILLISECONDS).milliseconds)
      Source.actorPublisher[EventEnvelope2](props)
        .mapMaterializedValue(_ => NotUsed)
    case _ =>
      throw new IllegalArgumentException("Redis does not support " + offset.getClass.getName + " offsets")
  }

}
