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
package snapshot
package redis

import util.ByteString

import akka.serialization.SerializationExtension
import akka.persistence.serialization._

import akka.persistence.redis._

import _root_.redis._
import api._

import com.typesafe.config.Config

import scala.concurrent.Future

/** Stores snapshots inside a redis database.
 *
 *  For each persistence identifier `persistenceId`, it creates one key, namely:
 *  `snapshot:persistenceId` that contains the ordered set of snapshot data. The data order
 *  is the one of `sequenceId`.
 */
class RedisSnapshotStore(conf: Config) extends SnapshotStore {

  import RedisKeys._

  implicit def system = context.system

  implicit def ec = context.system.dispatcher

  val serialization = SerializationExtension(context.system)

  def snapshotToBytes(s: Snapshot): Array[Byte] = serialization.findSerializerFor(s).toBinary(s)

  def snapshotFromBytes(a: Array[Byte]): Snapshot = serialization.deserialize(a, classOf[Snapshot]).get

  var redis: RedisClient = _

  override def preStart(): Unit = {
    redis = RedisUtils.create(conf)
    super.preStart()
  }

  override def postStop(): Unit = try {
    redis.stop()
  } finally {
    super.postStop()
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    // in this case we first load the snapshots matching the sequence number boundaries
    // and among them, we next remove the one matching the timestamp boundaries
    redis
      .zrevrangebyscore[SnapshotEntry](snapshotKey(persistenceId), Limit(criteria.minSequenceNr), Limit(criteria.maxSequenceNr))
      .flatMap { seq =>
        val toremove = seq
          .map {
            case SnapshotEntry(sequenceNr, timestamp, snapshot) =>
              SnapshotMetadata(persistenceId, sequenceNr, timestamp)
          }
          .filter(criteria.matches(_))
          .map {
            case SnapshotMetadata(_, sequenceNr, _) =>
              redis.zremrangebyscore(snapshotKey(persistenceId), Limit(sequenceNr), Limit(sequenceNr))
          }
        Future.sequence(toremove)
      }
      .map(_ => ())

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    redis
      .zremrangebyscore(snapshotKey(metadata.persistenceId), Limit(metadata.sequenceNr), Limit(metadata.sequenceNr))
      .map(_ => ())

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    if (criteria == SnapshotSelectionCriteria.None) {
      Future.successful(None)
    } else if (criteria == SnapshotSelectionCriteria.Latest) {
      // it is simply the one with the highest sequence number
      redis
        .zrevrangebyscore[SnapshotEntry](snapshotKey(persistenceId), Limit(0), Limit(Long.MaxValue), Some(0L -> 1L))
        .map {
          case Seq(SnapshotEntry(sequenceNr, timestamp, (snapshot)), _*) =>
            Some(SelectedSnapshot(SnapshotMetadata(persistenceId, sequenceNr, timestamp), snapshotFromBytes(snapshot.toArray).data))
          case _ =>
            None
        }
    } else {
      // otherwise retrieve all the snapshots in the given sequence number range and then filter by timestamp
      redis
        .zrevrangebyscore[SnapshotEntry](snapshotKey(persistenceId), Limit(criteria.minSequenceNr), Limit(criteria.maxSequenceNr))
        .map {
          case Seq() =>
            None
          case seq =>
            // the sequence is ordered by higher sequence number first, so take the first one for which the criteria match
            // i.e. the highest sequence number whose timestamp is in the request range.
            seq
              .map {
                case SnapshotEntry(sequenceNr, timestamp, snapshot) =>
                  SelectedSnapshot(SnapshotMetadata(persistenceId, sequenceNr, timestamp), snapshotFromBytes(snapshot.toArray).data)
              }
              .find(sel => criteria.matches(sel.metadata))
        }
    }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val data = ByteString(snapshotToBytes(Snapshot(snapshot)))
    redis
      .zadd(snapshotKey(metadata.persistenceId), (metadata.sequenceNr.toDouble, SnapshotEntry(metadata.sequenceNr, metadata.timestamp, data)))
      .map(_ => ())
  }

}
