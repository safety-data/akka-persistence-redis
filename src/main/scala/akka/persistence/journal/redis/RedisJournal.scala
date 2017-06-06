/*
 * Copyright © 2017 Safety Data - CFH SAS.
 * Based on code by
 * Copyright 2014 HootSuite Media Inc.
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
package akka.persistence
package journal
package redis

import akka.persistence.redis._

import akka.actor._
import akka.util.ByteString

import akka.serialization.SerializationExtension

import _root_.redis._
import commands._
import api._

import com.typesafe.config.Config

import scala.concurrent.Future

import scala.util.{
  Try,
  Success,
  Failure
}

import scala.collection.immutable.Seq

/** Stores events inside a redis database.
 *
 *  For each persistence id `persistenceId`, it creates two keys:
 *   - `journal:persisted:persistenceId` contains a sorted set of events (sorted by sequence number)
 *   - `journal:persisted:persistenceId:highestSequenceNr` contains the highest sequence number
 *
 *  For each tag `t`, it creates a key `journal:tag:t` which contains the ordered list of pair (sequenceNr, persistenceId).
 *  Order in this list is the order in which events with the given tag `t` were recorded.
 *
 *  It also maintains following keys:
 *   - `journal:persistenceIds` which is a set of persistence identifiers.
 *   - `journal:tags` which is the set of all tags.
 *
 *  Using the redis PubSub mechanism, this journal notifies whoever is interested on following channels:
 *   - `journal:channel:ids` any new persisted identifier is published
 *   - `journal:channel:persisted:<id>` anytime a new event is appended to a persistence id `<id>`, the sequenceNr is published
 *   - `journal:channel:tags` anytime a new event is appended to a tag, the tag name is published
 */
class RedisJournal(conf: Config) extends AsyncWriteJournal {

  import RedisKeys._

  implicit def system = context.system

  implicit object longFormatter extends ByteStringDeserializer[Long] {
    def deserialize(bs: ByteString): Long =
      bs.utf8String.toLong
  }

  implicit def ec = context.system.dispatcher

  implicit val serialization = SerializationExtension(context.system)

  var redis: RedisClient = _

  override def preStart() {
    redis = RedisUtils.create(conf)
    super.preStart()
  }

  override def postStop() {
    redis.stop()
    super.postStop()
  }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    for (highestStored <- redis.get[Long](highestSequenceNrKey(persistenceId)))
      yield highestStored.getOrElse(fromSequenceNr)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] =
    for {
      entries <- redis.zrangebyscore[Array[Byte]](journalKey(persistenceId), Limit(fromSequenceNr), Limit(toSequenceNr), Some(0L -> max))
    } yield for {
      entry <- entries
    } recoveryCallback(persistentFromBytes(entry))

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    for {
      _ <- redis.zremrangebyscore(journalKey(persistenceId), Limit(-1), Limit(toSequenceNr))
    } yield ()

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    Future.sequence(messages.map(asyncWriteBatch))

  private def asyncWriteBatch(a: AtomicWrite): Future[Try[Unit]] = {
    val transaction = redis.transaction()

    val batchOperations = Future
      .sequence(a.payload.map(asyncWriteOperation(transaction, _)))
      .zip(transaction.set(highestSequenceNrKey(a.persistenceId), a.highestSequenceNr))
      .zip(transaction.sadd(identifiersKey, a.persistenceId))
      .flatMap {
        case ((_, _), n) =>
          // notify about new persistence identifier if needed
          if (n > 0)
            redis.publish(identifiersChannel, a.persistenceId).map(_ => ())
          else
            Future.successful(())
      }

    transaction.exec()

    batchOperations
      .map(Success(_))
      .recover {
        case ex => Failure(ex)
      }
  }

  private def asyncWriteOperation(transaction: TransactionBuilder, pr: PersistentRepr): Future[Unit] =
    Try(extract(pr)) match {
      case Success((entry, tags)) =>
        transaction
          .zadd(journalKey(pr.persistenceId), (pr.sequenceNr, entry))
          // notify about new event being appended for this persistence id
          .zip(transaction.publish(journalChannel(pr.persistenceId), pr.sequenceNr))
          .zip(Future.sequence(tags.map { t =>
            transaction
              .rpush(tagKey(t), f"${pr.sequenceNr}:${pr.persistenceId}")
              // notify about new event being appended for this tag
              .zip(transaction.publish(tagsChannel, t))
          }))
          .map(_ => ())
      case Failure(t) => Future.failed(t)
    }

  private def extract(pr: PersistentRepr): (Array[Byte], Set[String]) = pr.payload match {
    case Tagged(event, tags) =>
      persistentToBytes(pr.withPayload(event)) -> tags
    case event =>
      persistentToBytes(pr) -> Set.empty[String]
  }

}
