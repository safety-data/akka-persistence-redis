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

import stream.javadsl._

import javadsl._

class JavaReadJournal private[redis] (scalaReadJournal: ScalaReadJournal) extends ReadJournal
    with EventsByTagQuery2
    with EventsByPersistenceIdQuery
    with AllPersistenceIdsQuery
    with CurrentPersistenceIdsQuery {

  def allPersistenceIds(): Source[String, NotUsed] =
    scalaReadJournal.allPersistenceIds().asJava

  def currentPersistenceIds(): Source[String, NotUsed] =
    scalaReadJournal.currentPersistenceIds().asJava

  def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    scalaReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope2, NotUsed] =
    scalaReadJournal.eventsByTag(tag, offset).asJava

}
