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
package redis

object RedisKeys {
  val identifiersKey = "journal:persistenceIds"
  def highestSequenceNrKey(persistenceId: String) = f"journal:persisted:$persistenceId:highestSequenceNr"
  def journalKey(persistenceId: String) = f"journal:persisted:$persistenceId"
  def journalChannel(persistenceId: String) = f"journal:channel:persisted:$persistenceId"
  def tagKey(tag: String) = f"journal:tag:$tag"
  val tagsChannel = "journal:channel:tags"
  val identifiersChannel = "journal:channel:ids"
  def snapshotKey(persistenceId: String) = f"snapshot:$persistenceId"
}
