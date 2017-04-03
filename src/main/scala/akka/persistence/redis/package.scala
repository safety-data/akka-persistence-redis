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
package akka.persistence

import akka.util._

import akka.serialization.Serialization

import _root_.redis._

import java.nio.ByteOrder

package object redis {

  def persistentToBytes(p: PersistentRepr)(implicit serialization: Serialization): Array[Byte] = serialization.serialize(p).get

  def persistentFromBytes(a: Array[Byte])(implicit serialization: Serialization): PersistentRepr = serialization.deserialize(a, classOf[PersistentRepr]).get

  implicit object SnapshotEntrySerializer extends ByteStringFormatter[SnapshotEntry] {

    def deserialize(bs: ByteString): SnapshotEntry = {
      val iterator = bs.iterator
      val sequenceNr = iterator.getLong(ByteOrder.BIG_ENDIAN)
      val timestamp = iterator.getLong(ByteOrder.BIG_ENDIAN)
      val snapshot = iterator.toByteString
      SnapshotEntry(sequenceNr, timestamp, snapshot)
    }

    def serialize(data: SnapshotEntry): ByteString = data match {
      case SnapshotEntry(sequenceNr, timestamp, snapshot) =>
        val builder = new ByteStringBuilder
        builder.putLong(sequenceNr)(ByteOrder.BIG_ENDIAN)
        builder.putLong(timestamp)(ByteOrder.BIG_ENDIAN)
        builder.append(snapshot)
        builder.result
    }

  }

}
