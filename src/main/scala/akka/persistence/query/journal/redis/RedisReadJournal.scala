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
package query
package journal
package redis

import akka.actor._
import scaladsl.{ ReadJournal => SReadJournal }
import javadsl.{ ReadJournal => JReadJournal }

import com.typesafe.config.Config

class RedisReadJournalProvider(system: ExtendedActorSystem, config: Config)
    extends ReadJournalProvider {

  def javadslReadJournal(): JReadJournal =
    new JavaReadJournal(new ScalaReadJournal(system, config))

  def scaladslReadJournal(): SReadJournal =
    new ScalaReadJournal(system, config)

}
