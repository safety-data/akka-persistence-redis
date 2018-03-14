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

import actor._

import _root_.redis._

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object RedisUtils {

  def host(conf: Config) = conf.getString("redis.host")

  def port(conf: Config) = conf.getInt("redis.port")

  def database(conf: Config) = if (conf.hasPath("redis.database")) Some(conf.getInt("redis.database")) else None

  def password(conf: Config) = if (conf.hasPath("redis.password")) Some(conf.getString("redis.password")) else None

  def create(conf: Config)(implicit system: ActorSystem): RedisClient =
    conf.getString("redis.mode") match {
      case "simple" =>
        new RedisClient(
          host = host(conf),
          port = port(conf),
          db = database(conf),
          password = password(conf))

      case "sentinel" =>
        val sentinels =
          conf
            .getConfigList("redis.sentinels")
            .asScala
            .map(c => (c.getString("host"), c.getInt("port")))
            .toSeq
        new SentinelMonitoredRedisClient(sentinels = sentinels, master = conf.getString("redis.master"), db = database(conf), password = password(conf)).redisClient

      case mode =>
        throw new Exception(f"Unsupported redis mode $mode")
    }

}
