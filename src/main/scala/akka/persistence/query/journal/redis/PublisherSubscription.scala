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

import actor.ActorRef

import akka.persistence.redis._

import _root_.redis._
import actors._
import api.pubsub._

import java.net.InetSocketAddress

import com.typesafe.config.Config

private class PublisherSubscription(conf: Config, publisher: ActorRef, channel: String) extends RedisSubscriberActor(new InetSocketAddress(RedisUtils.host(conf), RedisUtils.port(conf)), Seq(channel), Nil, RedisUtils.password(conf), _ => ()) {

  def onMessage(m: Message): Unit = publisher ! m
  def onPMessage(pm: PMessage): Unit = publisher ! pm

}
