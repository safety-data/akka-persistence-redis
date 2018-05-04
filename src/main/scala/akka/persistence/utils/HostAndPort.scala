/*
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
package akka.persistence.utils

/** Convenience class for holding host and port tuples and not having to depend on Guava.
 *  @param host the hostname
 *  @param portOpt the (optional) port
 */
case class HostAndPort private (host: String, portOpt: Option[Int]) {

  // construction assertion
  portOpt match {
    case Some(p) => require(isValidPort(p))
    case _       =>
  }

  /** Get the port value or if not set the defaultPort value
   *  @param defaultPort the default to return if port is not set
   *  @return the port or the default
   */
  def portOrDefault(defaultPort: Int): Int = portOpt match {
    case Some(p) => p
    case None    => defaultPort
  }

  /** Get the port or a negative fallback value
   *  @return the port or a negative fallback
   */
  def port: Int = portOrDefault(HostAndPort.noPortFallback)

  /** Returns the host and port as a tuple (String, Int)
   *  @return the tuple
   */
  def asTuple: (String, Int) = (host, port)

  private def isValidPort(port: Int) = port >= 0 && port <= 65535

  override def toString: String =
    portOpt match {
      case Some(p) => s"$host:$p"
      case None    => host
    }
}

/** Companion Object to create HostAndPort instances either from string or from host, port
 */
object HostAndPort {

  val noPortFallback: Int = -1

  /** Create hostAndPort from a String like <host>:<port> or <host>
   *
   *  @param hostPortString like <host>:<port> or <host>
   *  @return the HostAndPort
   */
  def apply(hostPortString: String): HostAndPort = {
    require(hostPortString != null)
    require(hostPortString.nonEmpty)
    require(validString(hostPortString))

    val parts = hostPortString.split(":")
    parts match {
      case Array(host, port) =>
        new HostAndPort(host, Some(Integer.parseInt(port)))
      case Array(host) =>
        new HostAndPort(host, None)
    }
  }

  private def validString(hostPortString: String) =
    hostPortString.split(":").length <= 2

}
