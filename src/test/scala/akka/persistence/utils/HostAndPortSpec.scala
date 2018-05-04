package akka.persistence.utils

import org.scalatest.{ FlatSpec, Matchers }

class HostAndPortSpec extends FlatSpec with Matchers {

  "A HostAndPort" should "have localhost as host and 8080 as port" in {
    val hostAndPort = HostAndPort("localhost:8080")
    assert(hostAndPort.host == "localhost")
    assert(hostAndPort.port == 8080)
  }

  it should "return a tuple (String, Int)" in {
    val hostAndPort = HostAndPort("my.host.name:10001")
    assert(hostAndPort.asTuple == ("my.host.name", 10001))
  }

  it should "return 8080 instead of 9090" in {
    val hostAndPort = HostAndPort("localhost:8080")
    assert(hostAndPort.portOrDefault(9090) == 8080)
  }

  it should "throw an IllegalArgumentException if the port is negative" in {
    a[IllegalArgumentException] should be thrownBy {
      HostAndPort("localhost:-1")
    }

  }

  it should "throw an IllegalArgumentException if the port is too high" in {
    a[IllegalArgumentException] should be thrownBy {
      HostAndPort("localhost:65536")
    }
  }

}
