package feast.ingestion.metrics

import java.net.{DatagramPacket, DatagramSocket, SocketTimeoutException}

import scala.collection.mutable.ArrayBuffer

class StatsDStub {
  val socket = new DatagramSocket()
  socket.setSoTimeout(100)

  def port: Int = socket.getLocalPort

  def receive: Array[String] = {
    val messages: ArrayBuffer[String] = ArrayBuffer()
    var finished = false

    do {
      val buf = new Array[Byte](65535)
      val p = new DatagramPacket(buf, buf.length)
      try {
        socket.receive(p)
      } catch {
        case _: SocketTimeoutException =>
          finished = true
      }
      messages += new String(p.getData, 0, p.getLength)
    } while (!finished)

    messages.toArray
  }
}
