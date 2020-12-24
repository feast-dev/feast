/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.metrics

import java.net.{DatagramPacket, DatagramSocket, SocketTimeoutException}

import scala.collection.mutable.ArrayBuffer

class StatsDStub {
  val socket = new DatagramSocket()
  socket.setSoTimeout(100)

  def port: Int = socket.getLocalPort

  def receive: Array[String] = {
    val messages: ArrayBuffer[String] = ArrayBuffer()
    var finished                      = false

    do {
      val buf = new Array[Byte](65535)
      val p   = new DatagramPacket(buf, buf.length)
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

  private val metricLine = """(.+):(.+)\|(.+)#(.+)""".r

  def receivedMetrics: Map[String, Float] = {
    receive
      .flatMap {
        case metricLine(name, value, type_, tags) =>
          Seq(name -> value.toFloat)
        case s: String =>
          Seq()
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
  }
}
