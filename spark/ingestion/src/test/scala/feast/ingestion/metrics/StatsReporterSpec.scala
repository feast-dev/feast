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

import java.util
import java.util.Collections

import com.codahale.metrics.{Gauge, Histogram, MetricRegistry, UniformReservoir}
import feast.ingestion.UnitSpec

import scala.jdk.CollectionConverters._

class StatsReporterSpec extends UnitSpec {
  trait Scope {
    val server = new StatsDStub
    val reporter = new StatsdReporterWithTags(
      new MetricRegistry,
      "127.0.0.1",
      server.port
    )

    def gauge[A](v: A): Gauge[A] = new Gauge[A] {
      override def getValue: A = v
    }

    def histogram(values: Seq[Int]): Histogram = {
      val hist = new Histogram(new UniformReservoir)
      values.foreach(hist.update)
      hist
    }
  }

  "Statsd reporter" should "send simple gauge unmodified" in new Scope {
    reporter.report(
      gauges = new util.TreeMap(
        Map(
          "test" -> gauge(0)
        ).asJava
      ),
      counters = Collections.emptySortedMap(),
      histograms = Collections.emptySortedMap(),
      meters = Collections.emptySortedMap(),
      timers = Collections.emptySortedMap()
    )

    server.receive should contain("test:0|g")
  }

  "Statsd reporter" should "keep tags part in the message's end" in new Scope {
    reporter.report(
      gauges = Collections.emptySortedMap(),
      counters = Collections.emptySortedMap(),
      histograms = new util.TreeMap(
        Map(
          "prefix.1111.test#fs=name,job=aaa" -> histogram((1 to 100))
        ).asJava
      ),
      meters = Collections.emptySortedMap(),
      timers = Collections.emptySortedMap()
    )

    server.receive should contain("prefix.test.p95:95.95|ms|#fs:name,job:aaa")
  }
}
