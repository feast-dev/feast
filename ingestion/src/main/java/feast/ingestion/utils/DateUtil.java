/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion.utils;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

public class DateUtil {

  private static final DateTimeFormatter FALLBACK_TIMESTAMP_FORMAT;

  static {
    DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder();
    DateTimeFormatter base = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    DateTimeFormatter zone = DateTimeFormat.forPattern(" ZZZ");
    DateTimeParser fractionSecondParser =
        new DateTimeFormatterBuilder().appendLiteral(".").appendFractionOfSecond(1, 6).toParser();

    FALLBACK_TIMESTAMP_FORMAT =
        formatterBuilder
            .append(base)
            .appendOptional(fractionSecondParser)
            .append(zone)
            .toFormatter();
  }

  public static String toString(DateTime datetime) {
    return datetime.toString(ISODateTimeFormat.dateTime());
  }

  public static String toString(Timestamp timestamp) {
    return toString(toDateTime(timestamp));
  }

  public static DateTime toDateTime(Timestamp timestamp) {
    long millis = timestamp.getSeconds() * 1000 + (timestamp.getNanos() / 1000000);
    return new DateTime(millis, DateTimeZone.UTC);
  }

  public static DateTime toDateTime(String datetimeString) {
    try {
      return ISODateTimeFormat.dateTimeParser().parseDateTime(datetimeString);
    } catch (IllegalArgumentException e) {
      return DateTime.parse(datetimeString, FALLBACK_TIMESTAMP_FORMAT);
    }
  }

  public static Timestamp toTimestamp(DateTime datetime) {
    return Timestamp.newBuilder()
        .setSeconds(datetime.getMillis() / 1000)
        .setNanos(datetime.getMillisOfSecond() * 1000000)
        .build();
  }

  public static Timestamp toTimestamp(String datetimeString) {
    return toTimestamp(toDateTime(datetimeString));
  }

  public static java.sql.Timestamp toSqlTimestamp(Timestamp timestamp) {
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    return java.sql.Timestamp.from(instant);
  }

  public static Timestamp maxTimestamp(Timestamp a, Timestamp b) {
    if (a.getSeconds() != b.getSeconds()) {
      return a.getSeconds() < b.getSeconds() ? b : a;
    } else {
      return a.getNanos() < b.getNanos() ? b : a;
    }
  }

  public static long toMillis(Timestamp timestamp) {
    return toDateTime(timestamp).getMillis();
  }
}
