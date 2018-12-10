/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.util;

import com.google.protobuf.Timestamp;
import feast.types.GranularityProto.Granularity.Enum;
import org.joda.time.*;

/** Utility class for time-related function. */
public class TimeUtil {
  public static final int NANO_IN_MILLI = 1000000;
  public static final int NANO_IN_MICRO = 1000;
  public static final int MILLI_IN_SECOND = 1000;

  private TimeUtil() {}

  /**
   * Round down timestamp to the nearest granularity.
   *
   * @param timestamp original timestamp.
   * @param granularity granularity of the rounded timestamp.
   * @return
   */
  public static Timestamp roundFloorTimestamp(Timestamp timestamp, Enum granularity) {
    MutableDateTime dt = new MutableDateTime(DateTimeZone.UTC);
    DateTimeField roundingField;
    switch (granularity) {
      case DAY:
        roundingField = dt.getChronology().dayOfMonth();
        break;
      case HOUR:
        roundingField = dt.getChronology().hourOfDay();
        break;
      case MINUTE:
        roundingField = dt.getChronology().minuteOfHour();
        break;
      case SECOND:
        roundingField = dt.getChronology().secondOfMinute();
        break;
      case NONE:
        return Timestamp.newBuilder().setSeconds(0).setNanos(0).build();
      default:
        throw new RuntimeException("Unrecognised time series granularity");
    }
    dt.setMillis(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    dt.setRounding(roundingField, MutableDateTime.ROUND_FLOOR);
    return dateTimeToTimestamp(dt.toDateTime());
  }

  /**
   * Returns the current value of the running Java Virtual Machine's
   * high-resolution time source, in microseconds.
   * @return current micro time.
   * @see System#nanoTime()
   */
  public static long microTime() {
    return System.nanoTime() / NANO_IN_MICRO;
  }

  /**
   * Convert {@link DateTime} into {@link Timestamp}
   *
   * @param dateTime
   * @return
   */
  private static Timestamp dateTimeToTimestamp(DateTime dateTime) {
    return Timestamp.newBuilder()
        .setSeconds(dateTime.getMillis() / MILLI_IN_SECOND)
        .setNanos((int) (dateTime.getMillis() % MILLI_IN_SECOND) * NANO_IN_MILLI)
        .build();
  }
}
