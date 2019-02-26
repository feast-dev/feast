package feast.serving.testutil;

import com.google.protobuf.Timestamp;
import feast.types.GranularityProto.Granularity.Enum;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class TimeUtil {
  public static final int NANO_IN_MILLI = 1000000;
  public static final int NANO_IN_MICRO = 1000;
  public static final int MILLI_IN_SECOND = 1000;

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
