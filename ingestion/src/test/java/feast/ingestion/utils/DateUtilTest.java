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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.protobuf.Timestamp;
import junit.framework.TestCase;
import org.joda.time.DateTime;

public class DateUtilTest extends TestCase {
  public void testStringToTimestamp() {
    Timestamp timestamp1 = DateUtil.toTimestamp("2018-07-03T15:09:34.123888999Z");
    Timestamp timestamp2 = DateUtil.toTimestamp("2018-07-03T15:09:34.123000000Z");
    Timestamp timestamp3 = DateUtil.toTimestamp("2018-07-03T15:09:34.124000000Z");
    // we are okay with only millisecond granularity

    assertThat(timestamp1, is(equalTo(timestamp2)));
    assertThat(timestamp1, is(not(equalTo(timestamp3))));
  }

  public void testBigqueryTimestampStringToTimestamp() {
    Timestamp timestamp = DateUtil.toTimestamp("2018-10-23 00:00:00 UTC");
    Timestamp timestamp2 = DateUtil.toTimestamp("2018-10-23T00:00:00.000Z");

    assertThat(timestamp, equalTo(timestamp2));
  }

  public void testBigqueryTimestampWithFractionSecondStringToTimestamp() {
    Timestamp timestamp = DateUtil.toTimestamp("2018-10-23 00:00:00.123456 UTC");
    Timestamp timestamp2 = DateUtil.toTimestamp("2018-10-23T00:00:00.123456Z");

    assertThat(timestamp, equalTo(timestamp2));
  }

  public void testTimestampToDateTime() {
    Timestamp timestamp1 = DateUtil.toTimestamp("2018-07-03T15:09:34.123888999Z");
    DateTime datetime = DateUtil.toDateTime(timestamp1);
    assertThat(2018, is(equalTo(datetime.getYear())));
    assertThat(7, is(equalTo(datetime.getMonthOfYear())));
    assertThat(3, is(equalTo(datetime.getDayOfMonth())));
    assertThat(15, is(equalTo(datetime.getHourOfDay())));
    assertThat(9, is(equalTo(datetime.getMinuteOfHour())));
    assertThat(34, is(equalTo(datetime.getSecondOfMinute())));
    assertThat(123, is(equalTo(datetime.getMillisOfSecond())));
  }
}
