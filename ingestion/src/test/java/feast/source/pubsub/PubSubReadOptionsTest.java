/*
 * Copyright 2019 The Feast Authors
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

package feast.source.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import feast.options.OptionsParser;
import feast.source.pubsub.PubSubFeatureSource.MessageFormat;
import feast.source.pubsub.PubSubFeatureSource.PubSubReadOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class PubSubReadOptionsTest {

  @Test
  public void testParseSubscription() {
    Map<String, String> opts = new HashMap<>();
    opts.put("subscription", "foo");

    PubSubReadOptions options = OptionsParser.parse(opts, PubSubReadOptions.class);
    assertFalse(options.discardUnknownFeatures);
    assertEquals(options.format, MessageFormat.FEATURE_ROW_PROTO); // default format
  }

  @Test
  public void testParseSubscriptionWithFormat() {
    Map<String, String> opts = new HashMap<>();
    opts.put("subscription", "foo");
    opts.put("format", "csv");

    PubSubReadOptions options = OptionsParser.parse(opts, PubSubReadOptions.class);
    assertFalse(options.discardUnknownFeatures);
    assertEquals(options.format, MessageFormat.CSV);
  }
}
