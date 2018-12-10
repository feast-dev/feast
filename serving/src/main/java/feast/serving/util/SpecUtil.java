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

import org.joda.time.Duration;
import feast.serving.service.RedisFeatureStorage;
import feast.specs.FeatureSpecProto.FeatureSpec;
import org.joda.time.format.ISOPeriodFormat;

/** Utility class for Spec storage. */
public final class SpecUtil {

  private SpecUtil() {}

  /**
   * Return bucket's duration/size of a certain feature
   *
   * @param featureSpec feature spec.
   * @return bucket size.
   * @throws IllegalArgumentException if the bucket size is not specified in the {@code featureSpec}
   *     or unable to parse the value.
   */
  public static Duration getBucketSize(FeatureSpec featureSpec) {
    try {
      String bucketSize =
          featureSpec
              .getDataStores()
              .getServing()
              .getOptionsMap()
              .get(RedisFeatureStorage.OPT_REDIS_BUCKET_SIZE);
      if (bucketSize == null) {
        return Duration.standardHours(1); // use duration 1H if bucket time is not specified (align with ingestion)
      }
      return ISOPeriodFormat.standard().parsePeriod(bucketSize).toStandardDuration();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to get bucket size of feature spec: " + featureSpec.getId(), e);
    }
  }
}
