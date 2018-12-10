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

package feast;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.Builder;
import org.apache.beam.sdk.transforms.SerializableFunction;

@Builder
public class SerializableCache<KEY, VALUE> implements Serializable {
  private transient LoadingCache<KEY, VALUE> cache;
  private SerializableFunction<KEY, VALUE> loadingFunction;
  private Integer maximumSize;
  private Duration expireAfterAccess;

  public VALUE get(KEY key) {
    try {
      return getCache().get(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private LoadingCache<KEY, VALUE> getCache() {
    if (cache == null) {
      CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
      if (maximumSize != null) {
        builder = builder.maximumSize(maximumSize);
      }
      if (expireAfterAccess != null) {
        builder = builder.expireAfterAccess(expireAfterAccess);
      }
      cache =
          builder.build(
              new CacheLoader<KEY, VALUE>() {
                @Override
                public VALUE load(KEY key) throws Exception {
                  return loadingFunction.apply(key);
                }
              });
    }
    return cache;
  }
}
