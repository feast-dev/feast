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
package feast.auth.config;

import feast.common.validators.OneOfStrings;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SecurityProperties {
  private AuthenticationProperties authentication;
  private AuthorizationProperties authorization;

  @Getter
  @Setter
  public static class AuthenticationProperties {

    // Enable authentication
    private boolean enabled;

    // Named authentication provider to use
    @OneOfStrings({"jwt"})
    private String provider;

    // K/V options to initialize the provider with
    private Map<String, String> options;
  }

  @Getter
  @Setter
  public static class AuthorizationProperties {

    // Enable authorization. Authentication must be enabled if authorization is enabled.
    private boolean enabled;

    // Named authorization provider to use.
    @OneOfStrings({"none", "keto"})
    private String provider;

    // K/V options to initialize the provider with
    private Map<String, String> options;
  }
}
