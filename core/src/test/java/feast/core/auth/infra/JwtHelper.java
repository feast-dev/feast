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
package feast.core.auth.infra;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import feast.common.auth.credentials.JwtCallCredentials;
import io.grpc.*;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;

public final class JwtHelper {

  private static RSAKey key = null;
  private JWKSet keySet;

  public JwtHelper() {
    try {
      key = new RSAKeyGenerator(2048).keyID("123").generate();
    } catch (JOSEException e) {
      throw new RuntimeException("Could not generate RSA key");
    }
    RSAKey.Builder builder = null;
    try {
      builder =
          new RSAKey.Builder((RSAPublicKey) this.getKey().toKeyPair().getPublic())
              .keyUse(KeyUse.SIGNATURE)
              .algorithm(JWSAlgorithm.RS256)
              .keyID(this.getKey().getKeyID());
    } catch (JOSEException e) {
      throw new RuntimeException("Could not create RSAKey builder");
    }
    keySet = new JWKSet(builder.build());
  }

  public CallCredentials getCallCredentials(String email) throws JOSEException {
    String jwt = createToken(email);
    return new JwtCallCredentials(jwt);
  }

  public String createToken(String email) throws JOSEException {
    assert key != null;
    JWSHeader header =
        new JWSHeader.Builder(JWSAlgorithm.RS256)
            .type(JOSEObjectType.JWT)
            .keyID(key.getKeyID())
            .build();

    JWTClaimsSet payload =
        new JWTClaimsSet.Builder()
            .issuer("me")
            .audience("you")
            .subject(email)
            .expirationTime(Date.from(Instant.now().plusSeconds(120)))
            .build();

    SignedJWT signedJWT = new SignedJWT(header, payload);
    signedJWT.sign(new RSASSASigner(key.toRSAPrivateKey()));
    return signedJWT.serialize();
  }

  public RSAKey getKey() {
    return key;
  }

  public JWKSet getKeySet() {
    return this.keySet;
  }
}
