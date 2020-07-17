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
package feast.core.controller.exception.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.UnsatisfiedServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

  @ExceptionHandler({ InvalidProtocolBufferException.class })
  protected ResponseEntity<Object> handleInvalidProtocolBuffer(
      InvalidProtocolBufferException ex, WebRequest request) {
    Map<String, String> bodyOfResponse = Map.of("error",
        "Unexpected error occurred in Feast Core.");
    return handleExceptionInternal(
        ex, bodyOfResponse, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

  @ExceptionHandler({ UnsatisfiedServletRequestParameterException.class })
  protected ResponseEntity<Object> handleUnsatisfiedServletRequestParameter(
      UnsatisfiedServletRequestParameterException ex, WebRequest request) {
    Map<String, String> bodyOfResponse = Map.of("error", ex.getMessage());
    return handleExceptionInternal(
        ex, bodyOfResponse, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

  @Override
  protected ResponseEntity<Object> handleMissingServletRequestParameter(
      MissingServletRequestParameterException ex, HttpHeaders headers, HttpStatus status,
      WebRequest request) {
    Map<String,String> bodyOfResponse = Map.of("error", ex.getMessage());
    return this.handleExceptionInternal(ex, bodyOfResponse, headers, status, request);
  }

}
