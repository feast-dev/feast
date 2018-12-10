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

package feast.serving.http;

import feast.serving.exception.FeatureRetrievalException;
import feast.serving.exception.SpecRetrievalException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.NoSuchElementException;

/**
 * Exception Handler to translate exception thrown by the application into appropriate error
 * response.
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
public class HttpExceptionHandler extends ResponseEntityExceptionHandler {
  @ExceptionHandler(NoSuchElementException.class)
  protected ResponseEntity<Object> handleNoSuchElementException(NoSuchElementException ex) {
    Error error = new Error(HttpStatus.NOT_FOUND, ex.getMessage());
    return buildResponseEntity(error);
  }

  @ExceptionHandler(SpecRetrievalException.class)
  protected ResponseEntity<Object> handleSpecRetrievalException(SpecRetrievalException ex) {
    Error error = new Error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
    return buildResponseEntity(error);
  }

  @ExceptionHandler(FeatureRetrievalException.class)
  protected ResponseEntity<Object> handleFeatureRetrievalException(FeatureRetrievalException ex) {
    Error error = new Error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
    return buildResponseEntity(error);
  }

  @ExceptionHandler(IllegalStateException.class)
  protected ResponseEntity<Object> handleIllegalStateException(IllegalStateException ex) {
    Error error = new Error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
    return buildResponseEntity(error);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  protected ResponseEntity<Object> handleIllegalArgumentException(IllegalArgumentException ex) {
    Error error = new Error(HttpStatus.BAD_REQUEST, ex.getMessage());
    return buildResponseEntity(error);
  }

  @Override
  protected ResponseEntity<Object> handleExceptionInternal(
      Exception ex, Object body, HttpHeaders headers, HttpStatus status, WebRequest request) {
    Error error = new Error(status, ex.getMessage());
    return buildResponseEntity(error);
  }

  protected ResponseEntity<Object> buildResponseEntity(Error error) {
    return new ResponseEntity<Object>(error, error.getStatus());
  }
}
