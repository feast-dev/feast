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
import feast.core.exception.RetrievalException;
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

/** A exception handler for some common exceptions while accessing Feast Core via HTTP. */
@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

  /**
   * Handles the case when a request object (such as {@link
   * feast.proto.core.CoreServiceProto.GetFeatureSetRequest}) or a response object (such as {@link
   * feast.proto.core.CoreServiceProto.GetFeatureSetResponse} is malformed.
   *
   * @param ex the {@link InvalidProtocolBufferException} that occurred.
   * @param request the {@link WebRequest} that caused this exception.
   * @return (500 Internal Server Error)
   */
  @ExceptionHandler({InvalidProtocolBufferException.class})
  protected ResponseEntity<Object> handleInvalidProtocolBuffer(
      InvalidProtocolBufferException ex, WebRequest request) {
    Map<String, String> bodyOfResponse =
        Map.of("error", "An unexpected error occurred in Feast Core.");
    return handleExceptionInternal(
        ex, bodyOfResponse, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

  /**
   * Handles the case that retrieval of information from the services triggered and exception.
   * Instead of returning 500 with no error message, returns 500 with a body describing the error
   * message.
   *
   * @param ex the {@link RetrievalException} that occurred.
   * @param request the {@link WebRequest} that caused this exception.
   * @return (500 Internal Server Error)
   */
  @ExceptionHandler({RetrievalException.class})
  protected ResponseEntity<Object> handleRetrieval(RetrievalException ex, WebRequest request) {
    Map<String, String> bodyOfResponse = Map.of("error", ex.getMessage());
    return handleExceptionInternal(
        ex, bodyOfResponse, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

  /**
   * Handles various exceptions that are due to malformed or invalid requests, such as
   *
   * <ul>
   *   <li>{@link UnsatisfiedServletRequestParameterException} where a parameter is requested in
   *       {@link org.springframework.web.bind.annotation.RequestMapping} but not supplied.
   *   <li>{@link IllegalArgumentException} where unsupported parameters are provided.
   * </ul>
   *
   * @param ex the {@link UnsatisfiedServletRequestParameterException} that occurred.
   * @param request the {@link WebRequest} that caused this exception.
   * @return (400 Bad Request)
   */
  @ExceptionHandler({
    UnsatisfiedServletRequestParameterException.class,
    IllegalArgumentException.class
  })
  protected ResponseEntity<Object> handleBadRequest(Exception ex, WebRequest request) {
    ex.printStackTrace();
    Map<String, String> bodyOfResponse = Map.of("error", ex.getMessage());
    return handleExceptionInternal(
        ex, bodyOfResponse, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
  }

  /**
   * Handles {@link MissingServletRequestParameterException} which occurs when a controller method
   * expects a certain parameter but is not supplied by the request. The original implementation
   * returns an empty body with (400 Bad Request), we add in the error and stacktrace.
   *
   * @param ex the {@link MissingServletRequestParameterException} that occurred.
   * @param request the {@link WebRequest} that caused this exception.
   * @param headers the {@link HttpHeaders} from the request.
   * @param status the {@link HttpStatus} generated for the response, (400 Bad Request)
   * @return (400 Bad Request)
   */
  @Override
  protected ResponseEntity<Object> handleMissingServletRequestParameter(
      MissingServletRequestParameterException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    ex.printStackTrace();
    Map<String, String> bodyOfResponse = Map.of("error", ex.getMessage());
    return this.handleExceptionInternal(ex, bodyOfResponse, headers, status, request);
  }
}
