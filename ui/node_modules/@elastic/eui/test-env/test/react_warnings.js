"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.stopThrowingReactWarnings = exports.startThrowingReactWarnings = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
  Use this utility to throw errors whenever React complains via the console
  about things like invalid propTypes. This lets us assert that a propType
  check works correctly with `toThrow`.

  Usage looks like:

  beforeAll(startThrowingReactWarnings);
  afterAll(stopThrowingReactWarnings);
*/
var consoleWarn = console.warn;
var consoleError = console.error;

var startThrowingReactWarnings = function startThrowingReactWarnings() {
  console.warn = console.error = function (msg) {
    throw msg;
  };
};

exports.startThrowingReactWarnings = startThrowingReactWarnings;

var stopThrowingReactWarnings = function stopThrowingReactWarnings() {
  console.warn = consoleWarn;
  console.error = consoleError;
};

exports.stopThrowingReactWarnings = stopThrowingReactWarnings;