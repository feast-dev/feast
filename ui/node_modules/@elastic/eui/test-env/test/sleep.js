"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.sleep = sleep;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// async timeout function for awaiting state or DOM updates
function sleep() {
  var ms = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 50;
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}