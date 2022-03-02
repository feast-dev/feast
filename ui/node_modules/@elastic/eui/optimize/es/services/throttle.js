/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var throttle = function throttle(fn) {
  var wait = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 50;
  var time = Date.now();
  return function () {
    if (time + wait - Date.now() < 0) {
      fn.apply(void 0, arguments);
      time = Date.now();
    }
  };
};