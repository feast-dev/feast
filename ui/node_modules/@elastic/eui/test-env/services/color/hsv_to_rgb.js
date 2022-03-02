"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.hsvToRgb = hsvToRgb;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function hsvToRgb(_ref) {
  var h = _ref.h,
      s = _ref.s,
      v = _ref.v;
  h /= 60;

  var fn = function fn(n) {
    var k = (n + h) % 6;
    return v - v * s * Math.max(Math.min(k, 4 - k, 1), 0);
  };

  var r = fn(5);
  var g = fn(3);
  var b = fn(1);
  return {
    r: Math.round(r * 255),
    g: Math.round(g * 255),
    b: Math.round(b * 255)
  };
}