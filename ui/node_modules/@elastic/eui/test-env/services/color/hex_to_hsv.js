"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.hexToHsv = hexToHsv;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _hex_to_rgb = require("./hex_to_rgb");

var _rgb_to_hsv = require("./rgb_to_hsv");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function hexToHsv(hex) {
  var _hexToRgb = (0, _hex_to_rgb.hexToRgb)(hex),
      _hexToRgb2 = (0, _slicedToArray2.default)(_hexToRgb, 3),
      r = _hexToRgb2[0],
      g = _hexToRgb2[1],
      b = _hexToRgb2[2];

  return (0, _rgb_to_hsv.rgbToHsv)({
    r: r,
    g: g,
    b: b
  });
}