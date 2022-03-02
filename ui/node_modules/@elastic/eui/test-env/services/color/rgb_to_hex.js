"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.rgbToHex = rgbToHex;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function asHex(value) {
  var hex = parseInt(value, 10).toString(16);
  return hex.length === 1 ? "0".concat(hex) : hex;
}

function rgbToHex(rgb) {
  var withoutWhitespace = rgb.replace(/\s+/g, '');
  var rgbMatch = withoutWhitespace.match(/^rgba?\((\d+),(\d+),(\d+)(?:,(?:1(?:\.0*)?|0(?:\.\d+)?))?\)$/i);

  if (!rgbMatch) {
    return '';
  }

  var _rgbMatch = (0, _slicedToArray2.default)(rgbMatch, 4),
      r = _rgbMatch[1],
      g = _rgbMatch[2],
      b = _rgbMatch[3];

  return "#".concat(asHex(r)).concat(asHex(g)).concat(asHex(b));
}