"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isColorDark = isColorDark;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * This function calculates if the specified color is "dark", which usually means
 * you need light text if you use it as a background color to fulfill WCAG contrast
 * requirement.
 * The color must be specified via its red, green and blue value in the range of
 * 0 to 255.
 * The formula is based on this Stackoverflow answer: https://stackoverflow.com/a/3943023
 * which itself is based upon the WCAG recommendation for color contrast.
 *
 * @param {number} red The red component in the range 0 to 255
 * @param {number} green The green component in the range 0 to 255
 * @param {number} blue The blue component in the range 0 to 255
 * @returns {boolean} True if the color is dark, false otherwise.
 */
function isColorDark(red, green, blue) {
  var _map$map = [red, green, blue].map(function (c) {
    return c / 255.0;
  }).map(function (c) {
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  }),
      _map$map2 = (0, _slicedToArray2.default)(_map$map, 3),
      r = _map$map2[0],
      g = _map$map2[1],
      b = _map$map2[2];

  var luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return luminance <= 0.179;
}