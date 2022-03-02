"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.lightness = exports.desaturate = exports.saturate = exports.shade = exports.tint = exports.transparentize = void 0;

var _chromaJs = _interopRequireDefault(require("chroma-js"));

var _is_valid_hex = require("./is_valid_hex");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var inOriginalFormat = function inOriginalFormat(originalColor, newColor) {
  return (0, _is_valid_hex.isValidHex)(originalColor) ? newColor.hex() : newColor.css();
};
/**
 * Makes a color more transparent.
 * @param color - Color to manipulate
 * @param alpha - alpha channel value. From 0-1.
 */


var transparentize = function transparentize(color, alpha) {
  return (0, _chromaJs.default)(color).alpha(alpha).css();
};
/**
 * Mixes a provided color with white.
 * @param color - Color to mix with white
 * @param ratio - Mix weight. From 0-1. Larger value indicates more white.
 */


exports.transparentize = transparentize;

var tint = function tint(color, ratio) {
  var tint = _chromaJs.default.mix(color, '#fff', ratio, 'rgb');

  return inOriginalFormat(color, tint);
};
/**
 * Mixes a provided color with black.
 * @param color - Color to mix with black
 * @param ratio - Mix weight. From 0-1. Larger value indicates more black.
 */


exports.tint = tint;

var shade = function shade(color, ratio) {
  var shade = _chromaJs.default.mix(color, '#000', ratio, 'rgb');

  return inOriginalFormat(color, shade);
};
/**
 * Increases the saturation of a color by manipulating the hsl saturation.
 * @param color - Color to manipulate
 * @param amount - Amount to change in absolute terms. 0-1.
 */


exports.shade = shade;

var saturate = function saturate(color, amount) {
  var saturate = (0, _chromaJs.default)(color).set('hsl.s', "+".concat(amount));
  return inOriginalFormat(color, saturate);
};
/**
 * Decreases the saturation of a color by manipulating the hsl saturation.
 * @param color - Color to manipulate
 * @param amount - Amount to change in absolute terms. 0-1.
 */


exports.saturate = saturate;

var desaturate = function desaturate(color, amount) {
  var desaturate = (0, _chromaJs.default)(color).set('hsl.s', "-".concat(amount));
  return inOriginalFormat(color, desaturate);
};
/**
 * Returns the lightness value of a color. 0-100
 * @param color
 */


exports.desaturate = desaturate;

var lightness = function lightness(color) {
  return (0, _chromaJs.default)(color).get('hsl.l') * 100;
};

exports.lightness = lightness;