/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import chroma from 'chroma-js';
import { isValidHex } from './is_valid_hex';

var inOriginalFormat = function inOriginalFormat(originalColor, newColor) {
  return isValidHex(originalColor) ? newColor.hex() : newColor.css();
};
/**
 * Makes a color more transparent.
 * @param color - Color to manipulate
 * @param alpha - alpha channel value. From 0-1.
 */


export var transparentize = function transparentize(color, alpha) {
  return chroma(color).alpha(alpha).css();
};
/**
 * Mixes a provided color with white.
 * @param color - Color to mix with white
 * @param ratio - Mix weight. From 0-1. Larger value indicates more white.
 */

export var tint = function tint(color, ratio) {
  var tint = chroma.mix(color, '#fff', ratio, 'rgb');
  return inOriginalFormat(color, tint);
};
/**
 * Mixes a provided color with black.
 * @param color - Color to mix with black
 * @param ratio - Mix weight. From 0-1. Larger value indicates more black.
 */

export var shade = function shade(color, ratio) {
  var shade = chroma.mix(color, '#000', ratio, 'rgb');
  return inOriginalFormat(color, shade);
};
/**
 * Increases the saturation of a color by manipulating the hsl saturation.
 * @param color - Color to manipulate
 * @param amount - Amount to change in absolute terms. 0-1.
 */

export var saturate = function saturate(color, amount) {
  var saturate = chroma(color).set('hsl.s', "+".concat(amount));
  return inOriginalFormat(color, saturate);
};
/**
 * Decreases the saturation of a color by manipulating the hsl saturation.
 * @param color - Color to manipulate
 * @param amount - Amount to change in absolute terms. 0-1.
 */

export var desaturate = function desaturate(color, amount) {
  var desaturate = chroma(color).set('hsl.s', "-".concat(amount));
  return inOriginalFormat(color, desaturate);
};
/**
 * Returns the lightness value of a color. 0-100
 * @param color
 */

export var lightness = function lightness(color) {
  return chroma(color).get('hsl.l') * 100;
};