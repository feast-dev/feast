"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.colorPalette = colorPalette;
exports.MID_COLOR_STOP = void 0;

var _chromaJs = _interopRequireDefault(require("chroma-js"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var MID_COLOR_STOP = '#EBEFF5';
/**
 * This function takes an array of colors and returns an array of interpolated
 * colors based on the number of steps/len needed for use in UI elements such as charts.
 * Derived from https://github.com/gka/palettes (Unlicensed)
 */

exports.MID_COLOR_STOP = MID_COLOR_STOP;

function colorPalette(
/**
 * The main color code or array of codes
 */
colors) {
  var len = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 10;
  var diverging = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  var categorical = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;
  var hexStart = colors.slice();
  var hexEnd = [];
  var even = len % 2 === 0;
  var numColorsLeft = diverging ? Math.ceil(len / 2) + (even ? 1 : 0) : len;
  var numColorsRight = diverging ? Math.ceil(len / 2) + (even ? 1 : 0) : 0; // If only a single color is provided prepend the mid-point color

  if (hexStart.length === 1) {
    hexStart.unshift(MID_COLOR_STOP);
  } // If diverging, split the start array and duplicate the mid color


  if (diverging) {
    // If there's no midpoint, create one
    if (hexStart.length < 3) {
      hexStart[2] = hexStart[1];
      hexStart[1] = MID_COLOR_STOP;
    }

    var numColorsHalf = Math.ceil(hexStart.length / 2);
    var colorsLeft = hexStart.filter(function (item, index) {
      if (index < numColorsHalf) {
        return true; // keep it
      }
    });
    var colorsRight = hexStart.reverse().filter(function (item, index) {
      if (index < numColorsHalf) {
        return true; // keep it
      }
    }).reverse();
    hexStart = colorsLeft;
    hexEnd = colorsRight;
  }

  function createSteps(colors, steps) {
    if (!colors.length) {
      return colors;
    }

    if (!categorical) {
      return _chromaJs.default.bezier(colors).scale().correctLightness().colors(steps);
    } else {
      return _chromaJs.default.scale(colors).colors(steps);
    }
  }

  var stepsLeft = createSteps(hexStart, numColorsLeft);
  var stepsRight = createSteps(hexEnd, numColorsRight);
  return (even && diverging ? stepsLeft.slice(0, stepsLeft.length - 1) : stepsLeft).concat(stepsRight.slice(1));
}