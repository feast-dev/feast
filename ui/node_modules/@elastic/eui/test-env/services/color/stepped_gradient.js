"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getSteppedGradient = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _chromaJs = _interopRequireDefault(require("chroma-js"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var getSteppedGradient = function getSteppedGradient(colors, steps) {
  var range = colors[colors.length - 1].stop - colors[0].stop;
  var offset = colors[0].stop;
  var finalStops = (0, _toConsumableArray2.default)(colors.map(function (item) {
    return (item.stop - offset) / range;
  }));
  var color = (0, _toConsumableArray2.default)(colors.map(function (item) {
    return item.color;
  }));
  return _chromaJs.default.scale(color).domain(finalStops).colors(steps);
};

exports.getSteppedGradient = getSteppedGradient;