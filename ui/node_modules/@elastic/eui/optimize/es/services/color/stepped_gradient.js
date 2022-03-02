import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import chroma from 'chroma-js';
export var getSteppedGradient = function getSteppedGradient(colors, steps) {
  var range = colors[colors.length - 1].stop - colors[0].stop;
  var offset = colors[0].stop;

  var finalStops = _toConsumableArray(colors.map(function (item) {
    return (item.stop - offset) / range;
  }));

  var color = _toConsumableArray(colors.map(function (item) {
    return item.color;
  }));

  return chroma.scale(color).domain(finalStops).colors(steps);
};